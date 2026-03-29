"""
結算後自動領取模組。

職責：
1. 掃描 Polymarket Data API 的 redeemable positions。
2. 將可領取倉位轉為 CTF redeemPositions 交易。
3. 透過官方 relayer 送出 gasless 領取交易。
4. 將提交流程與狀態寫入 settlement_claims，避免重複領取。
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional

import requests
from eth_abi import encode
from eth_abi.packed import encode_packed
from eth_utils import keccak, to_bytes, to_checksum_address

from py_builder_relayer_client.builder.derive import derive
from py_builder_relayer_client.builder.safe import build_safe_transaction_request
from py_builder_relayer_client.config import get_contract_config
from py_builder_relayer_client.models import OperationType, SafeTransaction, SafeTransactionArgs
from py_builder_relayer_client.signer import Signer
from py_builder_signing_sdk.config import BuilderConfig
from py_builder_signing_sdk.sdk_types import BuilderApiKeyCreds

logger = logging.getLogger(__name__)
claim_logger = logging.getLogger("polymarket.claim")
error_logger = logging.getLogger("polymarket.error")

POLYGON_CHAIN_ID = 137
DEFAULT_DATA_API_HOST = "https://data-api.polymarket.com"
DEFAULT_RELAYER_HOST = "https://relayer-v2.polymarket.com"
DEFAULT_COLLATERAL_TOKEN = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
DEFAULT_CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
DEFAULT_PROXY_FACTORY = "0xaB45c5A4B0c941a2F231C04C3f49182e1A254052"
DEFAULT_RELAY_HUB = "0xD216153c06E857cD7f72665E0aF1d7D82172F494"
DEFAULT_PROXY_GAS_LIMIT = 500_000
ZERO_BYTES32 = "0x" + ("00" * 32)
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
REDEEM_FUNCTION_SIGNATURE = "redeemPositions(address,bytes32,bytes32,uint256[])"
PROXY_FUNCTION_SIGNATURE = "proxy((uint8,address,uint256,bytes)[])"
PROXY_INIT_CODE_HASH = "0xd21df8dc65880a8606f09fe0ce3df9b8869287ab0b058be05aa9e8af6330a00b"
RELAYER_PAYLOAD_PATH = "/relay-payload"
PENDING_RELAYER_STATES = {"STATE_NEW", "STATE_EXECUTED"}
MINED_RELAYER_STATES = {"STATE_MINED"}
CONFIRMED_RELAYER_STATES = {"STATE_CONFIRMED"}
FAILED_RELAYER_STATES = {"STATE_FAILED", "STATE_INVALID"}


class SettlementClaimError(Exception):
    """結算領取流程錯誤。"""


class ClaimStatus(str, Enum):
    """領取追蹤狀態。"""

    SUBMITTED = "submitted"
    MINED = "mined"
    CONFIRMED = "confirmed"
    FAILED = "failed"
    SKIPPED = "skipped"
    DRY_RUN = "dry_run"


class ClaimRelayerType(str, Enum):
    """領取提交使用的 relayer 路徑類型。"""

    AUTO = "auto"
    SAFE = "safe"
    PROXY = "proxy"


@dataclass(frozen=True)
class ClaimPreflightReport:
    """領取路徑正式前檢查摘要。"""

    ready: bool
    message: str
    submission_type: Optional[str] = None
    fetched_positions: int = 0
    relay_nonce: Optional[int] = None


@dataclass(frozen=True)
class RedeemablePosition:
    """可領取倉位摘要。"""

    condition_id: str
    market_id: str
    question: str
    token_id: str
    proxy_wallet: str
    size: float
    redeemable: bool
    raw_payload: Dict[str, Any]


@dataclass
class SettlementClaimResult:
    """單筆領取交易結果。"""

    claim_id: str
    condition_id: str
    market_id: str
    claim_account: str
    status: ClaimStatus
    submitted_at: datetime
    completed_at: Optional[datetime] = None
    question: Optional[str] = None
    proxy_wallet: Optional[str] = None
    transaction_id: Optional[str] = None
    transaction_hash: Optional[str] = None
    safe_nonce: Optional[int] = None
    error_message: Optional[str] = None
    raw_response: Optional[Dict[str, Any]] = None


class SettlementClaimer:
    """結算後自動領取協調器。"""

    def __init__(
        self,
        db_path: Optional[str],
        data_api_host: Optional[str] = None,
        relayer_host: Optional[str] = None,
        claim_account: Optional[str] = None,
        private_key: Optional[str] = None,
        collateral_token: str = DEFAULT_COLLATERAL_TOKEN,
        ctf_address: str = DEFAULT_CTF_ADDRESS,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.db_path = db_path
        self.data_api_host = (data_api_host or os.getenv("POLY_DATA_API_HOST") or DEFAULT_DATA_API_HOST).rstrip("/")
        self.relayer_host = (relayer_host or os.getenv("POLY_RELAYER_HOST") or DEFAULT_RELAYER_HOST).rstrip("/")
        self.claim_account = (
            claim_account
            or os.getenv("FUNDER_ADDRESS")
            or os.getenv("WALLET_ADDRESS")
            or ""
        )
        self.private_key = private_key or os.getenv("WALLET_PRIVATE_KEY") or os.getenv("PRIVATE_KEY") or ""
        self.collateral_token = collateral_token
        self.ctf_address = ctf_address
        self.session = session or requests.Session()
        self._memory_claims: Dict[str, SettlementClaimResult] = {}

        self._builder_config = self._build_builder_config()
        self._relayer_api_key = os.getenv("RELAYER_API_KEY", "")
        self._relayer_api_key_address = os.getenv("RELAYER_API_KEY_ADDRESS", "")
        self._rpc_url = os.getenv("POLY_RPC_URL", "").strip()
        relayer_type = os.getenv("POLY_CLAIM_RELAYER_TYPE", ClaimRelayerType.AUTO.value).strip().lower()
        self._claim_relayer_type = ClaimRelayerType(relayer_type or ClaimRelayerType.AUTO.value)
        self._signer = Signer(self.private_key, POLYGON_CHAIN_ID) if self.private_key else None
        self._contract_config = get_contract_config(POLYGON_CHAIN_ID)
        self._last_proxy_gas_fallback_used = False

        if self.db_path:
            self._init_database()

    def _build_builder_config(self) -> Optional[BuilderConfig]:
        """建立 Builder 憑證配置。"""
        api_key = os.getenv("POLY_BUILDER_API_KEY", "").strip()
        api_secret = (
            os.getenv("POLY_BUILDER_API_SECRET")
            or os.getenv("POLY_BUILDER_SECRET")
            or ""
        ).strip()
        passphrase = os.getenv("POLY_BUILDER_API_PASSPHRASE", "").strip()
        if not api_key or not api_secret or not passphrase:
            return None
        creds = BuilderApiKeyCreds(
            key=api_key,
            secret=api_secret,
            passphrase=passphrase,
        )
        return BuilderConfig(local_builder_creds=creds)

    def _init_database(self) -> None:
        """初始化 settlement_claims 資料表。"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS settlement_claims (
                claim_id TEXT PRIMARY KEY,
                condition_id TEXT NOT NULL,
                market_id TEXT NOT NULL,
                claim_account TEXT NOT NULL,
                question TEXT,
                proxy_wallet TEXT,
                status TEXT NOT NULL,
                submitted_at TEXT NOT NULL,
                completed_at TEXT,
                transaction_id TEXT,
                transaction_hash TEXT,
                safe_nonce INTEGER,
                error_message TEXT,
                raw_response_json TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_settlement_claims_condition
            ON settlement_claims(condition_id, claim_account, submitted_at DESC)
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_settlement_claims_status
            ON settlement_claims(status, submitted_at DESC)
            """
        )
        conn.commit()
        conn.close()

    def _ensure_ready(self) -> None:
        """確認自動領取所需的最小配置完整。"""
        if not self.claim_account:
            raise SettlementClaimError("缺少領取地址，請設定 FUNDER_ADDRESS 或 WALLET_ADDRESS")
        if not self.private_key:
            raise SettlementClaimError("缺少 WALLET_PRIVATE_KEY，無法簽署領取交易")
        if not self._has_auth_credentials():
            raise SettlementClaimError(
                "缺少 relayer 認證。請提供 Builder 憑證，或 RELAYER_API_KEY / RELAYER_API_KEY_ADDRESS"
            )

    def _ensure_scan_ready(self) -> None:
        """確認掃描 redeemable positions 所需的最小配置完整。"""
        if not self.claim_account:
            raise SettlementClaimError("缺少領取地址，請設定 FUNDER_ADDRESS 或 WALLET_ADDRESS")

    def _has_auth_credentials(self) -> bool:
        """判斷是否至少配置一種 relayer 認證。"""
        if self._builder_config is not None:
            return True
        return bool(self._relayer_api_key and self._relayer_api_key_address)

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Any:
        """送出 HTTP 請求並解析 JSON。"""
        response = self.session.request(
            method=method,
            url=url,
            params=params,
            json=json_body,
            headers=headers,
            timeout=20,
        )
        response.raise_for_status()
        if not response.text:
            return None
        return response.json()

    def _build_auth_headers(self, method: str, path: str, payload: Optional[Dict[str, Any]]) -> Dict[str, str]:
        """建立 relayer 認證標頭。"""
        if self._builder_config is not None:
            body = str(payload) if payload is not None else None
            header_payload = self._builder_config.generate_builder_headers(method, path, body)
            if header_payload is None:
                raise SettlementClaimError("Builder 憑證簽名失敗")
            return header_payload.to_dict()

        if self._relayer_api_key and self._relayer_api_key_address:
            return {
                "RELAYER_API_KEY": self._relayer_api_key,
                "RELAYER_API_KEY_ADDRESS": self._relayer_api_key_address,
            }

        raise SettlementClaimError("未配置可用的 relayer 認證")

    @staticmethod
    def _same_address(left: str, right: str) -> bool:
        """比較兩個地址是否相同。"""
        if not left or not right:
            return False
        return left.strip().lower() == right.strip().lower()

    @staticmethod
    def _create2_address(bytecode_hash: str, from_address: str, salt: bytes) -> str:
        """依 CREATE2 規則推導合約地址。"""
        raw_bytecode_hash = bytecode_hash[2:] if bytecode_hash.startswith("0x") else bytecode_hash
        raw_from_address = from_address[2:] if from_address.startswith("0x") else from_address
        address_hash = keccak(
            b"\xff"
            + to_bytes(hexstr=f"0x{raw_from_address}")
            + salt
            + to_bytes(hexstr=f"0x{raw_bytecode_hash}")
        )
        return to_checksum_address(address_hash[-20:].hex())

    def _get_proxy_factory(self) -> str:
        """取得 proxy factory 地址，舊版 SDK 缺欄位時退回官方常數。"""
        return getattr(self._contract_config, "proxy_factory", "") or DEFAULT_PROXY_FACTORY

    def _get_relay_hub(self) -> str:
        """取得 relay hub 地址，舊版 SDK 缺欄位時退回官方常數。"""
        return getattr(self._contract_config, "relay_hub", "") or DEFAULT_RELAY_HUB

    def _get_expected_proxy_wallet(self) -> str:
        """推導 signer 對應的 Polymarket proxy wallet。"""
        if self._signer is None:
            raise SettlementClaimError("缺少 signer")
        signer_address = to_checksum_address(self._signer.address())
        proxy_factory = to_checksum_address(self._get_proxy_factory())
        salt = keccak(encode_packed(["address"], [signer_address]))
        return self._create2_address(PROXY_INIT_CODE_HASH, proxy_factory, salt)

    def _get_expected_safe(self) -> str:
        """推導 signer 對應的 Safe 地址。"""
        if self._signer is None:
            raise SettlementClaimError("缺少 signer")
        return derive(self._signer.address(), self._contract_config.safe_factory)

    def _is_safe_deployed(self) -> bool:
        """確認 Safe 是否已部署。"""
        safe_address = self._get_expected_safe()
        payload = self._request_json(
            "GET",
            f"{self.relayer_host}/deployed",
            params={"address": safe_address},
        )
        return bool(payload and payload.get("deployed"))

    def _get_safe_nonce(self) -> int:
        """查詢 relayer 當前 Safe nonce。"""
        if self._signer is None:
            raise SettlementClaimError("缺少 signer")
        payload = self._request_json(
            "GET",
            f"{self.relayer_host}/nonce",
            params={"address": self._signer.address(), "type": "SAFE"},
        )
        if not payload or payload.get("nonce") is None:
            raise SettlementClaimError("無法取得 Safe nonce")
        return int(payload["nonce"])

    def _get_proxy_relay_payload(self) -> Dict[str, Any]:
        """查詢 relayer 提供的 proxy 提交資訊。"""
        if self._signer is None:
            raise SettlementClaimError("缺少 signer")
        payload = self._request_json(
            "GET",
            f"{self.relayer_host}{RELAYER_PAYLOAD_PATH}",
            params={"address": self._signer.address(), "type": "PROXY"},
        )
        if not isinstance(payload, dict) or payload.get("nonce") is None or not payload.get("address"):
            raise SettlementClaimError("無法取得 proxy relay payload")
        return payload

    @staticmethod
    def _normalize_bytes32(value: str) -> str:
        """正規化 bytes32 十六進位字串。"""
        raw = (value or "").strip()
        if raw.startswith("0x"):
            raw = raw[2:]
        raw = raw.lower()
        if len(raw) != 64:
            raise SettlementClaimError(f"condition_id 不是合法 bytes32: {value}")
        return f"0x{raw}"

    def _build_redeem_calldata(self, condition_id: str) -> str:
        """組裝 CTF redeemPositions calldata。"""
        normalized_condition_id = self._normalize_bytes32(condition_id)
        function_selector = keccak(text=REDEEM_FUNCTION_SIGNATURE)[:4]
        encoded_args = encode(
            ["address", "bytes32", "bytes32", "uint256[]"],
            [
                self.collateral_token,
                bytes.fromhex(ZERO_BYTES32[2:]),
                bytes.fromhex(normalized_condition_id[2:]),
                [1, 2],
            ],
        )
        return "0x" + (function_selector + encoded_args).hex()

    def _build_safe_relayer_payload(self, position: RedeemablePosition, safe_nonce: int) -> Dict[str, Any]:
        """建立 SAFE 路徑的 relayer /submit payload。"""
        if self._signer is None:
            raise SettlementClaimError("缺少 signer")
        redeem_tx = SafeTransaction(
            to=self.ctf_address,
            operation=OperationType.Call,
            data=self._build_redeem_calldata(position.condition_id),
            value="0",
        )
        args = SafeTransactionArgs(
            from_address=self._signer.address(),
            nonce=str(safe_nonce),
            chain_id=POLYGON_CHAIN_ID,
            transactions=[redeem_tx],
        )
        return build_safe_transaction_request(
            signer=self._signer,
            args=args,
            config=self._contract_config,
            metadata=f"Redeem condition {position.condition_id}",
        ).to_dict()

    def _encode_proxy_transaction_data(self, transactions: List[Dict[str, Any]]) -> str:
        """依官方 proxy ABI 編碼批次交易資料。"""
        function_selector = keccak(text=PROXY_FUNCTION_SIGNATURE)[:4]
        encoded_transactions = []
        for txn in transactions:
            data = str(txn["data"])
            encoded_transactions.append(
                (
                    int(txn["call_type"]),
                    to_checksum_address(str(txn["to"])),
                    int(txn["value"]),
                    to_bytes(hexstr=data),
                )
            )
        encoded_args = encode(["(uint8,address,uint256,bytes)[]"], [encoded_transactions])
        return "0x" + (function_selector + encoded_args).hex()

    def _estimate_proxy_gas(self, data: str) -> str:
        """估算 proxy factory 執行 gas，失敗時退回官方預設值。"""
        self._last_proxy_gas_fallback_used = False
        if not self._rpc_url or self._signer is None:
            self._last_proxy_gas_fallback_used = True
            claim_logger.warning("claim gas estimate 跳過 RPC，改用預設 gas limit=%s", DEFAULT_PROXY_GAS_LIMIT)
            return str(DEFAULT_PROXY_GAS_LIMIT)

        payload = {
            "jsonrpc": "2.0",
            "method": "eth_estimateGas",
            "params": [
                {
                    "from": self._signer.address(),
                    "to": self._get_proxy_factory(),
                    "data": data,
                }
            ],
            "id": 1,
        }
        try:
            response = requests.post(self._rpc_url, json=payload, timeout=10)
            response.raise_for_status()
            result = response.json()
            if isinstance(result, dict) and result.get("result"):
                return str(int(str(result["result"]), 16))
        except (requests.RequestException, ValueError, TypeError):
            self._last_proxy_gas_fallback_used = True
            logger.warning("proxy gas estimate 失敗，改用預設 gas limit=%s", DEFAULT_PROXY_GAS_LIMIT)
            claim_logger.warning("claim gas estimate 失敗，改用預設 gas limit=%s", DEFAULT_PROXY_GAS_LIMIT)
        return str(DEFAULT_PROXY_GAS_LIMIT)

    def _create_proxy_struct_hash(
        self,
        *,
        to: str,
        data: str,
        nonce: str,
        relay_address: str,
        gas_limit: str,
        gas_price: str = "0",
        relayer_fee: str = "0",
    ) -> str:
        """建立 proxy 路徑簽名所需 struct hash。"""
        if self._signer is None:
            raise SettlementClaimError("缺少 signer")
        message = (
            b"rlx:"
            + to_bytes(hexstr=self._signer.address())
            + to_bytes(hexstr=to)
            + to_bytes(hexstr=data)
            + int(relayer_fee).to_bytes(32, "big")
            + int(gas_price).to_bytes(32, "big")
            + int(gas_limit).to_bytes(32, "big")
            + int(nonce).to_bytes(32, "big")
            + to_bytes(hexstr=self._get_relay_hub())
            + to_bytes(hexstr=relay_address)
        )
        return "0x" + keccak(message).hex()

    def _build_proxy_relayer_payload(self, position: RedeemablePosition, relay_payload: Dict[str, Any]) -> Dict[str, Any]:
        """建立 PROXY 路徑的 relayer /submit payload。"""
        if self._signer is None:
            raise SettlementClaimError("缺少 signer")

        proxy_data = self._encode_proxy_transaction_data(
            [
                {
                    "call_type": 1,
                    "to": self.ctf_address,
                    "value": 0,
                    "data": self._build_redeem_calldata(position.condition_id),
                }
            ]
        )
        gas_limit = self._estimate_proxy_gas(proxy_data)
        nonce = str(relay_payload["nonce"])
        relay_address = str(relay_payload["address"])
        signature = self._signer.sign_eip712_struct_hash(
            self._create_proxy_struct_hash(
                to=self._get_proxy_factory(),
                data=proxy_data,
                nonce=nonce,
                relay_address=relay_address,
                gas_limit=gas_limit,
            )
        )
        return {
            "type": ClaimRelayerType.PROXY.value.upper(),
            "from": self._signer.address(),
            "to": self._get_proxy_factory(),
            "proxyWallet": self._get_expected_proxy_wallet(),
            "data": proxy_data,
            "nonce": nonce,
            "signature": signature,
            "signatureParams": {
                "gasPrice": "0",
                "gasLimit": gas_limit,
                "relayerFee": "0",
                "relayHub": self._get_relay_hub(),
                "relay": relay_address,
            },
            "metadata": f"Redeem condition {position.condition_id}",
        }

    def _resolve_claim_relayer_type(self, position: RedeemablePosition) -> ClaimRelayerType:
        """判斷本次領取應走 SAFE 或 PROXY 路徑。"""
        expected_safe = self._get_expected_safe()
        expected_proxy = self._get_expected_proxy_wallet()

        if self._claim_relayer_type == ClaimRelayerType.SAFE:
            if not self._same_address(self.claim_account, expected_safe):
                raise SettlementClaimError("已指定 safe 模式，但 claim_account 與 signer 推導的 safe 地址不一致")
            return ClaimRelayerType.SAFE

        if self._claim_relayer_type == ClaimRelayerType.PROXY:
            if not self._same_address(self.claim_account, expected_proxy):
                raise SettlementClaimError("已指定 proxy 模式，但 claim_account 與 signer 推導的 proxy wallet 不一致")
            if position.proxy_wallet and not self._same_address(position.proxy_wallet, expected_proxy):
                raise SettlementClaimError("position 內的 proxy wallet 與 signer 推導地址不一致")
            return ClaimRelayerType.PROXY

        if self._same_address(self.claim_account, expected_proxy):
            if position.proxy_wallet and not self._same_address(position.proxy_wallet, expected_proxy):
                raise SettlementClaimError("position 內的 proxy wallet 與 signer 推導地址不一致")
            return ClaimRelayerType.PROXY

        if self._same_address(self.claim_account, expected_safe):
            return ClaimRelayerType.SAFE

        raise SettlementClaimError(
            "claim_account 無法對應 signer 推導的 safe 或 proxy wallet，請檢查 FUNDER_ADDRESS / WALLET_ADDRESS 或 POLY_CLAIM_RELAYER_TYPE"
        )

    def _build_relayer_payload(
        self,
        position: RedeemablePosition,
        submission_type: ClaimRelayerType,
    ) -> tuple[Dict[str, Any], int]:
        """依提交類型建立 relayer payload 與對應 nonce。"""
        if submission_type == ClaimRelayerType.SAFE:
            safe_nonce = self._get_safe_nonce()
            return self._build_safe_relayer_payload(position, safe_nonce), safe_nonce

        relay_payload = self._get_proxy_relay_payload()
        proxy_nonce = int(str(relay_payload["nonce"]))
        return self._build_proxy_relayer_payload(position, relay_payload), proxy_nonce

    def _upsert_claim_result(self, result: SettlementClaimResult) -> None:
        """將領取結果寫入資料表或記憶體。"""
        self._memory_claims[result.claim_id] = result
        if not self.db_path:
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO settlement_claims (
                claim_id,
                condition_id,
                market_id,
                claim_account,
                question,
                proxy_wallet,
                status,
                submitted_at,
                completed_at,
                transaction_id,
                transaction_hash,
                safe_nonce,
                error_message,
                raw_response_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                result.claim_id,
                result.condition_id,
                result.market_id,
                result.claim_account,
                result.question,
                result.proxy_wallet,
                result.status.value,
                result.submitted_at.isoformat(),
                result.completed_at.isoformat() if result.completed_at else None,
                result.transaction_id,
                result.transaction_hash,
                result.safe_nonce,
                result.error_message,
                json.dumps(result.raw_response, ensure_ascii=False) if result.raw_response is not None else None,
            ),
        )
        conn.commit()
        conn.close()

    def _row_to_claim_result(self, row: sqlite3.Row) -> SettlementClaimResult:
        """將資料表列轉回領取結果。"""
        raw_response = json.loads(row["raw_response_json"]) if row["raw_response_json"] else None
        return SettlementClaimResult(
            claim_id=row["claim_id"],
            condition_id=row["condition_id"],
            market_id=row["market_id"],
            claim_account=row["claim_account"],
            question=row["question"],
            proxy_wallet=row["proxy_wallet"],
            status=ClaimStatus(row["status"]),
            submitted_at=datetime.fromisoformat(row["submitted_at"]),
            completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
            transaction_id=row["transaction_id"],
            transaction_hash=row["transaction_hash"],
            safe_nonce=row["safe_nonce"],
            error_message=row["error_message"],
            raw_response=raw_response,
        )

    def _get_latest_claim_for_condition(self, condition_id: str) -> Optional[SettlementClaimResult]:
        """查詢 condition 最新領取紀錄。"""
        if self.db_path:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT *
                FROM settlement_claims
                WHERE condition_id = ? AND claim_account = ?
                ORDER BY submitted_at DESC
                LIMIT 1
                """,
                (condition_id, self.claim_account),
            )
            row = cursor.fetchone()
            conn.close()
            if row:
                return self._row_to_claim_result(row)

        candidates = [
            result
            for result in self._memory_claims.values()
            if result.condition_id == condition_id and result.claim_account == self.claim_account
        ]
        if not candidates:
            return None
        return sorted(candidates, key=lambda item: item.submitted_at, reverse=True)[0]

    def _get_blocking_claim_for_condition(self, condition_id: str) -> Optional[SettlementClaimResult]:
        """查詢會阻止重複提交的領取紀錄。"""
        blocking_statuses = {
            ClaimStatus.SUBMITTED,
            ClaimStatus.MINED,
            ClaimStatus.CONFIRMED,
        }
        if self.db_path:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT *
                FROM settlement_claims
                WHERE condition_id = ?
                  AND claim_account = ?
                  AND status IN (?, ?, ?)
                ORDER BY submitted_at DESC
                LIMIT 1
                """,
                (
                    condition_id,
                    self.claim_account,
                    ClaimStatus.SUBMITTED.value,
                    ClaimStatus.MINED.value,
                    ClaimStatus.CONFIRMED.value,
                ),
            )
            row = cursor.fetchone()
            conn.close()
            if row:
                return self._row_to_claim_result(row)

        candidates = [
            result
            for result in self._memory_claims.values()
            if result.condition_id == condition_id
            and result.claim_account == self.claim_account
            and result.status in blocking_statuses
        ]
        if not candidates:
            return None
        return sorted(candidates, key=lambda item: item.submitted_at, reverse=True)[0]

    def _get_pending_claims(self) -> List[SettlementClaimResult]:
        """取得尚未進入終態的領取紀錄。"""
        pending_statuses = {ClaimStatus.SUBMITTED.value, ClaimStatus.MINED.value}
        if self.db_path:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT *
                FROM settlement_claims
                WHERE claim_account = ? AND status IN (?, ?)
                ORDER BY submitted_at ASC
                """,
                (self.claim_account, ClaimStatus.SUBMITTED.value, ClaimStatus.MINED.value),
            )
            rows = cursor.fetchall()
            conn.close()
            return [self._row_to_claim_result(row) for row in rows]

        return [
            result
            for result in self._memory_claims.values()
            if result.claim_account == self.claim_account and result.status.value in pending_statuses
        ]

    @staticmethod
    def _extract_string(payload: Dict[str, Any], keys: Iterable[str]) -> str:
        """從多個候選欄位中取第一個非空字串。"""
        for key in keys:
            value = payload.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
        return ""

    @staticmethod
    def _extract_float(payload: Dict[str, Any], keys: Iterable[str]) -> float:
        """從多個候選欄位中取第一個可轉為浮點數的值。"""
        for key in keys:
            value = payload.get(key)
            if value in (None, ""):
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return 0.0

    def _parse_position(self, payload: Dict[str, Any]) -> Optional[RedeemablePosition]:
        """將 Data API payload 轉為內部倉位模型。"""
        condition_id = self._extract_string(payload, ["conditionId", "condition_id"])
        if not condition_id:
            return None

        market_id = self._extract_string(payload, ["marketId", "market_id", "market"])
        token_id = self._extract_string(payload, ["asset", "tokenId", "token_id"])
        question = self._extract_string(payload, ["title", "question", "marketQuestion"])
        proxy_wallet = self._extract_string(payload, ["proxyWallet", "proxy_wallet", "proxyAddress"])
        size = self._extract_float(payload, ["size", "amount", "balance"])
        redeemable = bool(payload.get("redeemable", True))

        return RedeemablePosition(
            condition_id=condition_id,
            market_id=market_id,
            question=question,
            token_id=token_id,
            proxy_wallet=proxy_wallet,
            size=size,
            redeemable=redeemable,
            raw_payload=payload,
        )

    def fetch_redeemable_positions(self) -> List[RedeemablePosition]:
        """抓取當前帳戶可領取倉位。"""
        self._ensure_scan_ready()
        payload = self._request_json(
            "GET",
            f"{self.data_api_host}/positions",
            params={
                "user": self.claim_account,
                "redeemable": "true",
                "sizeThreshold": "0",
            },
        )
        if not isinstance(payload, list):
            logger.warning("redeemable positions 回傳不是 list，略過本輪掃描")
            return []

        deduped: Dict[str, RedeemablePosition] = {}
        for item in payload:
            if not isinstance(item, dict):
                continue
            position = self._parse_position(item)
            if position is None or not position.redeemable:
                continue
            deduped.setdefault(position.condition_id, position)
        return list(deduped.values())

    def run_preflight(self) -> ClaimPreflightReport:
        """執行正式 claim 路徑的提交前檢查，但不真正送出交易。"""
        try:
            self._ensure_ready()
            positions = self.fetch_redeemable_positions()
            expected_proxy = self._get_expected_proxy_wallet()
            sample_position = positions[0] if positions else RedeemablePosition(
                condition_id=ZERO_BYTES32,
                market_id="preflight",
                question="preflight",
                token_id="",
                proxy_wallet=expected_proxy,
                size=0.0,
                redeemable=True,
                raw_payload={},
            )
            submission_type = self._resolve_claim_relayer_type(sample_position)
            if submission_type == ClaimRelayerType.SAFE and not self._is_safe_deployed():
                return ClaimPreflightReport(
                    ready=False,
                    message="Safe 尚未部署，無法通過 claim preflight",
                    submission_type=submission_type.value,
                    fetched_positions=len(positions),
                )
            _, relay_nonce = self._build_relayer_payload(sample_position, submission_type)
            return ClaimPreflightReport(
                ready=True,
                message=f"claim preflight 通過 | submission_type={submission_type.value} | positions={len(positions)}",
                submission_type=submission_type.value,
                fetched_positions=len(positions),
                relay_nonce=relay_nonce,
            )
        except (SettlementClaimError, requests.RequestException) as exc:
            error_logger.error("claim preflight 失敗 | error=%s", exc)
            return ClaimPreflightReport(
                ready=False,
                message=str(exc),
            )

    def refresh_pending_claims(self) -> List[SettlementClaimResult]:
        """回補已提交領取交易的 relayer 狀態。"""
        refreshed: List[SettlementClaimResult] = []
        for claim in self._get_pending_claims():
            if not claim.transaction_id:
                continue
            payload = self._request_json(
                "GET",
                f"{self.relayer_host}/transaction",
                params={"id": claim.transaction_id},
            )
            txns = payload if isinstance(payload, list) else []
            if not txns:
                continue
            latest = txns[0]
            relayer_state = str(latest.get("state") or "").strip()
            claim.transaction_hash = str(latest.get("transactionHash") or claim.transaction_hash or "").strip() or None
            claim.raw_response = latest

            if relayer_state in CONFIRMED_RELAYER_STATES:
                claim.status = ClaimStatus.CONFIRMED
                claim.completed_at = datetime.now(timezone.utc)
            elif relayer_state in MINED_RELAYER_STATES:
                claim.status = ClaimStatus.MINED
            elif relayer_state in FAILED_RELAYER_STATES:
                claim.status = ClaimStatus.FAILED
                claim.completed_at = datetime.now(timezone.utc)
                claim.error_message = latest.get("error") or latest.get("failureReason") or claim.error_message
            elif relayer_state in PENDING_RELAYER_STATES:
                claim.status = ClaimStatus.SUBMITTED

            self._upsert_claim_result(claim)
            refreshed.append(claim)
        return refreshed

    def submit_claim(self, position: RedeemablePosition) -> SettlementClaimResult:
        """送出單筆 redeemPositions 領取交易。"""
        self._ensure_ready()

        existing = self._get_blocking_claim_for_condition(position.condition_id)
        if existing and existing.status in {ClaimStatus.SUBMITTED, ClaimStatus.MINED, ClaimStatus.CONFIRMED}:
            skipped = SettlementClaimResult(
                claim_id=str(uuid.uuid4()),
                condition_id=position.condition_id,
                market_id=position.market_id,
                claim_account=self.claim_account,
                question=position.question,
                proxy_wallet=position.proxy_wallet,
                status=ClaimStatus.SKIPPED,
                submitted_at=datetime.now(timezone.utc),
                completed_at=datetime.now(timezone.utc),
                error_message=f"已有既存領取紀錄: {existing.status.value}",
                raw_response={"existing_claim_id": existing.claim_id},
            )
            self._upsert_claim_result(skipped)
            claim_logger.info(
                "claim 跳過 | condition=%s | status=%s | reason=%s",
                position.condition_id,
                skipped.status.value,
                skipped.error_message,
            )
            return skipped

        submitted_at = datetime.now(timezone.utc)
        relay_nonce: Optional[int] = None
        submission_type: Optional[ClaimRelayerType] = None

        try:
            submission_type = self._resolve_claim_relayer_type(position)
            if submission_type == ClaimRelayerType.SAFE and not self._is_safe_deployed():
                raise SettlementClaimError("Safe 尚未部署，無法提交自動領取")

            submit_payload, relay_nonce = self._build_relayer_payload(position, submission_type)
            headers = self._build_auth_headers("POST", "/submit", submit_payload)
            response = self._request_json(
                "POST",
                f"{self.relayer_host}/submit",
                json_body=submit_payload,
                headers=headers,
            )
            if self._last_proxy_gas_fallback_used:
                claim_logger.warning(
                    "claim 提交使用預設 gas limit | condition=%s | submission_type=%s",
                    position.condition_id,
                    submission_type.value,
                )
            result = SettlementClaimResult(
                claim_id=str(uuid.uuid4()),
                condition_id=position.condition_id,
                market_id=position.market_id,
                claim_account=self.claim_account,
                question=position.question,
                proxy_wallet=position.proxy_wallet,
                status=ClaimStatus.SUBMITTED,
                submitted_at=submitted_at,
                transaction_id=response.get("transactionID") if isinstance(response, dict) else None,
                transaction_hash=response.get("transactionHash") if isinstance(response, dict) else None,
                safe_nonce=relay_nonce,
                raw_response={
                    "submission_type": submission_type.value if submission_type else None,
                    "response": response,
                },
            )
            self._upsert_claim_result(result)
            claim_logger.info(
                "claim 已提交 | condition=%s | status=%s | submission_type=%s | tx_id=%s",
                position.condition_id,
                result.status.value,
                submission_type.value,
                result.transaction_id,
            )
            return result
        except (SettlementClaimError, requests.RequestException) as exc:
            failed = SettlementClaimResult(
                claim_id=str(uuid.uuid4()),
                condition_id=position.condition_id,
                market_id=position.market_id,
                claim_account=self.claim_account,
                question=position.question,
                proxy_wallet=position.proxy_wallet,
                status=ClaimStatus.FAILED,
                submitted_at=submitted_at,
                completed_at=datetime.now(timezone.utc),
                safe_nonce=relay_nonce,
                error_message=str(exc),
                raw_response={
                    "submission_type": submission_type.value if submission_type else None,
                },
            )
            self._upsert_claim_result(failed)
            error_logger.error(
                "claim 提交失敗 | condition=%s | error=%s",
                position.condition_id,
                exc,
            )
            return failed

    def scan_and_claim(self, dry_run: bool = False) -> List[SettlementClaimResult]:
        """執行一輪 pending 回補與新倉位自動領取。"""
        self._ensure_scan_ready()
        results: List[SettlementClaimResult] = []
        if not dry_run:
            self._ensure_ready()
            results.extend(self.refresh_pending_claims())
        positions = self.fetch_redeemable_positions()
        logger.info(
            "Auto claim scan | account=%s | redeemable_positions=%s | dry_run=%s",
            self.claim_account,
            len(positions),
            dry_run,
        )

        for position in positions:
            if dry_run:
                results.append(
                    SettlementClaimResult(
                        claim_id=str(uuid.uuid4()),
                        condition_id=position.condition_id,
                        market_id=position.market_id,
                        claim_account=self.claim_account,
                        question=position.question,
                        proxy_wallet=position.proxy_wallet,
                        status=ClaimStatus.DRY_RUN,
                        submitted_at=datetime.now(timezone.utc),
                        completed_at=datetime.now(timezone.utc),
                        raw_response=position.raw_payload,
                    )
                )
            else:
                results.append(self.submit_claim(position))
        return results
