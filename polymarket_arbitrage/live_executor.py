"""
Package G: Live Executor - 實盤交易執行器

收益最大化風控策略：
- 單筆 Edge 門檻：>3%（保守但確保正期望值）
- 單筆倉位：動態計算（根據 edge 大小和信心分數）
- 最大持倉：15 個市場（充分分散）
- 單日停損：-30% 帳戶餘額
- 追蹤所有成交並回寫至 SignalLogger
"""

import os
import time
import logging
import json
import sqlite3
from typing import TYPE_CHECKING, Optional, Dict, List, Any, Tuple

# 定義 logger
lifecycle_logger = logging.getLogger("polymarket.lifecycle")
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from enum import Enum

# Polymarket SDK
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    OrderArgs,
    ApiCreds,
    BalanceAllowanceParams,
    PartialCreateOrderOptions,
)
from py_clob_client.constants import POLYGON

from .market_definition import MarketDefinition
from .proxy_account import (
    ProxyAccountIdentity,
    resolve_proxy_account_identity,
    same_address,
)
from .reference_builder import ReferencePrice
from .fair_prob_model import FairProbEstimate
from .signal_logger import SignalLogger, SignalObservation
from .updown_tail_pricer import TAIL_WINDOWS, UpDownTailPricer

if TYPE_CHECKING:
    from .research_pipeline import TradingCandidate

logger = logging.getLogger(__name__)
preflight_logger = logging.getLogger("polymarket.preflight")
order_logger = logging.getLogger("polymarket.order")
fill_logger = logging.getLogger("polymarket.fill")
error_logger = logging.getLogger("polymarket.error")


class LiveExecutionStatus(Enum):
    """實盤執行狀態"""

    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class ExecutionError(Exception):
    """執行層錯誤"""

    pass


class InsufficientBalanceError(ExecutionError):
    """餘額不足"""

    pass


class RiskLimitExceededError(ExecutionError):
    """超過風控限制"""

    pass


class LivePreflightStatus(Enum):
    """正式啟動前檢查狀態。"""

    READY = "ready"
    FAILED = "failed"


@dataclass(frozen=True)
class LiveRiskConfig:
    """
    實盤風控配置 - 收益最大化策略
    """

    # Edge 門檻（核心過濾器）
    min_edge_threshold: float = 0.03  # 3% minimum edge
    min_confidence_score: float = 0.3  # 信心分數門檻

    # 倉位管理
    max_position_per_trade: float = 250.0  # 單筆最大 $250
    min_position_per_trade: float = 25.0  # 單筆最小 $25
    position_sizing_formula: str = "kelly_fractional"  # 倉位計算公式
    kelly_fraction: float = 0.25  # 使用 1/4 Kelly

    # 持倉限制
    max_open_positions: int = 8  # 最大同時持倉數
    max_same_market_positions: int = 1  # 單市場單方向

    # 時間限制
    min_time_to_expiry_minutes: float = 5.0  # 至少 5 分鐘到期
    max_time_to_expiry_hours: float = 168.0  # 最多 7 天

    # 停損/停利
    daily_loss_limit_pct: float = 0.30  # 單日 -30% 停損
    trailing_stop_pct: Optional[float] = 0.20  # 20% 追蹤停損

    # 執行參數
    max_slippage_tolerance: float = 0.02  # 最大滑價 2%
    order_timeout_seconds: int = 45  # 訂單超時
    retry_attempts: int = 5  # 重試次數
    allow_taker_fallback: bool = False  # 長週期是否允許 taker fallback
    min_marketable_buy_notional: float = 1.0  # 市價/可立即成交 BUY 的最小名義金額

    # 流動性過濾
    min_market_liquidity: float = 1000.0  # 最小流動性 $1000
    max_spread_pct: float = 0.10  # 最大價差 10%

    @property
    def is_aggressive(self) -> bool:
        """是否為積極策略"""
        return self.min_edge_threshold <= 0.05


@dataclass
class LiveExecutionResult:
    """實盤執行結果"""

    order_id: str
    market_id: str
    observation_id: str
    side: str  # "YES" or "NO"
    size: float
    price: float
    filled_size: float
    avg_fill_price: float
    fee_paid: float
    status: LiveExecutionStatus
    created_at: datetime
    filled_at: Optional[datetime] = None
    error_message: Optional[str] = None
    raw_response: Optional[Dict] = None

    @property
    def is_filled(self) -> bool:
        return self.status == LiveExecutionStatus.FILLED

    @property
    def actual_cost(self) -> float:
        """實際成本（含手續費）"""
        return self.filled_size * self.avg_fill_price + self.fee_paid

    @property
    def slippage(self) -> float:
        """實際滑價"""
        if self.avg_fill_price > 0 and self.price > 0:
            return abs(self.avg_fill_price - self.price) / self.price
        return 0.0


@dataclass(frozen=True)
class LivePreflightCheck:
    """單項 preflight 檢查結果。"""

    name: str
    passed: bool
    message: str


@dataclass(frozen=True)
class LivePreflightReport:
    """正式啟動前檢查摘要。"""

    status: LivePreflightStatus
    ready: bool
    checks: List[LivePreflightCheck]
    signer_address: Optional[str] = None
    proxy_wallet: Optional[str] = None
    funder_address: Optional[str] = None


@dataclass(frozen=True)
class LiveRuntimeRestoreResult:
    """重啟恢復摘要。"""

    pending_order_count: int
    directional_exposure_count: int
    remote_open_order_count: int
    remote_position_count: int


@dataclass
class AccountState:
    """帳戶狀態快照"""

    timestamp: datetime
    wallet_address: str
    usdc_balance: float
    positions: List[Dict[str, Any]]  # 當前持倉
    open_orders: List[Dict[str, Any]]  # 未完成訂單
    daily_pnl: float  # 當日盈虧
    daily_trades: int  # 當日交易次數

    @property
    def available_capital(self) -> float:
        """可用資金"""
        locked = sum(p.get("size", 0) * p.get("price", 0) for p in self.positions)
        return self.usdc_balance - locked


class LiveExecutor:
    """
    實盤交易執行器

    核心職責：
    1. 風控檢查（所有 gate 通過才下單）
    2. 倉位計算（根據 edge 和信心動態調整）
    3. 訂單提交與追蹤
    4. 成交回寫至 SignalLogger
    5. 帳戶狀態監控
    """

    def __init__(
        self,
        signal_logger: SignalLogger,
        risk_config: Optional[LiveRiskConfig] = None,
    ):
        self.logger = signal_logger
        self.db_path = signal_logger.db_path
        self.risk = risk_config or LiveRiskConfig()

        # CLOB 客戶端（延遲初始化）
        self._clob_client: Optional[ClobClient] = None
        self._wallet_address: Optional[str] = None
        self._account_identity: Optional[ProxyAccountIdentity] = None

        # 執行狀態
        self._pending_orders: Dict[str, LiveExecutionResult] = {}
        self._pending_order_exposure_keys: Dict[str, str] = {}
        self._directional_exposure_keys: set[str] = set()
        self._daily_stats = {
            "date": datetime.now(timezone.utc).date(),
            "trades": 0,
            "pnl": 0.0,
            "volume": 0.0,
        }

        # 從環境讀取配置
        self._api_key = os.getenv("POLYMARKET_API_KEY", "")
        self._api_secret = os.getenv("POLYMARKET_API_SECRET", "")
        self._passphrase = os.getenv("POLYMARKET_API_PASSPHRASE") or os.getenv(
            "POLYMARKET_PASSPHRASE", ""
        )
        self._private_key = os.getenv("WALLET_PRIVATE_KEY") or os.getenv(
            "PRIVATE_KEY", ""
        )
        self._wallet_address = os.getenv("WALLET_ADDRESS", "")
        self._funder_address = os.getenv("FUNDER_ADDRESS", "")
        self._tail_pricer = UpDownTailPricer()
        self._preflight_timeout = int(os.getenv("POLY_PREFLIGHT_TIMEOUT", "20"))

        if self.db_path:
            self._init_runtime_state_tables()

    def _init_runtime_state_tables(self) -> None:
        """初始化正式版運行狀態資料表。"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS live_pending_orders (
                order_id TEXT PRIMARY KEY,
                market_id TEXT NOT NULL,
                observation_id TEXT NOT NULL,
                asset TEXT NOT NULL,
                side TEXT NOT NULL,
                size REAL NOT NULL,
                price REAL NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                filled_at TEXT,
                exposure_key TEXT,
                raw_response_json TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS live_directional_exposures (
                exposure_key TEXT PRIMARY KEY,
                asset TEXT NOT NULL,
                side TEXT NOT NULL,
                market_id TEXT NOT NULL,
                order_id TEXT,
                source_status TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS live_preflight_heartbeats (
                check_name TEXT PRIMARY KEY,
                checked_at TEXT NOT NULL
            )
            """
        )
        conn.commit()
        conn.close()

    def get_account_identity(self) -> ProxyAccountIdentity:
        """取得 proxy-only 帳戶身份解析結果。"""
        if self._account_identity is not None:
            return self._account_identity
        if not self._private_key:
            raise ExecutionError("缺少 WALLET_PRIVATE_KEY，無法推導 signer")
        funder_address = self._funder_address or self._wallet_address
        if not funder_address:
            raise ExecutionError("缺少 FUNDER_ADDRESS，無法驗證 proxy wallet")

        identity = resolve_proxy_account_identity(
            self._private_key,
            funder_address=funder_address,
            wallet_address=self._wallet_address or None,
        )
        self._account_identity = identity
        self._wallet_address = identity.wallet_address
        self._funder_address = identity.funder_address
        return identity

    def _verify_sqlite_writable(self) -> Tuple[bool, str]:
        """驗證 SQLite 資料庫可寫入。"""
        if not self.db_path:
            return False, "未設定 SQLite db_path"
        try:
            conn = sqlite3.connect(self.db_path, timeout=self._preflight_timeout)
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO live_preflight_heartbeats (check_name, checked_at)
                VALUES (?, ?)
                """,
                ("sqlite_writable", datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
            conn.close()
            return True, "SQLite 可寫入"
        except sqlite3.Error as exc:
            return False, f"SQLite 寫入失敗: {exc}"

    def _append_preflight_check(
        self,
        checks: List[LivePreflightCheck],
        *,
        name: str,
        passed: bool,
        message: str,
    ) -> None:
        """追加單項 preflight 結果並輸出分類日誌。"""
        checks.append(LivePreflightCheck(name=name, passed=passed, message=message))
        log_message = f"{name} | passed={passed} | {message}"
        if passed:
            preflight_logger.info(log_message)
        else:
            preflight_logger.error(log_message)
            error_logger.error("preflight 失敗 | %s", log_message)

    def run_preflight(
        self, settlement_claimer: Optional[Any] = None
    ) -> LivePreflightReport:
        """執行正式 live 啟動前檢查。"""
        checks: List[LivePreflightCheck] = []
        signer_address: Optional[str] = None
        proxy_wallet: Optional[str] = None
        funder_address: Optional[str] = None

        creds_ok, creds_msg = self.check_credentials()
        self._append_preflight_check(
            checks, name="credentials", passed=creds_ok, message=creds_msg
        )
        if not creds_ok:
            return LivePreflightReport(
                status=LivePreflightStatus.FAILED,
                ready=False,
                checks=checks,
            )

        try:
            identity = self.get_account_identity()
            signer_address = identity.signer_address
            proxy_wallet = identity.proxy_wallet
            funder_address = identity.funder_address
            self._append_preflight_check(
                checks,
                name="proxy_identity",
                passed=same_address(identity.funder_address, identity.proxy_wallet),
                message=f"signer={identity.signer_address} proxy={identity.proxy_wallet} funder={identity.funder_address}",
            )
        except Exception as exc:
            self._append_preflight_check(
                checks, name="proxy_identity", passed=False, message=str(exc)
            )
            return LivePreflightReport(
                status=LivePreflightStatus.FAILED,
                ready=False,
                checks=checks,
                signer_address=signer_address,
                proxy_wallet=proxy_wallet,
                funder_address=funder_address,
            )

        try:
            account = self.get_account_state()
            self._append_preflight_check(
                checks,
                name="clob_account_state",
                passed=True,
                message=f"balance={account.usdc_balance:.2f} open_orders={len(account.open_orders)} positions={len(account.positions)}",
            )
        except Exception as exc:
            self._append_preflight_check(
                checks, name="clob_account_state", passed=False, message=str(exc)
            )

        sqlite_ok, sqlite_msg = self._verify_sqlite_writable()
        self._append_preflight_check(
            checks, name="sqlite", passed=sqlite_ok, message=sqlite_msg
        )

        if settlement_claimer is not None:
            try:
                claim_report = settlement_claimer.run_preflight()
                self._append_preflight_check(
                    checks,
                    name="claim_path",
                    passed=claim_report.ready,
                    message=claim_report.message,
                )
            except Exception as exc:
                self._append_preflight_check(
                    checks, name="claim_path", passed=False, message=str(exc)
                )

        ready = all(item.passed for item in checks)
        return LivePreflightReport(
            status=LivePreflightStatus.READY if ready else LivePreflightStatus.FAILED,
            ready=ready,
            checks=checks,
            signer_address=signer_address,
            proxy_wallet=proxy_wallet,
            funder_address=funder_address,
        )

    def _persist_pending_order(
        self, result: LiveExecutionResult, *, asset: str, exposure_key: str
    ) -> None:
        """將 pending order 落庫。"""
        self._pending_order_exposure_keys[result.order_id] = exposure_key
        if not self.db_path:
            return
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO live_pending_orders (
                order_id, market_id, observation_id, asset, side, size, price, status, created_at, filled_at, exposure_key, raw_response_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                result.order_id,
                result.market_id,
                result.observation_id,
                asset,
                result.side,
                result.size,
                result.price,
                result.status.value,
                result.created_at.isoformat(),
                result.filled_at.isoformat() if result.filled_at else None,
                exposure_key,
                json.dumps(result.raw_response, ensure_ascii=False)
                if result.raw_response is not None
                else None,
            ),
        )
        conn.commit()
        conn.close()

    def _delete_pending_order(self, order_id: str) -> None:
        """刪除已終態的 pending order。"""
        self._pending_order_exposure_keys.pop(order_id, None)
        if not self.db_path:
            return
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM live_pending_orders WHERE order_id = ?", (order_id,)
        )
        conn.commit()
        conn.close()

    def _persist_directional_exposure(
        self,
        *,
        exposure_key: str,
        asset: str,
        side: str,
        market_id: str,
        order_id: str,
        source_status: str,
    ) -> None:
        """將方向暴露鍵持久化。"""
        self._directional_exposure_keys.add(exposure_key)
        if not self.db_path:
            return
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO live_directional_exposures (
                exposure_key, asset, side, market_id, order_id, source_status, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                exposure_key,
                asset,
                side,
                market_id,
                order_id,
                source_status,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()

    def _delete_directional_exposure(self, exposure_key: str) -> None:
        """刪除不再有效的方向暴露鍵。"""
        self._directional_exposure_keys.discard(exposure_key)
        if not self.db_path:
            return
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM live_directional_exposures WHERE exposure_key = ?",
            (exposure_key,),
        )
        conn.commit()
        conn.close()

    def restore_runtime_state(self) -> LiveRuntimeRestoreResult:
        """從 SQLite 與遠端帳戶狀態恢復 pending orders / directional exposure。"""
        restored_pending: Dict[str, LiveExecutionResult] = {}
        restored_exposures: set[str] = set()

        if self.db_path:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM live_pending_orders ORDER BY created_at ASC")
            for row in cursor.fetchall():
                raw_response = (
                    json.loads(row["raw_response_json"])
                    if row["raw_response_json"]
                    else None
                )
                restored_pending[row["order_id"]] = LiveExecutionResult(
                    order_id=row["order_id"],
                    market_id=row["market_id"],
                    observation_id=row["observation_id"],
                    side=row["side"],
                    size=float(row["size"]),
                    price=float(row["price"]),
                    filled_size=0.0,
                    avg_fill_price=0.0,
                    fee_paid=0.0,
                    status=LiveExecutionStatus(row["status"]),
                    created_at=datetime.fromisoformat(row["created_at"]),
                    filled_at=datetime.fromisoformat(row["filled_at"])
                    if row["filled_at"]
                    else None,
                    raw_response=raw_response,
                )
                if row["exposure_key"]:
                    self._pending_order_exposure_keys[row["order_id"]] = row[
                        "exposure_key"
                    ]
                    restored_exposures.add(row["exposure_key"])
            cursor.execute("SELECT exposure_key FROM live_directional_exposures")
            restored_exposures.update(
                {row["exposure_key"] for row in cursor.fetchall()}
            )
            conn.close()

        self._pending_orders = restored_pending
        self._directional_exposure_keys = restored_exposures

        account = self.get_account_state()
        preflight_logger.info(
            "runtime restore | pending=%s | exposures=%s | remote_open_orders=%s | remote_positions=%s",
            len(self._pending_orders),
            len(self._directional_exposure_keys),
            len(account.open_orders),
            len(account.positions),
        )
        return LiveRuntimeRestoreResult(
            pending_order_count=len(self._pending_orders),
            directional_exposure_count=len(self._directional_exposure_keys),
            remote_open_order_count=len(account.open_orders),
            remote_position_count=len(account.positions),
        )

    def get_directional_exposure_keys(self) -> set[str]:
        """取得當前方向暴露鍵集合。"""
        return set(self._directional_exposure_keys)

    def _get_clob_client(self) -> ClobClient:
        """獲取/初始化 CLOB 客戶端"""
        if self._clob_client is None:
            creds_ok, creds_msg = self.check_credentials()
            if not creds_ok:
                raise ExecutionError(creds_msg)
            identity = self.get_account_identity()

            self._clob_client = ClobClient(
                host="https://clob.polymarket.com",
                key=self._private_key,
                chain_id=POLYGON,
                signature_type=1,  # POLY_PROXY
                funder=identity.funder_address,
            )
            # 設置 API 憑證（必須使用 ApiCreds 對象）
            api_creds = ApiCreds(
                api_key=self._api_key,
                api_secret=self._api_secret,
                api_passphrase=self._passphrase or "",
            )
            self._clob_client.set_api_creds(api_creds)
        return self._clob_client

    def _extract_best_book_price(self, levels: Any, is_bid: bool = True) -> float:
        """從 order book 提取最優價格，兼容 SDK 物件與 dict。
        
        Args:
            levels: order book 檔位列表
            is_bid: True 表示買單(bids)，False 表示賣單(asks)
                   bids 取最高價，asks 取最低價
        """
        if not levels:
            return 0.0

        # 提取所有價格
        prices = []
        for level in levels:
            raw_price = getattr(level, "price", None)
            if raw_price is None and isinstance(level, dict):
                raw_price = level.get("price")
            if raw_price not in (None, ""):
                try:
                    prices.append(float(raw_price))
                except (ValueError, TypeError):
                    continue
        
        if not prices:
            return 0.0
        
        # bids 取最高，asks 取最低
        return max(prices) if is_bid else min(prices)

    def _refresh_tail_side_quote(
        self, candidate: "TradingCandidate"
    ) -> Tuple[float, float]:
        """送單前重抓選定方向最新 bid / ask，避免沿用研究時刻舊盤口。"""
        opportunity = candidate.opportunity
        estimate = candidate.tail_estimate
        if estimate is None:
            return 0.0, 0.0

        token_id = (
            opportunity.yes_token_id
            if estimate.selected_side == "YES"
            else opportunity.no_token_id
        )
        if not token_id:
            return 0.0, 0.0

        try:
            client = self._get_clob_client()
            payload = client.get_order_book(token_id)
            bids = getattr(payload, "bids", None)
            asks = getattr(payload, "asks", None)
            if bids is None and isinstance(payload, dict):
                bids = payload.get("bids")
            if asks is None and isinstance(payload, dict):
                asks = payload.get("asks")
            return self._extract_best_book_price(bids, is_bid=True), self._extract_best_book_price(
                asks, is_bid=False
            )
        except Exception as exc:
            error_logger.error(
                "尾盤送單前刷新 order book 失敗 | market=%s | side=%s | error=%s",
                opportunity.market_id,
                estimate.selected_side,
                exc,
            )
            return 0.0, 0.0

    def check_credentials(self) -> Tuple[bool, str]:
        """檢查憑證是否完整"""
        missing = []
        if not self._api_key:
            missing.append("POLYMARKET_API_KEY")
        if not self._api_secret:
            missing.append("POLYMARKET_API_SECRET")
        if not self._private_key:
            missing.append("WALLET_PRIVATE_KEY")
        if not (self._funder_address or self._wallet_address):
            missing.append("FUNDER_ADDRESS")

        if missing:
            return False, f"Missing credentials: {', '.join(missing)}"
        try:
            identity = self.get_account_identity()
        except Exception as exc:
            return False, str(exc)
        if not same_address(identity.funder_address, identity.proxy_wallet):
            return False, (
                "FUNDER_ADDRESS 與 signer 推導的 proxy wallet 不一致，"
                f"expected={identity.proxy_wallet}, actual={identity.funder_address}"
            )
        return True, "All credentials present"

    def get_account_state(self) -> AccountState:
        """獲取當前帳戶狀態"""
        try:
            client = self._get_clob_client()
            identity = self.get_account_identity()

            # 使用 funder_address 查詢餘額（資金實際存放處）
            balance_address = identity.funder_address

            # 嘗試獲取餘額（使用正確的 BalanceAllowanceParams）
            usdc_balance = 0.0
            try:
                # 使用 BalanceAllowanceParams 查詢 USDC 餘額
                params = BalanceAllowanceParams(
                    asset_type="COLLATERAL", signature_type=1
                )
                result = client.get_balance_allowance(params)
                if result and "balance" in result:
                    # 餘額是 6 位小數精度
                    usdc_balance = float(result["balance"]) / 1e6
                    logger.info(f"   USDC Balance from CLOB: {usdc_balance:.2f}")
            except Exception as e:
                logger.warning(f"Could not fetch balance from CLOB: {e}")

            # 獲取持倉（使用 funder_address）
            positions = []
            try:
                if hasattr(client, "get_positions"):
                    positions = client.get_positions(balance_address) or []
            except Exception as e:
                logger.warning(f"Could not fetch positions: {e}")

            # 獲取未完成訂單
            open_orders = []
            try:
                if hasattr(client, "get_open_orders"):
                    open_orders = client.get_open_orders() or []
            except Exception as e:
                logger.warning(f"Could not fetch open orders: {e}")

            # 重置每日統計（如果日期變了）
            today = datetime.now(timezone.utc).date()
            if today != self._daily_stats["date"]:
                self._daily_stats = {
                    "date": today,
                    "trades": 0,
                    "pnl": 0.0,
                    "volume": 0.0,
                }

            return AccountState(
                timestamp=datetime.now(timezone.utc),
                wallet_address=identity.wallet_address,
                usdc_balance=usdc_balance,
                positions=positions if positions else [],
                open_orders=open_orders if open_orders else [],
                daily_pnl=self._daily_stats["pnl"],
                daily_trades=self._daily_stats["trades"],
            )

        except Exception as e:
            logger.error(f"Failed to get account state: {e}")
            raise ExecutionError(f"Account state fetch failed: {e}")

    def check_risk_limits(
        self, account: AccountState, proposed_amount: float
    ) -> Tuple[bool, str]:
        """
        檢查風控限制

        Args:
            proposed_amount: 擬議交易金額（美元）

        Returns: (passed, reason_if_failed)
        """
        # 1. 每日停損檢查
        if account.daily_pnl < -account.usdc_balance * self.risk.daily_loss_limit_pct:
            return False, f"Daily loss limit hit: {account.daily_pnl:.2f} USDC"

        # 2. 持倉數量檢查
        open_positions = len(account.positions)
        if open_positions >= self.risk.max_open_positions:
            return (
                False,
                f"Max positions reached: {open_positions}/{self.risk.max_open_positions}",
            )

        # 3. 可用資金檢查
        if proposed_amount > account.available_capital:
            return (
                False,
                f"Insufficient balance: {account.available_capital:.2f} USDC available",
            )

        # 4. 單筆倉位限制（金額）
        if proposed_amount > self.risk.max_position_per_trade:
            return (
                False,
                f"Position size {proposed_amount:.2f} exceeds max {self.risk.max_position_per_trade}",
            )

        if proposed_amount < self.risk.min_position_per_trade:
            return (
                False,
                f"Position size {proposed_amount:.2f} below min {self.risk.min_position_per_trade}",
            )

        return True, "OK"

    def calculate_position_size(
        self,
        edge: float,
        confidence: float,
        yes_ask: float,
        account: AccountState,
    ) -> float:
        """
        計算倉位金額 - Fractional Kelly 策略

        Returns:
            目標交易金額（美元），後續需轉換為 shares = amount / price

        公式: amount = min(max_size, kelly_fraction * edge / variance * capital)
        簡化: amount = base_size * edge_multiplier * confidence_multiplier
        """
        # 基礎倉位（總資金的 2%）
        base_size = account.usdc_balance * 0.02

        # Edge 乘數（edge 越大，倉位越大）
        # edge = 0.03 → 1x, edge = 0.10 → 3x
        edge_multiplier = min(edge / self.risk.min_edge_threshold, 3.0)

        # 信心乘數
        confidence_multiplier = max(confidence, 0.5)

        # 計算目標倉位
        target_size = base_size * edge_multiplier * confidence_multiplier

        # 應用限制
        target_size = min(target_size, self.risk.max_position_per_trade)
        target_size = max(target_size, self.risk.min_position_per_trade)
        target_size = min(target_size, account.available_capital * 0.9)  # 保留 10% 緩衝

        # 向下取整到整數
        return float(
            Decimal(str(target_size)).quantize(Decimal("1"), rounding=ROUND_DOWN)
        )

    def should_execute(
        self,
        market_def: MarketDefinition,
        fair_prob: FairProbEstimate,
        yes_ask: float,
        no_ask: float,
        account: AccountState,
    ) -> Tuple[bool, Optional[str], Optional[Dict]]:
        """
        決定是否執行交易 - 核心決策函數

        Returns: (should_execute, reason_if_no, execution_params_if_yes)
        """
        execution_params = {
            "side": None,
            "size": 0.0,
            "expected_price": 0.0,
            "edge": 0.0,
        }

        # 1. 檢查 Edge（含估算手續費扣除）
        # Polymarket fee ≈ 2% on winning side (varies by market)
        estimated_fee_pct = 0.02
        edge_yes = fair_prob.p_yes - yes_ask - estimated_fee_pct
        edge_no = fair_prob.p_no - no_ask - estimated_fee_pct if no_ask else -1.0

        # 選擇較大的 edge（使用絕對值比較，支持做多和做空）
        abs_edge_yes = abs(edge_yes)
        abs_edge_no = abs(edge_no) if no_ask else 0.0
        
        if abs_edge_yes >= abs_edge_no and edge_yes >= self.risk.min_edge_threshold:
            side = "YES"
            edge = edge_yes
            expected_price = yes_ask
        elif abs_edge_no >= self.risk.min_edge_threshold:
            side = "NO"
            edge = edge_no
            expected_price = no_ask
        else:
            return (
                False,
                f"Edge too small: YES={edge_yes:.4f}, NO={edge_no:.4f}, min={self.risk.min_edge_threshold}",
                None,
            )

        # 2. 檢查信心分數
        if fair_prob.model_confidence_score < self.risk.min_confidence_score:
            return (
                False,
                f"Confidence too low: {fair_prob.model_confidence_score:.4f}",
                None,
            )

        # 3. 檢查時間
        time_to_expiry = fair_prob.time_to_expiry_sec
        if time_to_expiry < self.risk.min_time_to_expiry_minutes * 60:
            return False, f"Too close to expiry: {time_to_expiry / 60:.1f} min", None

        if time_to_expiry > self.risk.max_time_to_expiry_hours * 3600:
            return (
                False,
                f"Too far from expiry: {time_to_expiry / 3600:.1f} hours",
                None,
            )

        # 4. 計算倉位
        size = self.calculate_position_size(
            edge=edge,
            confidence=fair_prob.model_confidence_score,
            yes_ask=yes_ask,
            account=account,
        )
        
        # 確保能買到至少 5 shares (Polymarket API 要求)
        min_shares = 5
        min_amount_for_shares = min_shares * expected_price
        size = max(size, min_amount_for_shares)
        lifecycle_logger.info(
            "[DEBUG] 倉位調整 | original=%.4f | min_for_5_shares=%.4f | final=%.4f | price=%.4f",
            self.calculate_position_size(edge=edge, confidence=fair_prob.model_confidence_score, yes_ask=yes_ask, account=account),
            min_amount_for_shares, size, expected_price
        )

        if size < self.risk.min_position_per_trade:
            return False, f"Calculated size too small: {size:.2f} (min_position={self.risk.min_position_per_trade})", None

        # 5. 風控限制檢查
        passed, reason = self.check_risk_limits(account, size)
        if not passed:
            return False, f"Risk limit: {reason}", None

        # 6. 檢查是否已有相同市場持倉
        for pos in account.positions:
            if pos.get("market_id") == market_def.market_id:
                return False, "Already have position in this market", None

        execution_params.update(
            {
                "side": side,
                "size": size,
                "expected_price": expected_price,
                "edge": edge,
            }
        )

        return True, None, execution_params

    def execute_trade(
        self,
        market_def: MarketDefinition,
        ref_price: ReferencePrice,
        fair_prob: FairProbEstimate,
        observation: SignalObservation,
        yes_token_id: str,
        no_token_id: str,
        yes_ask: float,
        no_ask: float,
        tick_size: str = "0.01",
        neg_risk: bool = False,
        execution_side_override: Optional[str] = None,
        price_override: Optional[float] = None,
        size_override: Optional[float] = None,
        edge_override: Optional[float] = None,
        skip_decision_gate: bool = False,
    ) -> LiveExecutionResult:
        """
        執行實盤交易

        完整流程：
        1. 獲取帳戶狀態
        2. 決策是否下單
        3. 計算倉位
        4. 提交訂單
        5. 追蹤成交
        6. 回寫結果
        """
        # 檢查憑證
        creds_ok, creds_msg = self.check_credentials()
        if not creds_ok:
            raise ExecutionError(creds_msg)

        # 獲取帳戶狀態
        account = self.get_account_state()
        logger.info(
            f"Account: {account.usdc_balance:.2f} USDC, {len(account.positions)} positions"
        )

        if skip_decision_gate:
            side = execution_side_override or ""
            amount = size_override or 0.0
            expected_price = price_override or 0.0
            edge = edge_override or 0.0
            lifecycle_logger.info(
                "[DEBUG] execute_trade 尾盤模式 | side=%s | amount=%.4f | price=%.4f",
                side, amount, expected_price
            )
            if not side or expected_price <= 0 or amount <= 0:
                lifecycle_logger.warning("[DEBUG] 尾盤參數不完整 | side=%s | price=%.4f | amount=%.4f", side, expected_price, amount)
                return LiveExecutionResult(
                    order_id="",
                    market_id=market_def.market_id,
                    observation_id=observation.observation_id,
                    side=side,
                    size=amount,
                    price=expected_price,
                    filled_size=0.0,
                    avg_fill_price=0.0,
                    fee_paid=0.0,
                    status=LiveExecutionStatus.FAILED,
                    created_at=datetime.now(timezone.utc),
                    error_message="Tail execution override is incomplete",
                )
            passed, reason = self.check_risk_limits(account, amount)
            if not passed:
                return LiveExecutionResult(
                    order_id="",
                    market_id=market_def.market_id,
                    observation_id=observation.observation_id,
                    side=side,
                    size=amount,
                    price=expected_price,
                    filled_size=0.0,
                    avg_fill_price=0.0,
                    fee_paid=0.0,
                    status=LiveExecutionStatus.FAILED,
                    created_at=datetime.now(timezone.utc),
                    error_message=f"Risk limit: {reason}",
                )
        else:
            # 決策
            should_exec, reason, params = self.should_execute(
                market_def=market_def,
                fair_prob=fair_prob,
                yes_ask=yes_ask,
                no_ask=no_ask,
                account=account,
            )

            if not should_exec:
                logger.info(f"Trade rejected: {reason}")
                return LiveExecutionResult(
                    order_id="",
                    market_id=market_def.market_id,
                    observation_id=observation.observation_id,
                    side="",
                    size=0.0,
                    price=0.0,
                    filled_size=0.0,
                    avg_fill_price=0.0,
                    fee_paid=0.0,
                    status=LiveExecutionStatus.FAILED,
                    created_at=datetime.now(timezone.utc),
                    error_message=reason,
                )

            # 準備下單
            side = params["side"]
            amount = params["size"]  # 金額（美元）
            expected_price = params["expected_price"]
            edge = params["edge"]

        token_id = yes_token_id if side == "YES" else no_token_id

        # 轉換 shares：shares = 金額 / 價格
        shares = amount / expected_price if expected_price > 0 else 0

        # 檢查最小訂單金額（Polymarket 要求最少 1 USDC）
        if amount < 1.0:
            logger.warning(f"Order too small: ${amount:.2f} (< $1.0 min)")
            return LiveExecutionResult(
                order_id="",
                market_id=market_def.market_id,
                observation_id=observation.observation_id,
                side=side,
                size=amount,
                price=expected_price,
                filled_size=0.0,
                avg_fill_price=0.0,
                fee_paid=0.0,
                status=LiveExecutionStatus.FAILED,
                created_at=datetime.now(timezone.utc),
                error_message=f"Order too small: ${amount:.2f} (min $1.0)",
            )

        # 向下取整到整數 shares
        shares = int(shares)
        
        lifecycle_logger.info(
            "[DEBUG] shares 計算 | amount=%.4f | price=%.4f | shares=%d",
            amount, expected_price, shares
        )
        
        # Polymarket API 要求最少 5 shares
        if shares < 5:
            lifecycle_logger.warning(f"[DEBUG] Order too small: {shares} shares (< 5 min)")
            return LiveExecutionResult(
                order_id="",
                market_id=market_def.market_id,
                observation_id=observation.observation_id,
                side=side,
                size=amount,
                price=expected_price,
                filled_size=0.0,
                avg_fill_price=0.0,
                fee_paid=0.0,
                status=LiveExecutionStatus.FAILED,
                created_at=datetime.now(timezone.utc),
                error_message=f"Order too small: {shares} shares (min 5)",
            )
        
        lifecycle_logger.info("[DEBUG] shares 檢查通過 | shares=%d", shares)
        
        actual_amount = shares * expected_price  # 實際花費
        reference_ask = yes_ask if side == "YES" else no_ask

        if (
            reference_ask
            and expected_price >= reference_ask
            and actual_amount < self.risk.min_marketable_buy_notional
        ):
            return LiveExecutionResult(
                order_id="",
                market_id=market_def.market_id,
                observation_id=observation.observation_id,
                side=side,
                size=actual_amount,
                price=expected_price,
                filled_size=0.0,
                avg_fill_price=0.0,
                fee_paid=0.0,
                status=LiveExecutionStatus.FAILED,
                created_at=datetime.now(timezone.utc),
                error_message=(
                    f"Marketable BUY amount {actual_amount:.2f} below exchange minimum "
                    f"{self.risk.min_marketable_buy_notional:.2f}"
                ),
            )

        logger.info(
            f"Executing {side} order: {shares} shares @ {expected_price:.4f} = ${actual_amount:.2f}, edge={edge:.4f}"
        )
        order_logger.info(
            "送單準備 | market=%s | asset=%s | side=%s | shares=%s | price=%.4f | amount=%.2f | edge=%.4f",
            market_def.market_id,
            market_def.asset,
            side,
            shares,
            expected_price,
            actual_amount,
            edge,
        )

        try:
            client = self._get_clob_client()

            # 官方 SDK 需要 tick size 與 neg risk 設定。
            order_args = OrderArgs(
                token_id=token_id,
                price=expected_price,
                size=float(shares),
                side="BUY",
            )
            options = PartialCreateOrderOptions(
                tick_size=tick_size,
                neg_risk=neg_risk,
            )

            # 使用官方推薦的 create_and_post_order 一次完成簽名與下單。
            order_resp = client.create_and_post_order(
                order_args=order_args,
                options=options,
            )

            order_id = order_resp.get("orderID", "")

            result = LiveExecutionResult(
                order_id=order_id,
                market_id=market_def.market_id,
                observation_id=observation.observation_id,
                side=side,
                size=actual_amount,  # 記錄實際金額
                price=expected_price,
                filled_size=0.0,  # 初始為 0，稍後更新
                avg_fill_price=0.0,
                fee_paid=0.0,
                status=LiveExecutionStatus.SUBMITTED,
                created_at=datetime.now(timezone.utc),
                raw_response=order_resp,
            )

            # 保存到 pending
            self._pending_orders[order_id] = result
            exposure_key = f"{market_def.asset}:{side}"
            self._persist_pending_order(
                result, asset=market_def.asset, exposure_key=exposure_key
            )
            self._persist_directional_exposure(
                exposure_key=exposure_key,
                asset=market_def.asset,
                side=side,
                market_id=market_def.market_id,
                order_id=order_id,
                source_status=result.status.value,
            )

            # 更新每日統計（按金額統計）
            self._daily_stats["trades"] += 1
            self._daily_stats["volume"] += actual_amount

            logger.info(f"Order submitted: {order_id} ({shares} shares)")
            order_logger.info(
                "送單成功 | order_id=%s | market=%s | side=%s | amount=%.2f",
                order_id,
                market_def.market_id,
                side,
                actual_amount,
            )

            return result

        except Exception as e:
            logger.error(f"Order submission failed: {e}")
            error_logger.error(
                "送單失敗 | market=%s | side=%s | error=%s",
                market_def.market_id,
                side,
                e,
            )
            return LiveExecutionResult(
                order_id="",
                market_id=market_def.market_id,
                observation_id=observation.observation_id,
                side=side,
                size=amount,
                price=expected_price,
                filled_size=0.0,
                avg_fill_price=0.0,
                fee_paid=0.0,
                status=LiveExecutionStatus.FAILED,
                created_at=datetime.now(timezone.utc),
                error_message=str(e),
            )

    def execute_candidate(self, candidate: "TradingCandidate") -> LiveExecutionResult:
        """以研究候選機會為單位執行交易。"""
        lifecycle_logger.info(
            "[DEBUG] execute_candidate 入口 | style=%s | has_tail_estimate=%s | has_snapshot=%s",
            candidate.opportunity.market_style,
            candidate.tail_estimate is not None,
            candidate.runtime_snapshot is not None,
        )
        if (
            candidate.opportunity.market_style == "UP_DOWN"
            and candidate.tail_estimate is not None
            and candidate.runtime_snapshot is not None
        ):
            return self._execute_tail_candidate(candidate)
        opportunity = candidate.opportunity
        return self.execute_trade(
            market_def=candidate.market_definition,
            ref_price=candidate.reference_price,
            fair_prob=candidate.fair_probability,
            observation=candidate.observation,
            yes_token_id=opportunity.yes_token_id,
            no_token_id=opportunity.no_token_id,
            yes_ask=opportunity.yes_ask or 0.0,
            no_ask=opportunity.no_ask or 0.0,
            tick_size=candidate.tick_size,
            neg_risk=candidate.neg_risk,
        )

    def _execute_tail_candidate(
        self, candidate: "TradingCandidate"
    ) -> LiveExecutionResult:
        """以尾盤策略規則執行候選機會。"""
        opportunity = candidate.opportunity
        snapshot = candidate.runtime_snapshot
        estimate = candidate.tail_estimate
        if snapshot is None or estimate is None:
            raise ExecutionError("Tail candidate missing runtime snapshot or estimate")

        timeframe = opportunity.timeframe or ""
        ws = snapshot.window_state
        min_edge = self._tail_pricer.minimum_net_edge(timeframe, ws)
        if abs(estimate.selected_net_edge) < min_edge:
            return self._reject_tail_candidate(
                candidate,
                f"net_edge 不足: {estimate.selected_net_edge:.4f} (abs={abs(estimate.selected_net_edge):.4f}) < {min_edge:.4f}",
            )
        # NOTE: lead_z check disabled - purely edge-based strategy
        # min_lead_z = self._tail_pricer.minimum_lead_z(timeframe, ws)
        # if abs(estimate.lead_z) < min_lead_z:
        #     return self._reject_tail_candidate(
        #         candidate, f"lead_z 不足: {estimate.lead_z:.4f} < {min_lead_z:.4f}"
        #     )
        if ws not in {"armed", "attack", "observe"}:
            return self._reject_tail_candidate(candidate, f"狀態不允許下單: {ws}")

        # observe 使用不同的 exposure key 前綴，方便後續管理
        exposure_prefix = "obs" if ws == "observe" else "tail"
        exposure_key = f"{exposure_prefix}:{opportunity.asset}:{estimate.selected_side}"
        # 同時檢查無前綴格式（舊格式相容）
        legacy_key = f"{opportunity.asset}:{estimate.selected_side}"
        lifecycle_logger.info(
            "[DEBUG] 檢查倉位 | exposure_key=%s | legacy_key=%s | existing=%s",
            exposure_key, legacy_key, list(self._directional_exposure_keys)
        )
        if exposure_key in self._directional_exposure_keys or legacy_key in self._directional_exposure_keys:
            return self._reject_tail_candidate(
                candidate, f"同資產同方向已有倉位: {exposure_key} (或 {legacy_key})"
            )

        account = self.get_account_state()
        bucket_amount = account.usdc_balance * self._tail_pricer.position_bucket(
            timeframe, ws
        )
        reference_ask = (
            opportunity.yes_ask
            if estimate.selected_side == "YES"
            else opportunity.no_ask
        )
        # 計算目標倉位（Kelly 公式）
        kelly_size = self.calculate_position_size(
            edge=estimate.selected_net_edge,
            confidence=opportunity.confidence_score,
            yes_ask=reference_ask or 0.01,
            account=account,
        )
        
        # 受 bucket 限制，但確保 >= min_position
        # 如果 bucket 太小導致 < min_position，則不下單（資金不足）
        if bucket_amount < self.risk.min_position_per_trade:
            return self._reject_tail_candidate(
                candidate, 
                f"倉位比例不足: bucket={bucket_amount:.2f} < min={self.risk.min_position_per_trade}"
            )
        
        amount = min(kelly_size, bucket_amount)
        amount = max(amount, self.risk.min_position_per_trade)  # 保底
        passed, reason = self.check_risk_limits(account, amount)
        lifecycle_logger.info(
            "[DEBUG] 檢查風控 | passed=%s | reason=%s | amount=%.4f | balance=%.2f",
            passed, reason, amount, account.usdc_balance
        )
        if not passed:
            return self._reject_tail_candidate(candidate, f"Risk limit: {reason}")

        refreshed_bid, refreshed_ask = self._refresh_tail_side_quote(candidate)
        lifecycle_logger.info(
            "[DEBUG] 檢查 order book | bid=%.4f | ask=%.4f",
            refreshed_bid, refreshed_ask
        )
        if refreshed_bid <= 0 and refreshed_ask <= 0:
            return self._reject_tail_candidate(
                candidate, "送單前最新 order book 不可用"
            )

        order_price = self._select_tail_order_price(
            candidate,
            refreshed_bid=refreshed_bid,
            refreshed_ask=refreshed_ask,
        )
        lifecycle_logger.info(
            "[DEBUG] 檢查價格 | order_price=%.4f",
            order_price
        )
        if order_price <= 0:
            return self._reject_tail_candidate(candidate, "找不到可用的尾盤下單價格")

        refreshed_yes_ask = opportunity.yes_ask or 0.0
        refreshed_no_ask = opportunity.no_ask or 0.0
        if estimate.selected_side == "YES":
            refreshed_yes_ask = refreshed_ask
        else:
            refreshed_no_ask = refreshed_ask

        result = self.execute_trade(
            market_def=candidate.market_definition,
            ref_price=candidate.reference_price,
            fair_prob=candidate.fair_probability,
            observation=candidate.observation,
            yes_token_id=opportunity.yes_token_id,
            no_token_id=opportunity.no_token_id,
            yes_ask=refreshed_yes_ask,
            no_ask=refreshed_no_ask,
            tick_size=candidate.tick_size,
            neg_risk=candidate.neg_risk,
            execution_side_override=estimate.selected_side,
            price_override=order_price,
            size_override=amount,
            edge_override=estimate.selected_net_edge,
            skip_decision_gate=True,
        )
        if result.status in {LiveExecutionStatus.SUBMITTED, LiveExecutionStatus.FILLED}:
            self._directional_exposure_keys.add(exposure_key)
            self._directional_exposure_keys.add(legacy_key)  # 同時添加無前綴格式
        return result

    def _select_tail_order_price(
        self,
        candidate: "TradingCandidate",
        refreshed_bid: Optional[float] = None,
        refreshed_ask: Optional[float] = None,
    ) -> float:
        """根據 timeframe 與尾盤狀態選擇 maker / taker 價格。"""
        opportunity = candidate.opportunity
        snapshot = candidate.runtime_snapshot
        estimate = candidate.tail_estimate
        if snapshot is None or estimate is None:
            return 0.0

        if estimate.selected_side == "YES":
            maker_price = (
                refreshed_bid if refreshed_bid is not None else opportunity.yes_bid
            ) or 0.0
            taker_price = (
                refreshed_ask if refreshed_ask is not None else opportunity.yes_ask
            ) or 0.0
        else:
            maker_price = (
                refreshed_bid if refreshed_bid is not None else opportunity.no_bid
            ) or 0.0
            taker_price = (
                refreshed_ask if refreshed_ask is not None else opportunity.no_ask
            ) or 0.0

        # observe 階段：永遠只用 maker（bid 側掛單），絕不吃單
        if snapshot.window_state == "observe":
            return maker_price

        selected_execution_mode = getattr(
            estimate, "selected_execution_mode", None
        ) or getattr(opportunity, "selected_execution_mode", None)
        if selected_execution_mode == "maker":
            return maker_price or taker_price
        if selected_execution_mode == "taker":
            return taker_price or maker_price

        timeframe = opportunity.timeframe or ""
        if timeframe in {"5m", "15m"}:
            required_edge = self._tail_pricer.minimum_net_edge(timeframe) * 2
            # 5m 改為 armed/attack 都可進場；15m 維持 attack
            if timeframe == "5m":
                allowed_states = {"armed", "attack"}
            else:
                allowed_states = {"attack"}
            
            if (
                snapshot.window_state in allowed_states
                and estimate.selected_net_edge >= required_edge
            ):
                return taker_price
            return maker_price or taker_price

        attack_window = TAIL_WINDOWS.get(timeframe, {}).get("attack", 0)
        if (
            self.risk.allow_taker_fallback
            and snapshot.window_state == "attack"
            and attack_window > 0
            and snapshot.tau_seconds <= attack_window * (2 / 3)
        ):
            return taker_price
        return maker_price or taker_price

    def _reject_tail_candidate(
        self, candidate: "TradingCandidate", reason: str
    ) -> LiveExecutionResult:
        """建立尾盤候選機會拒絕結果。"""
        opportunity = candidate.opportunity
        lifecycle_logger.info(
            "[DEBUG] 尾盤候選拒絕 | asset=%s | side=%s | edge=%.4f | reason=%s",
            opportunity.asset,
            opportunity.selected_side,
            getattr(opportunity, "net_edge", 0),
            reason,
        )
        return LiveExecutionResult(
            order_id="",
            market_id=opportunity.market_id,
            observation_id=opportunity.observation_id,
            side=opportunity.selected_side,
            size=0.0,
            price=0.0,
            filled_size=0.0,
            avg_fill_price=0.0,
            fee_paid=0.0,
            status=LiveExecutionStatus.FAILED,
            created_at=datetime.now(timezone.utc),
            error_message=reason,
        )

    def _normalize_remote_order_status(self, order_info: Dict[str, Any]) -> str:
        """正規化交易所訂單狀態，兼容不同終態別名。"""
        status_str = str(order_info.get("status", "")).upper()
        if status_str in {"FILLED", "MATCHED"}:
            return "FILLED"
        if status_str in {"CANCELLED", "CANCELED"}:
            return "CANCELLED"
        return status_str

    def _extract_remote_filled_size(self, order_info: Dict[str, Any]) -> float:
        """從交易所回包提取成交數量，兼容 market order 與 limit order 欄位。"""
        raw_size_matched = order_info.get("size_matched", order_info.get("sizeMatched"))
        if raw_size_matched not in (None, ""):
            return float(raw_size_matched)

        raw_taker_amount = order_info.get("takerAmount", order_info.get("taker_amount"))
        if raw_taker_amount in (None, ""):
            return 0.0

        taker_amount = float(raw_taker_amount)
        if taker_amount >= 1_000_000:
            return taker_amount / 1e6
        return taker_amount

    def _extract_remote_fee_paid(self, order_info: Dict[str, Any]) -> float:
        """從交易所回包提取手續費，缺失時回傳 0。"""
        raw_fee = order_info.get("fee", order_info.get("fee_paid"))
        if raw_fee in (None, ""):
            return 0.0

        raw_fee_text = str(raw_fee).strip()
        fee_paid = float(raw_fee_text)
        if raw_fee_text.isdigit():
            return fee_paid / 1e6
        return fee_paid

    def _is_order_timed_out(
        self, result: LiveExecutionResult, now: Optional[datetime] = None
    ) -> bool:
        """判斷 pending 訂單是否已超過本地超時上限。"""
        current_time = now or datetime.now(timezone.utc)
        age_seconds = (current_time - result.created_at).total_seconds()
        return age_seconds >= self.risk.order_timeout_seconds

    def _mark_order_terminal(
        self,
        order_id: str,
        status: LiveExecutionStatus,
        error_message: Optional[str] = None,
    ) -> Optional[LiveExecutionResult]:
        """將 pending 訂單標記為終態並清理本地持久化狀態。"""
        result = self._pending_orders.get(order_id)
        if result is None:
            return None

        result.status = status
        if error_message:
            result.error_message = error_message
        if status == LiveExecutionStatus.FILLED:
            result.filled_at = datetime.now(timezone.utc)

        del self._pending_orders[order_id]
        exposure_key = self._pending_order_exposure_keys.get(order_id, "")
        self._delete_pending_order(order_id)
        if exposure_key:
            self._delete_directional_exposure(exposure_key)
        return result

    def poll_order_status(self, order_id: str) -> Optional[LiveExecutionResult]:
        """輪詢訂單狀態並更新"""
        if order_id not in self._pending_orders:
            return None

        try:
            client = self._get_clob_client()
            order_info = client.get_order(order_id)

            result = self._pending_orders[order_id]

            # 解析狀態
            status_str = self._normalize_remote_order_status(order_info)
            if status_str == "FILLED":
                result.status = LiveExecutionStatus.FILLED
                result.filled_at = datetime.now(timezone.utc)
                result.filled_size = self._extract_remote_filled_size(order_info)
                result.avg_fill_price = float(order_info.get("price", 0))
                result.fee_paid = self._extract_remote_fee_paid(order_info)

                # 從 pending 移除
                del self._pending_orders[order_id]
                self._delete_pending_order(order_id)

                logger.info(
                    f"Order filled: {order_id}, size={result.filled_size:.2f}, avg_price={result.avg_fill_price:.4f}"
                )
                fill_logger.info(
                    "成交完成 | order_id=%s | market=%s | side=%s | filled_size=%.2f | avg_price=%.4f",
                    order_id,
                    result.market_id,
                    result.side,
                    result.filled_size,
                    result.avg_fill_price,
                )

            elif status_str == "PARTIALLY_FILLED":
                result.status = LiveExecutionStatus.PARTIALLY_FILLED
                result.filled_size = self._extract_remote_filled_size(order_info)

            elif status_str == "CANCELLED":
                result = (
                    self._mark_order_terminal(order_id, LiveExecutionStatus.CANCELLED)
                    or result
                )

            elif status_str == "EXPIRED":
                result = (
                    self._mark_order_terminal(order_id, LiveExecutionStatus.EXPIRED)
                    or result
                )

            elif self._is_order_timed_out(result):
                cancelled = self.cancel_order(order_id)
                if cancelled:
                    result.status = LiveExecutionStatus.CANCELLED
                    result.error_message = (
                        f"Order timeout exceeded {self.risk.order_timeout_seconds}s"
                    )
                    lifecycle_logger = logging.getLogger("polymarket.lifecycle")
                    lifecycle_logger.info(
                        "訂單逾時取消 | order_id=%s | timeout_seconds=%s",
                        order_id,
                        self.risk.order_timeout_seconds,
                    )

            return result

        except Exception as e:
            logger.error(f"Failed to poll order {order_id}: {e}")
            error_logger.error("訂單輪詢失敗 | order_id=%s | error=%s", order_id, e)
            return None

    def get_pending_orders(self) -> List[LiveExecutionResult]:
        """獲取所有待處理訂單"""
        return list(self._pending_orders.values())

    def cancel_order(self, order_id: str) -> bool:
        """取消訂單"""
        try:
            client = self._get_clob_client()
            client.cancel(order_id)

            if order_id in self._pending_orders:
                self._pending_orders[order_id].status = LiveExecutionStatus.CANCELLED
                del self._pending_orders[order_id]
            exposure_key = self._pending_order_exposure_keys.get(order_id, "")
            self._delete_pending_order(order_id)
            if exposure_key:
                self._delete_directional_exposure(exposure_key)

            return True
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            error_logger.error("取消訂單失敗 | order_id=%s | error=%s", order_id, e)
            return False

    def cancel_all_pending(self) -> int:
        """取消所有待處理訂單，返回取消數量"""
        cancelled = 0
        for order_id in list(self._pending_orders.keys()):
            if self.cancel_order(order_id):
                cancelled += 1
        return cancelled


class LiveTradingLoop:
    """
    實盤交易主循環

    整合 A-F + G，持續監控市場並執行交易
    """

    def __init__(
        self,
        executor: LiveExecutor,
        signal_logger: SignalLogger,
        risk_config: Optional[LiveRiskConfig] = None,
    ):
        self.executor = executor
        self.logger = signal_logger
        self.risk = risk_config or LiveRiskConfig()
        self._running = False
        self._last_account_check = datetime.min.replace(tzinfo=timezone.utc)

    def start(self, scan_interval_seconds: int = 10):
        """啟動交易循環"""
        self._running = True
        logger.info("=== Live Trading Loop Started ===")
        logger.info(
            f"Risk Config: min_edge={self.risk.min_edge_threshold}, max_pos={self.risk.max_open_positions}"
        )

        # 檢查憑證
        creds_ok, creds_msg = self.executor.check_credentials()
        if not creds_ok:
            logger.error(f"Cannot start: {creds_msg}")
            raise ExecutionError(creds_msg)

        logger.info(f"Credentials: {creds_msg}")

        try:
            while self._running:
                cycle_start = time.time()

                try:
                    self._run_cycle()
                except Exception as e:
                    logger.error(f"Cycle error: {e}", exc_info=True)

                # 等待下一次掃描
                elapsed = time.time() - cycle_start
                sleep_time = max(0, scan_interval_seconds - elapsed)
                if sleep_time > 0:
                    time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
            self.stop()

    def stop(self):
        """停止交易循環"""
        logger.info("=== Stopping Live Trading Loop ===")
        self._running = False

        # 取消所有待處理訂單
        cancelled = self.executor.cancel_all_pending()
        logger.info(f"Cancelled {cancelled} pending orders")

    def _run_cycle(self):
        """單次交易循環"""
        # 1. 輪詢待處理訂單
        for order_id in list(self.executor.get_pending_orders()):
            result = self.executor.poll_order_status(order_id)
            if result and result.is_filled:
                logger.info(
                    f"Trade filled: {result.side} {result.filled_size:.2f} @ {result.avg_fill_price:.4f}"
                )

        # 2. 定期檢查帳戶狀態（每分鐘）
        now = datetime.now(timezone.utc)
        if (now - self._last_account_check).total_seconds() > 60:
            try:
                account = self.executor.get_account_state()
                logger.info(
                    f"Account: {account.usdc_balance:.2f} USDC | Daily PnL: {account.daily_pnl:.2f} | Trades: {account.daily_trades}"
                )
                self._last_account_check = now

                # 每日停損檢查
                if (
                    account.daily_pnl
                    < -account.usdc_balance * self.risk.daily_loss_limit_pct
                ):
                    logger.error(f"DAILY LOSS LIMIT HIT! Stopping trading.")
                    self.stop()
                    return

            except Exception as e:
                logger.error(f"Account check failed: {e}")

    def execute_signal(
        self,
        market_def: MarketDefinition,
        ref_price: ReferencePrice,
        fair_prob: FairProbEstimate,
        observation: SignalObservation,
        yes_token_id: str,
        no_token_id: str,
        yes_ask: float,
        no_ask: float,
    ) -> LiveExecutionResult:
        """
        執行單個訊號（由外部策略調用）

        這是主要入口：策略發現機會後，調用此方法執行
        """
        return self.executor.execute_trade(
            market_def=market_def,
            ref_price=ref_price,
            fair_prob=fair_prob,
            observation=observation,
            yes_token_id=yes_token_id,
            no_token_id=no_token_id,
            yes_ask=yes_ask,
            no_ask=no_ask,
        )
