"""SettlementClaimer 測試。"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

import pytest
from py_builder_relayer_client.builder.derive import derive
from py_builder_relayer_client.config import get_contract_config
from py_builder_relayer_client.signer import Signer

from polymarket_arbitrage.settlement_claimer import (
    ClaimStatus,
    RedeemablePosition,
    SettlementClaimer,
)


class DummyResponse:
    """模擬 requests.Response。"""

    def __init__(self, payload: Any, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = json.dumps(payload) if payload is not None else ""

    def raise_for_status(self) -> None:
        """模擬 HTTP 錯誤拋出。"""
        if self.status_code >= 400:
            raise RuntimeError(f"http error: {self.status_code}")

    def json(self) -> Any:
        """回傳預設 JSON。"""
        return self._payload


@pytest.fixture
def configured_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """配置自動領取所需的最小環境。"""
    private_key = "0x" + ("11" * 32)
    signer = Signer(private_key, 137)
    safe_address = derive(signer.address(), get_contract_config(137).safe_factory)

    monkeypatch.setenv("FUNDER_ADDRESS", safe_address)
    monkeypatch.setenv("WALLET_ADDRESS", safe_address)
    monkeypatch.setenv("WALLET_PRIVATE_KEY", private_key)
    monkeypatch.setenv("RELAYER_API_KEY", "relayer-key")
    monkeypatch.setenv("RELAYER_API_KEY_ADDRESS", signer.address())
    monkeypatch.setenv("POLY_CLAIM_RELAYER_TYPE", "auto")


def test_fetch_redeemable_positions_dedupes_by_condition(
    tmp_path: Path,
    configured_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """同一個 condition 的多筆 redeemable 倉位應只保留一筆。"""
    responses: List[DummyResponse] = [
        DummyResponse(
            [
                {
                    "conditionId": "0x" + ("aa" * 32),
                    "marketId": "market-1",
                    "title": "BTC 15m UP/DOWN",
                    "asset": "token-yes",
                    "proxyWallet": "0xproxy",
                    "size": "12.5",
                    "redeemable": True,
                },
                {
                    "conditionId": "0x" + ("aa" * 32),
                    "marketId": "market-1",
                    "title": "BTC 15m UP/DOWN",
                    "asset": "token-no",
                    "proxyWallet": "0xproxy",
                    "size": "7.5",
                    "redeemable": True,
                },
            ]
        )
    ]

    def fake_request(*args: Any, **kwargs: Any) -> DummyResponse:
        return responses.pop(0)

    claimer = SettlementClaimer(db_path=str(tmp_path / "claims.db"))
    monkeypatch.setattr(claimer.session, "request", fake_request)

    positions = claimer.fetch_redeemable_positions()

    assert len(positions) == 1
    assert positions[0].condition_id == "0x" + ("aa" * 32)
    assert positions[0].market_id == "market-1"
    assert positions[0].question == "BTC 15m UP/DOWN"


def test_submit_claim_persists_result_and_repeat_submit_is_skipped(
    tmp_path: Path,
    configured_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """成功提交後應落庫，重複提交同一 condition 需被跳過。"""
    responses: List[DummyResponse] = [
        DummyResponse({"deployed": True}),
        DummyResponse({"nonce": 7}),
        DummyResponse({"transactionID": "txn-123", "transactionHash": "0xabc"}),
    ]

    def fake_request(*args: Any, **kwargs: Any) -> DummyResponse:
        return responses.pop(0)

    db_path = tmp_path / "claims.db"
    claimer = SettlementClaimer(db_path=str(db_path))
    monkeypatch.setattr(claimer.session, "request", fake_request)

    position = RedeemablePosition(
        condition_id="0x" + ("bb" * 32),
        market_id="market-2",
        question="ETH 1h UP/DOWN",
        token_id="token-yes",
        proxy_wallet="0xproxy",
        size=5.0,
        redeemable=True,
        raw_payload={"marketId": "market-2"},
    )

    submitted = claimer.submit_claim(position)
    skipped = claimer.submit_claim(position)
    skipped_again = claimer.submit_claim(position)

    assert submitted.status == ClaimStatus.SUBMITTED
    assert submitted.transaction_id == "txn-123"
    assert submitted.transaction_hash == "0xabc"
    assert submitted.safe_nonce == 7
    assert skipped.status == ClaimStatus.SKIPPED
    assert skipped_again.status == ClaimStatus.SKIPPED

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM settlement_claims")
    count = cursor.fetchone()[0]
    conn.close()

    assert count == 3


def test_refresh_pending_claims_updates_confirmed_state(
    tmp_path: Path,
    configured_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """pending 領取紀錄應可被 relayer 狀態回補為 confirmed。"""
    responses: List[DummyResponse] = [
        DummyResponse({"deployed": True}),
        DummyResponse({"nonce": 9}),
        DummyResponse({"transactionID": "txn-789", "transactionHash": None}),
        DummyResponse(
            [
                {
                    "id": "txn-789",
                    "state": "STATE_CONFIRMED",
                    "transactionHash": "0xdef",
                }
            ]
        ),
    ]

    def fake_request(*args: Any, **kwargs: Any) -> DummyResponse:
        return responses.pop(0)

    claimer = SettlementClaimer(db_path=str(tmp_path / "claims.db"))
    monkeypatch.setattr(claimer.session, "request", fake_request)

    position = RedeemablePosition(
        condition_id="0x" + ("cc" * 32),
        market_id="market-3",
        question="SOL 5m UP/DOWN",
        token_id="token-yes",
        proxy_wallet="0xproxy",
        size=3.0,
        redeemable=True,
        raw_payload={"marketId": "market-3"},
    )
    submitted = claimer.submit_claim(position)
    refreshed = claimer.refresh_pending_claims()

    assert submitted.status == ClaimStatus.SUBMITTED
    assert len(refreshed) == 1
    assert refreshed[0].status == ClaimStatus.CONFIRMED
    assert refreshed[0].transaction_hash == "0xdef"
    assert refreshed[0].completed_at is not None


def test_scan_and_claim_dry_run_only_fetches_positions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """dry run 只應掃描 positions，不需要 relayer 憑證，也不能提交交易。"""
    monkeypatch.setenv("FUNDER_ADDRESS", "0x1111111111111111111111111111111111111111")
    monkeypatch.delenv("RELAYER_API_KEY", raising=False)
    monkeypatch.delenv("RELAYER_API_KEY_ADDRESS", raising=False)
    monkeypatch.delenv("POLY_BUILDER_API_KEY", raising=False)
    monkeypatch.delenv("POLY_BUILDER_API_SECRET", raising=False)
    monkeypatch.delenv("POLY_BUILDER_API_PASSPHRASE", raising=False)
    monkeypatch.delenv("WALLET_PRIVATE_KEY", raising=False)
    calls: List[str] = []

    def fake_request(*args: Any, **kwargs: Any) -> DummyResponse:
        calls.append(kwargs.get("url", ""))
        return DummyResponse(
            [
                {
                    "conditionId": "0x" + ("dd" * 32),
                    "marketId": "market-4",
                    "title": "BTC 5m UP/DOWN",
                    "asset": "token-yes",
                    "proxyWallet": "0xproxy",
                    "size": "2",
                    "redeemable": True,
                }
            ]
        )

    claimer = SettlementClaimer(db_path=str(tmp_path / "claims.db"))
    monkeypatch.setattr(claimer.session, "request", fake_request)

    results = claimer.scan_and_claim(dry_run=True)

    assert len(results) == 1
    assert results[0].status == ClaimStatus.DRY_RUN
    assert results[0].condition_id == "0x" + ("dd" * 32)
    assert all("/submit" not in call for call in calls)


def test_build_auth_headers_prefers_relayer_api_key_over_builder(
    tmp_path: Path,
    configured_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """當 relayer API key 與 Builder 憑證同時存在時，應優先使用 relayer header。"""

    class _BuilderShouldNotBeCalled:
        """若此物件被呼叫，代表認證優先順序錯誤。"""

        def generate_builder_headers(self, *_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError("雙憑證情境不應退回 Builder 認證")

    monkeypatch.setenv("POLY_BUILDER_API_KEY", "builder-key")
    monkeypatch.setenv("POLY_BUILDER_API_SECRET", "builder-secret")
    monkeypatch.setenv("POLY_BUILDER_API_PASSPHRASE", "builder-passphrase")

    claimer = SettlementClaimer(db_path=str(tmp_path / "claims.db"))
    claimer._builder_config = _BuilderShouldNotBeCalled()

    headers = claimer._build_auth_headers("POST", "/submit", {"type": "PROXY"})

    assert headers == {
        "RELAYER_API_KEY": "relayer-key",
        "RELAYER_API_KEY_ADDRESS": claimer._relayer_api_key_address,
    }


def test_build_auth_headers_falls_back_to_builder_when_relayer_key_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """當 relayer API key 缺失時，應退回 Builder 認證標頭。"""

    class _DummyBuilderHeaders:
        """模擬 Builder header 物件。"""

        def to_dict(self) -> Dict[str, str]:
            return {"POLY_BUILDER_API_KEY": "builder-key"}

    class _DummyBuilderConfig:
        """模擬 Builder 簽名器。"""

        def generate_builder_headers(self, *_args: Any, **_kwargs: Any) -> _DummyBuilderHeaders:
            return _DummyBuilderHeaders()

    monkeypatch.delenv("RELAYER_API_KEY", raising=False)
    monkeypatch.delenv("RELAYER_API_KEY_ADDRESS", raising=False)
    monkeypatch.setenv("POLY_BUILDER_API_KEY", "builder-key")
    monkeypatch.setenv("POLY_BUILDER_API_SECRET", "builder-secret")
    monkeypatch.setenv("POLY_BUILDER_API_PASSPHRASE", "builder-passphrase")

    claimer = SettlementClaimer(db_path=str(tmp_path / "claims.db"))
    claimer._builder_config = _DummyBuilderConfig()

    headers = claimer._build_auth_headers("POST", "/submit", {"type": "SAFE"})

    assert headers == {"POLY_BUILDER_API_KEY": "builder-key"}


def test_submit_claim_uses_proxy_path_when_claim_account_matches_proxy_wallet(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """當 claim_account 對應 proxy wallet 時，應改走 PROXY relayer 路徑。"""
    private_key = "0x" + ("22" * 32)
    signer = Signer(private_key, 137)

    bootstrap = SettlementClaimer(db_path=None, private_key=private_key, claim_account=signer.address())
    proxy_wallet = bootstrap._get_expected_proxy_wallet()

    monkeypatch.setenv("WALLET_PRIVATE_KEY", private_key)
    monkeypatch.setenv("FUNDER_ADDRESS", proxy_wallet)
    monkeypatch.setenv("WALLET_ADDRESS", proxy_wallet)
    monkeypatch.setenv("RELAYER_API_KEY", "relayer-key")
    monkeypatch.setenv("RELAYER_API_KEY_ADDRESS", signer.address())
    monkeypatch.setenv("POLY_CLAIM_RELAYER_TYPE", "auto")

    captured_submit_payload: Dict[str, Any] = {}
    captured_urls: List[str] = []

    def fake_request(*args: Any, **kwargs: Any) -> DummyResponse:
        url = kwargs.get("url", "")
        captured_urls.append(url)
        if url.endswith("/relay-payload"):
            return DummyResponse({"nonce": 13, "address": "0x3333333333333333333333333333333333333333"})
        if url.endswith("/submit"):
            captured_submit_payload.update(kwargs.get("json") or {})
            return DummyResponse({"transactionID": "txn-proxy", "transactionHash": "0x456"})
        raise AssertionError(f"unexpected url: {url}")

    claimer = SettlementClaimer(db_path=str(tmp_path / "claims.db"))
    monkeypatch.setattr(claimer.session, "request", fake_request)

    position = RedeemablePosition(
        condition_id="0x" + ("ee" * 32),
        market_id="market-5",
        question="BTC 15m UP/DOWN",
        token_id="token-yes",
        proxy_wallet=proxy_wallet,
        size=1.0,
        redeemable=True,
        raw_payload={"marketId": "market-5"},
    )

    submitted = claimer.submit_claim(position)

    assert submitted.status == ClaimStatus.SUBMITTED
    assert submitted.transaction_id == "txn-proxy"
    assert submitted.safe_nonce == 13
    assert submitted.raw_response["submission_type"] == "proxy"
    assert captured_submit_payload["type"] == "PROXY"
    assert captured_submit_payload["proxyWallet"] == proxy_wallet
    assert captured_submit_payload["signatureParams"]["relay"] == "0x3333333333333333333333333333333333333333"
    assert captured_submit_payload["signatureParams"]["relayHub"]
    assert captured_submit_payload["signatureParams"]["gasLimit"]
    assert all("/deployed" not in url for url in captured_urls)
    assert all("type=SAFE" not in url for url in captured_urls)


def test_submit_claim_fails_when_claim_account_matches_neither_safe_nor_proxy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """當 claim_account 與 signer 推導地址不一致時，應拒絕提交。"""
    private_key = "0x" + ("33" * 32)
    signer = Signer(private_key, 137)

    monkeypatch.setenv("WALLET_PRIVATE_KEY", private_key)
    monkeypatch.setenv("FUNDER_ADDRESS", "0x9999999999999999999999999999999999999999")
    monkeypatch.setenv("WALLET_ADDRESS", "0x9999999999999999999999999999999999999999")
    monkeypatch.setenv("RELAYER_API_KEY", "relayer-key")
    monkeypatch.setenv("RELAYER_API_KEY_ADDRESS", signer.address())
    monkeypatch.setenv("POLY_CLAIM_RELAYER_TYPE", "auto")

    claimer = SettlementClaimer(db_path=str(tmp_path / "claims.db"))

    position = RedeemablePosition(
        condition_id="0x" + ("ff" * 32),
        market_id="market-6",
        question="ETH 15m UP/DOWN",
        token_id="token-yes",
        proxy_wallet="",
        size=2.0,
        redeemable=True,
        raw_payload={"marketId": "market-6"},
    )

    failed = claimer.submit_claim(position)

    assert failed.status == ClaimStatus.FAILED
    assert "claim_account 無法對應 signer 推導的 safe 或 proxy wallet" in (failed.error_message or "")
