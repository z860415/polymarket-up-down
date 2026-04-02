"""`research_pipeline.py` 研究層測試。"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

from polymarket_arbitrage.research_pipeline import ResearchPipeline
from polymarket_arbitrage.signal_logger import SignalLogger
from polymarket_arbitrage.integrated_scanner_v2 import MarketTradability, ParsedMarket
from polymarket_arbitrage.updown_tail_pricer import (
    MarketRuntimeSnapshot,
    UpDownTailPricer,
)

from datetime import datetime, timedelta, timezone
import pytest


def test_fetch_orderbook_uses_public_clob_client_and_normalizes_levels(
    tmp_path,
) -> None:
    """研究層應使用官方 SDK 公開 client，而非舊的 `/book/{token}` 路徑。"""
    signal_logger = SignalLogger(str(tmp_path / "research.db"))
    pipeline = ResearchPipeline(signal_logger=signal_logger)

    mock_client = MagicMock()
    mock_client.get_order_book.return_value = SimpleNamespace(
        bids=[SimpleNamespace(price="0.41", size="100")],
        asks=[SimpleNamespace(price="0.43", size="200")],
    )
    pipeline._public_clob_client = mock_client

    result = __import__("asyncio").run(pipeline._fetch_orderbook("token-123"))

    assert result is not None
    assert len(result["bids"]) == 1
    assert len(result["asks"]) == 1
    mock_client.get_order_book.assert_called_once_with("token-123")


def test_should_include_market_allows_above_below_without_timeframe(tmp_path) -> None:
    """顯式允許 above_below 時，不應因 timeframe=None 被入口過濾掉。"""
    signal_logger = SignalLogger(str(tmp_path / "research.db"))
    pipeline = ResearchPipeline(signal_logger=signal_logger)
    parsed = ParsedMarket(
        raw_event={},
        raw_market={},
        asset="BTC",
        style="ABOVE_BELOW",
        timeframe=None,
        strike=150000.0,
        expiry=datetime.now(timezone.utc),
        is_crypto=True,
        is_short_term=False,
    )

    allowed = pipeline._should_include_market(
        parsed=parsed,
        tradability=None,
        allowed_timeframes={"1h", "4h", "1d"},
        allowed_assets={"BTC"},
        allowed_styles={"above_below"},
    )

    assert allowed is True


def test_should_include_market_rejects_expired_up_down(tmp_path) -> None:
    """已過期的 `UP_DOWN` 市場應在入口就被排除，避免舊 event 噪音進研究。"""
    signal_logger = SignalLogger(str(tmp_path / "research.db"))
    pipeline = ResearchPipeline(signal_logger=signal_logger)
    parsed = ParsedMarket(
        raw_event={},
        raw_market={},
        asset="BTC",
        style="UP_DOWN",
        timeframe="5m",
        strike=None,
        expiry=datetime.now(timezone.utc) - timedelta(seconds=5),
        is_crypto=True,
        is_short_term=True,
    )

    allowed = pipeline._should_include_market(
        parsed=parsed,
        tradability=None,
        allowed_timeframes={"5m"},
        allowed_assets={"BTC"},
        allowed_styles={"up_down"},
    )

    assert allowed is False


def test_estimate_effective_buy_price_uses_weighted_average(tmp_path) -> None:
    """單邊有效成交價應按固定 notional 做逐檔加權平均。"""
    signal_logger = SignalLogger(str(tmp_path / "research.db"))
    pipeline = ResearchPipeline(signal_logger=signal_logger)

    effective_price = pipeline._estimate_effective_buy_price(
        [
            {"price": "0.50", "size": "1"},
            {"price": "0.60", "size": "2"},
        ],
        target_notional_usdc=1.0,
    )

    assert effective_price == pytest.approx(0.5454545, abs=1e-6)
    assert pipeline._calculate_execution_cost_pct(
        0.50, effective_price
    ) == pytest.approx(0.0833333, abs=1e-6)


def test_estimate_effective_buy_price_returns_none_when_depth_insufficient(
    tmp_path,
) -> None:
    """若 ask 深度不足以覆蓋目標 notional，應回傳缺價。"""
    signal_logger = SignalLogger(str(tmp_path / "research.db"))
    pipeline = ResearchPipeline(signal_logger=signal_logger)

    effective_price = pipeline._estimate_effective_buy_price(
        [
            {"price": "0.50", "size": "1"},
        ],
        target_notional_usdc=1.0,
    )

    assert effective_price is None


def test_updown_pricer_applies_one_sided_execution_costs() -> None:
    """尾盤定價器需按 YES / NO 各自成本計算淨 edge。"""
    pricer = UpDownTailPricer()
    snapshot = MarketRuntimeSnapshot(
        market_id="m1",
        asset="BTC",
        timeframe="5m",
        anchor_price=100.0,
        spot_price=100.0,
        tau_seconds=20.0,
        sigma_tail=0.5,
        yes_bid=0.44,
        yes_ask=0.45,
        no_bid=0.44,
        no_ask=0.45,
        best_depth=600.0,
        fees_enabled=True,
        window_state="attack",
    )

    estimate = pricer.estimate(
        snapshot,
        yes_execution_cost_pct=0.02,
        no_execution_cost_pct=0.20,
    )

    assert estimate.slippage_cost_up < estimate.slippage_cost_down
    assert estimate.selected_side == "YES"
    assert estimate.slippage_cost == estimate.slippage_cost_up


def test_updown_pricer_uses_relaxed_15m_and_4h_thresholds() -> None:
    """15m 與 4h 應使用本輪放寬後的研究門檻。"""
    pricer = UpDownTailPricer()

    assert pricer.minimum_lead_z("15m") == pytest.approx(1.5)
    assert pricer.minimum_lead_z("4h") == pytest.approx(1.4)
    assert pricer.minimum_net_edge("15m") == pytest.approx(0.04)
    assert pricer.minimum_net_edge("4h") == pytest.approx(0.03)


def test_analyze_up_down_market_uses_selected_side_effective_cost(
    tmp_path, monkeypatch
) -> None:
    """UP_DOWN 需允許單邊有深度時繼續分析，並以選定方向成本作為 spread。"""
    signal_logger = SignalLogger(str(tmp_path / "research.db"))
    pipeline = ResearchPipeline(signal_logger=signal_logger, max_spread_pct=0.1)

    parsed = ParsedMarket(
        raw_event={},
        raw_market={},
        asset="BTC",
        style="UP_DOWN",
        timeframe="5m",
        strike=None,
        expiry=datetime.now(timezone.utc) + timedelta(seconds=20),
        is_crypto=True,
        is_short_term=True,
    )
    market = {
        "id": "m1",
        "question": "Bitcoin Up or Down - Test",
        "description": "",
        "feesEnabled": True,
    }
    tradability = MarketTradability(
        market_id="m1",
        slug="btc-up-down-test",
        is_active=True,
        is_closed=False,
        is_archived=False,
        status_reject=None,
        enable_orderbook=True,
        has_token_ids=True,
        yes_token="yes-token",
        no_token="no-token",
        orderbook_reject=None,
        price_available=True,
        midpoint_available=True,
        book_available=True,
        clob_reject=None,
        is_clob_eligible=True,
        is_book_verified=True,
        volume=1000.0,
    )

    async def fake_fetch_orderbook(token_id: str):
        if token_id == "yes-token":
            return {
                "bids": [{"price": "0.49", "size": "10"}],
                "asks": [{"price": "0.50", "size": "4"}],
            }
        return {
            "bids": [{"price": "0.49", "size": "10"}],
            "asks": [],
        }

    monkeypatch.setattr(pipeline, "_fetch_orderbook", fake_fetch_orderbook)
    monkeypatch.setattr(pipeline.binance_client, "get_spot_price", lambda symbol: 101.0)
    monkeypatch.setattr(
        pipeline.binance_client, "calculate_volatility", lambda symbol, *args: 0.01
    )
    monkeypatch.setattr(
        pipeline.anchor_store,
        "capture_anchor",
        lambda market_definition: SimpleNamespace(
            anchor_price=100.0, anchor_timestamp=datetime.now(timezone.utc)
        ),
    )
    monkeypatch.setattr(
        pipeline.tail_pricer, "minimum_lead_z", lambda timeframe, *a: 0.0
    )

    candidate, reject_reason, reject_detail = asyncio.run(
        pipeline._analyze_up_down_market(parsed, market, tradability)
    )

    assert reject_reason is None
    assert reject_detail is None
    assert candidate is not None
    assert candidate.opportunity.selected_side == "YES"
    assert candidate.opportunity.spread_pct == pytest.approx(0.0, abs=1e-9)


def test_analyze_up_down_market_rejects_when_both_sides_lack_effective_ask(
    tmp_path, monkeypatch
) -> None:
    """若兩邊都沒有足夠 ask 深度，UP_DOWN 應在早期直接拒絕。"""
    signal_logger = SignalLogger(str(tmp_path / "research.db"))
    pipeline = ResearchPipeline(signal_logger=signal_logger, max_spread_pct=0.1)

    parsed = ParsedMarket(
        raw_event={},
        raw_market={},
        asset="BTC",
        style="UP_DOWN",
        timeframe="5m",
        strike=None,
        expiry=datetime.now(timezone.utc) + timedelta(seconds=20),
        is_crypto=True,
        is_short_term=True,
    )
    market = {
        "id": "m2",
        "question": "Bitcoin Up or Down - Missing Ask",
        "description": "",
        "feesEnabled": True,
    }
    tradability = MarketTradability(
        market_id="m2",
        slug="btc-up-down-missing-ask",
        is_active=True,
        is_closed=False,
        is_archived=False,
        status_reject=None,
        enable_orderbook=True,
        has_token_ids=True,
        yes_token="yes-token",
        no_token="no-token",
        orderbook_reject=None,
        price_available=True,
        midpoint_available=True,
        book_available=True,
        clob_reject=None,
        is_clob_eligible=True,
        is_book_verified=True,
        volume=1000.0,
    )

    async def fake_fetch_orderbook(_: str):
        return {
            "bids": [{"price": "0.49", "size": "10"}],
            "asks": [{"price": "0.50", "size": "1"}],
        }

    monkeypatch.setattr(pipeline, "_fetch_orderbook", fake_fetch_orderbook)

    candidate, reject_reason, reject_detail = asyncio.run(
        pipeline._analyze_up_down_market(parsed, market, tradability)
    )

    assert candidate is None
    assert reject_reason == "ask_quote_missing"
    assert reject_detail["effective_cost_notional_usdc"] == pytest.approx(1.0)


def test_analyze_up_down_market_observe_rejects_before_expensive_fetches(
    tmp_path, monkeypatch
) -> None:
    """開盤但未進尾盤時，UP_DOWN 應以前置 `window_not_open` 早退。"""
    signal_logger = SignalLogger(str(tmp_path / "research.db"))
    pipeline = ResearchPipeline(signal_logger=signal_logger, max_spread_pct=0.1)

    parsed = ParsedMarket(
        raw_event={},
        raw_market={},
        asset="BTC",
        style="UP_DOWN",
        timeframe="5m",
        strike=None,
        expiry=datetime.now(timezone.utc) + timedelta(seconds=200),
        is_crypto=True,
        is_short_term=True,
    )
    market = {
        "id": "m3",
        "question": "Bitcoin Up or Down - Window Closed",
        "description": "",
        "feesEnabled": True,
    }
    tradability = MarketTradability(
        market_id="m3",
        slug="btc-up-down-window-closed",
        is_active=True,
        is_closed=False,
        is_archived=False,
        status_reject=None,
        enable_orderbook=True,
        has_token_ids=True,
        yes_token="yes-token",
        no_token="no-token",
        orderbook_reject=None,
        price_available=True,
        midpoint_available=True,
        book_available=True,
        clob_reject=None,
        is_clob_eligible=True,
        is_book_verified=True,
        volume=1000.0,
    )

    calls = {
        "orderbook": 0,
        "spot": 0,
        "anchor": 0,
        "volatility": 0,
    }

    async def fake_fetch_orderbook(token_id: str):
        calls["orderbook"] += 1
        return {
            "bids": [{"price": "0.49", "size": "10"}],
            "asks": [{"price": "0.50", "size": "4"}],
        }

    def fake_spot_price(*args, **kwargs):
        calls["spot"] += 1
        return 101.0

    def fake_capture_anchor(*args, **kwargs):
        calls["anchor"] += 1
        return SimpleNamespace(
            anchor_price=100.0, anchor_timestamp=datetime.now(timezone.utc)
        )

    def fake_volatility(*args, **kwargs):
        calls["volatility"] += 1
        return 0.01

    monkeypatch.setattr(pipeline, "_fetch_orderbook", fake_fetch_orderbook)
    monkeypatch.setattr(pipeline.binance_client, "get_spot_price", fake_spot_price)
    monkeypatch.setattr(
        pipeline.binance_client, "calculate_volatility", fake_volatility
    )
    monkeypatch.setattr(pipeline.anchor_store, "capture_anchor", fake_capture_anchor)
    monkeypatch.setattr(
        pipeline.tail_pricer, "minimum_lead_z", lambda timeframe, *a: 0.0
    )
    monkeypatch.setattr(
        pipeline.tail_pricer, "minimum_net_edge", lambda timeframe, *a: -1.0
    )

    candidate, reject_reason, reject_detail = asyncio.run(
        pipeline._analyze_up_down_market(parsed, market, tradability)
    )

    assert candidate is None
    assert reject_reason == "window_not_open"
    assert reject_detail["window_state"] == "observe"
    assert reject_detail["window_label"] == "已開盤未進尾盤"
    assert reject_detail["seconds_to_armed"] > 0
    assert reject_detail["seconds_to_attack"] > 0
    assert calls["orderbook"] == 0
    assert calls["spot"] == 0
    assert calls["anchor"] == 0
    assert calls["volatility"] == 0


def test_run_allows_observe_up_down_markets_into_analyze(tmp_path, monkeypatch) -> None:
    """主研究線應讓開盤中的 `observe` 市場進入 `_analyze_market()`，由研究層回 `window_not_open`。"""
    signal_logger = SignalLogger(str(tmp_path / "research.db"))
    pipeline = ResearchPipeline(signal_logger=signal_logger, max_spread_pct=0.1)
    analyze_calls = {"count": 0}

    parsed = ParsedMarket(
        raw_event={},
        raw_market={},
        asset="BTC",
        style="UP_DOWN",
        timeframe="5m",
        strike=None,
        expiry=datetime.now(timezone.utc) + timedelta(seconds=200),
        is_crypto=True,
        is_short_term=True,
    )
    market = {
        "id": "m4",
        "question": "Bitcoin Up or Down - Observe",
        "description": "",
        "feesEnabled": True,
    }
    tradability = MarketTradability(
        market_id="m4",
        slug="btc-up-down-observe",
        is_active=True,
        is_closed=False,
        is_archived=False,
        status_reject=None,
        enable_orderbook=True,
        has_token_ids=True,
        yes_token="yes-token",
        no_token="no-token",
        orderbook_reject=None,
        price_available=True,
        midpoint_available=True,
        book_available=True,
        clob_reject=None,
        is_clob_eligible=True,
        is_book_verified=True,
        volume=1000.0,
    )

    async def fake_get_all_events(limit: int = 200, allowed_styles=None):
        return [{"id": "event-1", "markets": [market]}]

    async def fake_check_tradability(_: dict, verify_depth: bool = True):
        return tradability

    async def fake_verify_orderbook_depth(tradability_input):
        return tradability_input

    async def fake_analyze_market(*args, **kwargs):
        analyze_calls["count"] += 1
        return (
            None,
            "window_not_open",
            {"reason": "window_not_open", "question": market["question"]},
        )

    monkeypatch.setattr(pipeline.scanner, "get_all_events", fake_get_all_events)
    monkeypatch.setattr(
        pipeline.scanner,
        "expand_markets",
        lambda events, allowed_styles=None: [(events[0], market)],
    )
    monkeypatch.setattr(
        pipeline.scanner, "parse_market", lambda event, raw_market: (parsed, None, {})
    )
    monkeypatch.setattr(pipeline.scanner, "check_tradability", fake_check_tradability)
    monkeypatch.setattr(
        pipeline.scanner,
        "verify_orderbook_depth",
        fake_verify_orderbook_depth,
    )
    monkeypatch.setattr(pipeline, "_analyze_market", fake_analyze_market)

    try:
        result = asyncio.run(
            pipeline.run(
                limit_events=1,
                allowed_assets=["BTC"],
                allowed_styles=["up_down"],
                allowed_timeframes=["5m"],
            )
        )
    finally:
        asyncio.run(pipeline.close())

    assert analyze_calls["count"] == 1
    assert result.analyzed_market_count == 1
    assert result.opportunity_count == 0
    assert result.reject_summary["window_not_open"] == 1
    assert len(result.reject_samples) == 1


def test_run_requires_book_verified_before_analyze(tmp_path, monkeypatch) -> None:
    """研究主線不得再讓只有 price / midpoint、但缺少雙邊深度的市場進分析。"""
    signal_logger = SignalLogger(str(tmp_path / "research.db"))
    pipeline = ResearchPipeline(signal_logger=signal_logger, max_spread_pct=0.1)
    analyze_calls = {"count": 0}

    parsed = ParsedMarket(
        raw_event={},
        raw_market={},
        asset="BTC",
        style="UP_DOWN",
        timeframe="5m",
        strike=None,
        expiry=datetime.now(timezone.utc) + timedelta(seconds=40),
        is_crypto=True,
        is_short_term=True,
    )
    market = {
        "id": "m-book-gate",
        "question": "Bitcoin Up or Down - Need Book",
        "description": "",
        "feesEnabled": True,
    }
    tradability = MarketTradability(
        market_id="m-book-gate",
        slug="btc-up-down-no-book",
        is_active=True,
        is_closed=False,
        is_archived=False,
        status_reject=None,
        enable_orderbook=True,
        has_token_ids=True,
        yes_token="yes-token",
        no_token="no-token",
        orderbook_reject=None,
        price_available=True,
        midpoint_available=True,
        book_available=False,
        clob_reject=None,
        is_clob_eligible=True,
        is_book_verified=False,
        volume=1000.0,
    )

    async def fake_get_all_events(limit: int = 200, allowed_styles=None):
        return [{"id": "event-1", "markets": [market]}]

    async def fake_check_tradability(_: dict, verify_depth: bool = True):
        return tradability

    async def fake_verify_orderbook_depth(tradability_input):
        return tradability_input

    async def fake_analyze_market(*args, **kwargs):
        analyze_calls["count"] += 1
        return None, "unexpected", {"reason": "unexpected"}

    monkeypatch.setattr(pipeline.scanner, "get_all_events", fake_get_all_events)
    monkeypatch.setattr(
        pipeline.scanner,
        "expand_markets",
        lambda events, allowed_styles=None: [(events[0], market)],
    )
    monkeypatch.setattr(
        pipeline.scanner, "parse_market", lambda event, raw_market: (parsed, None, {})
    )
    monkeypatch.setattr(pipeline.scanner, "check_tradability", fake_check_tradability)
    monkeypatch.setattr(
        pipeline.scanner,
        "verify_orderbook_depth",
        fake_verify_orderbook_depth,
    )
    monkeypatch.setattr(pipeline, "_analyze_market", fake_analyze_market)

    try:
        result = asyncio.run(
            pipeline.run(
                limit_events=1,
                allowed_assets=["BTC"],
                allowed_styles=["up_down"],
                allowed_timeframes=["5m"],
            )
        )
    finally:
        asyncio.run(pipeline.close())

    assert analyze_calls["count"] == 0
    assert result.pricing_verified_count == 1
    assert result.analyzed_market_count == 0


def test_run_delays_depth_verification_until_after_filter(tmp_path, monkeypatch) -> None:
    """第二段深度驗證只應發生在排序與窗口過濾後留下的市場。"""
    signal_logger = SignalLogger(str(tmp_path / "research.db"))
    pipeline = ResearchPipeline(signal_logger=signal_logger, max_spread_pct=0.1)
    verify_calls: list[str] = []

    parsed_keep = ParsedMarket(
        raw_event={},
        raw_market={},
        asset="BTC",
        style="UP_DOWN",
        timeframe="5m",
        strike=None,
        expiry=datetime.now(timezone.utc) + timedelta(seconds=40),
        is_crypto=True,
        is_short_term=True,
    )
    parsed_drop = ParsedMarket(
        raw_event={},
        raw_market={},
        asset="ETH",
        style="UP_DOWN",
        timeframe="5m",
        strike=None,
        expiry=datetime.now(timezone.utc) + timedelta(seconds=50),
        is_crypto=True,
        is_short_term=True,
    )
    market_keep = {
        "id": "keep-market",
        "question": "Bitcoin Up or Down - Keep",
        "description": "",
        "feesEnabled": True,
    }
    market_drop = {
        "id": "drop-market",
        "question": "Ethereum Up or Down - Drop",
        "description": "",
        "feesEnabled": True,
    }
    base_tradability = lambda market_id, slug, yes_token, no_token: MarketTradability(
        market_id=market_id,
        slug=slug,
        is_active=True,
        is_closed=False,
        is_archived=False,
        status_reject=None,
        enable_orderbook=True,
        has_token_ids=True,
        yes_token=yes_token,
        no_token=no_token,
        orderbook_reject=None,
        price_available=True,
        midpoint_available=True,
        book_available=False,
        clob_reject=None,
        is_clob_eligible=True,
        is_book_verified=False,
        volume=1000.0,
    )

    async def fake_get_all_events(limit: int = 200, allowed_styles=None):
        return [{"id": "event-1", "markets": [market_keep, market_drop]}]

    def fake_expand_markets(events, allowed_styles=None):
        return [(events[0], market_keep), (events[0], market_drop)]

    def fake_parse_market(event, raw_market):
        if raw_market["id"] == "keep-market":
            return parsed_keep, None, {}
        return parsed_drop, None, {}

    async def fake_check_tradability(raw_market: dict, verify_depth: bool = True):
        if raw_market["id"] == "keep-market":
            return base_tradability("keep-market", "keep", "yes-keep", "no-keep")
        return base_tradability("drop-market", "drop", "yes-drop", "no-drop")

    def fake_prioritize(tradable_markets, allowed_styles=None, now=None):
        return tradable_markets

    def fake_filter(tradable_markets, allowed_styles=None, now=None):
        kept = [item for item in tradable_markets if item[1]["id"] == "keep-market"]
        return kept, []

    async def fake_verify_orderbook_depth(tradability_input):
        verify_calls.append(tradability_input.market_id)
        tradability_input.book_available = True
        tradability_input.is_book_verified = True
        tradability_input.yes_orderbook = {"bids": [], "asks": [{"price": "0.50", "size": "10"}]}
        tradability_input.no_orderbook = {"bids": [], "asks": [{"price": "0.50", "size": "10"}]}
        return tradability_input

    async def fake_analyze_market(parsed, market, tradability):
        return None, "window_not_open", {"reason": "window_not_open", "market_id": tradability.market_id}

    monkeypatch.setattr(pipeline.scanner, "get_all_events", fake_get_all_events)
    monkeypatch.setattr(pipeline.scanner, "expand_markets", fake_expand_markets)
    monkeypatch.setattr(pipeline.scanner, "parse_market", fake_parse_market)
    monkeypatch.setattr(pipeline.scanner, "check_tradability", fake_check_tradability)
    monkeypatch.setattr(
        pipeline.scanner,
        "prioritize_markets_for_analysis",
        fake_prioritize,
    )
    monkeypatch.setattr(
        pipeline.scanner,
        "filter_live_markets_for_analysis",
        fake_filter,
    )
    monkeypatch.setattr(
        pipeline.scanner,
        "verify_orderbook_depth",
        fake_verify_orderbook_depth,
    )
    monkeypatch.setattr(pipeline, "_analyze_market", fake_analyze_market)

    try:
        result = asyncio.run(
            pipeline.run(
                limit_events=2,
                allowed_assets=["BTC", "ETH"],
                allowed_styles=["up_down"],
                allowed_timeframes=["5m"],
            )
        )
    finally:
        asyncio.run(pipeline.close())

    assert verify_calls == ["keep-market"]
    assert result.pricing_verified_count == 2
    assert result.analyzed_market_count == 1


def test_get_market_orderbooks_prefers_tradability_snapshot(tmp_path) -> None:
    """research 應優先重用 scanner 已驗證的雙邊深度快照。"""
    signal_logger = SignalLogger(str(tmp_path / "research.db"))
    pipeline = ResearchPipeline(signal_logger=signal_logger)
    tradability = MarketTradability(
        market_id="m-snapshot",
        slug="snapshot-market",
        is_active=True,
        is_closed=False,
        is_archived=False,
        status_reject=None,
        enable_orderbook=True,
        has_token_ids=True,
        yes_token="yes-token",
        no_token="no-token",
        orderbook_reject=None,
        price_available=True,
        midpoint_available=True,
        book_available=True,
        clob_reject=None,
        is_clob_eligible=True,
        is_book_verified=True,
        volume=100.0,
        yes_orderbook={"bids": [{"price": "0.48", "size": "5"}], "asks": []},
        no_orderbook={"bids": [{"price": "0.52", "size": "5"}], "asks": []},
    )

    async def fail_fetch(_: str):
        raise AssertionError("不應在已有快照時重抓 order book")

    pipeline._fetch_orderbook = fail_fetch  # type: ignore[method-assign]

    yes_orderbook, no_orderbook = asyncio.run(pipeline._get_market_orderbooks(tradability))

    assert yes_orderbook == tradability.yes_orderbook
    assert no_orderbook == tradability.no_orderbook


def test_run_includes_prefiltered_rejects_in_samples(tmp_path, monkeypatch) -> None:
    """前置過濾拒絕樣本也應進入 reject_samples，供監控頁顯示。"""
    signal_logger = SignalLogger(str(tmp_path / "research.db"))
    pipeline = ResearchPipeline(signal_logger=signal_logger, max_spread_pct=0.1)

    async def fake_get_all_events(limit: int = 200, allowed_styles=None):
        return []

    monkeypatch.setattr(pipeline.scanner, "get_all_events", fake_get_all_events)
    monkeypatch.setattr(
        pipeline.scanner,
        "expand_markets",
        lambda events, allowed_styles=None: [],
    )
    monkeypatch.setattr(
        pipeline.scanner,
        "prioritize_markets_for_analysis",
        lambda tradable_markets, allowed_styles=None, now=None: tradable_markets,
    )
    monkeypatch.setattr(
        pipeline.scanner,
        "filter_live_markets_for_analysis",
        lambda tradable_markets, allowed_styles=None, now=None: (
            [],
            [
                {
                    "reason": "market_not_open_yet",
                    "market_id": "future-1",
                    "question": "Bitcoin Up or Down - Future",
                    "window_state": "not_open",
                }
            ],
        ),
    )

    try:
        result = asyncio.run(
            pipeline.run(
                limit_events=1,
                allowed_assets=["BTC"],
                allowed_styles=["up_down"],
                allowed_timeframes=["5m"],
            )
        )
    finally:
        asyncio.run(pipeline.close())

    assert result.reject_summary["market_not_open_yet"] == 1
    assert len(result.reject_samples) == 1
    assert result.reject_samples[0]["reason"] == "market_not_open_yet"


def test_run_passes_normalized_styles_to_discovery(tmp_path, monkeypatch) -> None:
    """研究主線應把 style 條件傳給 discovery，讓 scanner 觸發合併來源邏輯。"""
    signal_logger = SignalLogger(str(tmp_path / "research.db"))
    pipeline = ResearchPipeline(signal_logger=signal_logger, max_spread_pct=0.1)
    recorded: dict[str, object] = {}

    async def fake_get_all_events(limit: int = 200, allowed_styles=None):
        recorded["limit"] = limit
        recorded["allowed_styles"] = allowed_styles
        return []

    monkeypatch.setattr(pipeline.scanner, "get_all_events", fake_get_all_events)

    try:
        result = asyncio.run(
            pipeline.run(
                limit_events=3,
                allowed_assets=["BTC"],
                allowed_styles=["up_down"],
                allowed_timeframes=["5m"],
            )
        )
    finally:
        asyncio.run(pipeline.close())

    assert result.scanned_event_count == 0
    assert recorded["limit"] == 3
    assert recorded["allowed_styles"] == {"up_down"}


def test_run_reuses_scanner_session_until_close(tmp_path) -> None:
    """同一個研究 pipeline 連續多輪 run 應重用同一個 scanner session。"""
    signal_logger = SignalLogger(str(tmp_path / "research.db"))
    pipeline = ResearchPipeline(signal_logger=signal_logger, max_spread_pct=0.1)

    class FakeScanner:
        def __init__(self) -> None:
            self.enter_count = 0
            self.exit_count = 0

        async def __aenter__(self):
            self.enter_count += 1
            return self

        async def __aexit__(self, exc_type, exc, tb):
            self.exit_count += 1

        async def get_all_events(self, limit: int = 200, allowed_styles=None):
            return []

        def expand_markets(self, events, allowed_styles=None):
            return []

        def prioritize_markets_for_analysis(
            self, tradable_markets, allowed_styles=None, now=None
        ):
            return tradable_markets

        def filter_live_markets_for_analysis(
            self, tradable_markets, allowed_styles=None, now=None
        ):
            return tradable_markets, []

    fake_scanner = FakeScanner()
    pipeline.scanner = fake_scanner

    asyncio.run(pipeline.run(limit_events=1, allowed_styles=["up_down"]))
    asyncio.run(pipeline.run(limit_events=1, allowed_styles=["up_down"]))

    assert fake_scanner.enter_count == 1
    assert fake_scanner.exit_count == 0

    asyncio.run(pipeline.close())

    assert fake_scanner.exit_count == 1


def test_analyze_up_down_market_reuses_market_data_cache_within_ttl(
    tmp_path, monkeypatch
) -> None:
    """短 TTL 內重複分析同資產時，現貨價與波動率查詢應重用快取。"""
    signal_logger = SignalLogger(str(tmp_path / "research.db"))
    pipeline = ResearchPipeline(
        signal_logger=signal_logger,
        max_spread_pct=0.1,
        market_data_cache_ttl_seconds=30.0,
    )

    parsed = ParsedMarket(
        raw_event={},
        raw_market={},
        asset="BTC",
        style="UP_DOWN",
        timeframe="5m",
        strike=None,
        expiry=datetime.now(timezone.utc) + timedelta(seconds=20),
        is_crypto=True,
        is_short_term=True,
    )
    market = {
        "id": "m-cache",
        "question": "Bitcoin Up or Down - Cache",
        "description": "",
        "feesEnabled": True,
    }
    tradability = MarketTradability(
        market_id="m-cache",
        slug="btc-up-down-cache",
        is_active=True,
        is_closed=False,
        is_archived=False,
        status_reject=None,
        enable_orderbook=True,
        has_token_ids=True,
        yes_token="yes-token",
        no_token="no-token",
        orderbook_reject=None,
        price_available=True,
        midpoint_available=True,
        book_available=True,
        clob_reject=None,
        is_clob_eligible=True,
        is_book_verified=True,
        volume=1000.0,
    )

    calls = {"spot": 0, "volatility": 0}

    async def fake_fetch_orderbook(_: str):
        return {
            "bids": [{"price": "0.49", "size": "10"}],
            "asks": [{"price": "0.50", "size": "4"}],
        }

    def fake_spot_price(*args, **kwargs):
        calls["spot"] += 1
        return 101.0

    def fake_capture_anchor(*args, **kwargs):
        return SimpleNamespace(
            anchor_price=100.0, anchor_timestamp=datetime.now(timezone.utc)
        )

    def fake_volatility(*args, **kwargs):
        calls["volatility"] += 1
        return 0.01

    monkeypatch.setattr(pipeline, "_fetch_orderbook", fake_fetch_orderbook)
    monkeypatch.setattr(pipeline.binance_client, "get_spot_price", fake_spot_price)
    monkeypatch.setattr(
        pipeline.binance_client, "calculate_volatility", fake_volatility
    )
    monkeypatch.setattr(pipeline.anchor_store, "capture_anchor", fake_capture_anchor)
    monkeypatch.setattr(
        pipeline.tail_pricer, "minimum_lead_z", lambda timeframe, *a: 0.0
    )
    monkeypatch.setattr(
        pipeline.tail_pricer, "minimum_net_edge", lambda timeframe, *a: -1.0
    )

    first_candidate, _, _ = asyncio.run(
        pipeline._analyze_up_down_market(parsed, market, tradability)
    )
    second_candidate, _, _ = asyncio.run(
        pipeline._analyze_up_down_market(parsed, market, tradability)
    )

    assert first_candidate is not None
    assert second_candidate is not None
    assert calls == {"spot": 1, "volatility": 1}
