"""`integrated_scanner_v2.py` 的解析測試。"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from polymarket_arbitrage.integrated_scanner_v2 import (
    MarketTradability,
    ParsedMarket,
    PolymarketScannerV2,
    RejectReason,
)


def test_parse_market_rejects_non_crypto_false_positive_from_substring() -> None:
    """`Netherlands` 之類字串不得因包含 `eth` 而被誤判為 ETH。"""
    scanner = PolymarketScannerV2(api_key="test")
    event = {
        "title": "2026 FIFA World Cup Winner",
        "description": "Football futures market",
    }
    market = {
        "question": "Will Netherlands win the 2026 FIFA World Cup?",
        "endDate": "2026-07-20T00:00:00Z",
    }

    parsed, reject_reason, diagnostics = scanner.parse_market(event, market)

    assert parsed is None
    assert reject_reason == RejectReason.NON_CRYPTO_UNRELATED
    assert diagnostics["detected_asset"] is None


def test_parse_market_prefers_full_crypto_keyword_over_embedded_ticker() -> None:
    """完整資產詞應優先於嵌入式 ticker 子字串。"""
    scanner = PolymarketScannerV2(api_key="test")
    event = {
        "title": "MegaETH market cap (FDV) one day after launch?",
        "description": "Crypto market",
    }
    market = {
        "question": "MegaETH market cap (FDV) >$2B one day after launch?",
        "endDate": "2026-07-01T04:00:00Z",
    }

    parsed, reject_reason, diagnostics = scanner.parse_market(event, market)

    assert reject_reason is None
    assert parsed is not None
    assert parsed.asset == "MEGA"
    assert diagnostics["detected_asset"] == "MEGA"


def test_parse_market_rejects_sports_team_named_like_crypto_asset() -> None:
    """像 Avalanche 這類同名體育隊伍不得被誤判成 AVAX。"""
    scanner = PolymarketScannerV2(api_key="test")
    event = {
        "title": "2026 NHL Stanley Cup Champion",
        "description": "Ice hockey futures market",
    }
    market = {
        "question": "Will the Colorado Avalanche win the 2026 NHL Stanley Cup?",
        "endDate": "2026-06-30T00:00:00Z",
    }

    parsed, reject_reason, diagnostics = scanner.parse_market(event, market)

    assert parsed is None
    assert reject_reason == RejectReason.NON_CRYPTO_UNRELATED
    assert diagnostics["detected_asset"] == "AVAX"


def test_build_events_query_params_uses_official_crypto_filter() -> None:
    """官方 events 查詢需先帶 crypto 類別過濾，避免全站熱門事件噪音。"""
    scanner = PolymarketScannerV2(api_key="test")

    params = scanner._build_events_query_params(limit=30)

    assert params["tag_slug"] == "crypto"
    assert params["related_tags"] == "true"
    assert params["active"] == "true"
    assert params["closed"] == "false"
    assert params["archived"] == "false"
    assert params["order"] == "endDate"
    assert params["ascending"] == "true"
    assert params["limit"] == 30
    assert params["offset"] == 0


def test_build_markets_query_params_prioritizes_tradable_markets() -> None:
    """主 discovery 查詢需優先拉可交易且有流動性的 market。"""
    scanner = PolymarketScannerV2(api_key="test")

    params = scanner._build_markets_query_params(limit=30)

    assert params["active"] == "true"
    assert params["closed"] == "false"
    assert params["archived"] == "false"
    assert params["enableOrderBook"] == "true"
    assert params["liquidity_num_min"] == 1
    assert params["order"] == "volume"
    assert params["ascending"] == "false"
    assert params["limit"] == 300
    assert params["offset"] == 0


def test_normalize_market_to_event_reuses_embedded_event_fields() -> None:
    """`/markets` 回傳需可被正規化成既有 parse 流程可讀的 event-like 結構。"""
    scanner = PolymarketScannerV2(api_key="test")
    market = {
        "id": "1757000",
        "slug": "eth-updown-5m-1774777800",
        "question": "Ethereum Up or Down - March 29, 5:50AM-5:55AM ET",
        "description": "Market level description",
        "endDate": "2026-03-29T09:55:00Z",
        "events": [
            {
                "id": "317243",
                "slug": "eth-updown-5m-1774777800",
                "title": "Ethereum Up or Down - March 29, 5:50AM-5:55AM ET",
                "description": "Event level description",
                "startDate": "2026-03-29T09:50:00Z",
                "endDate": "2026-03-29T09:55:00Z",
            }
        ],
    }

    event = scanner._normalize_market_to_event(market)

    assert event["id"] == "317243"
    assert event["slug"] == "eth-updown-5m-1774777800"
    assert event["title"] == "Ethereum Up or Down - March 29, 5:50AM-5:55AM ET"
    assert event["description"] == "Event level description"
    assert event["startDate"] == "2026-03-29T09:50:00Z"
    assert event["endDate"] == "2026-03-29T09:55:00Z"
    assert event["markets"] == [market]


def test_get_all_events_merges_markets_and_crypto_events_for_up_down() -> None:
    """`UP_DOWN` discovery 應合併熱門 market 與近端 crypto event。"""
    scanner = PolymarketScannerV2(api_key="test")

    async def fake_get_all_markets(limit: int = 200):
        return [
            {
                "id": "m-hot",
                "slug": "btc-hot",
                "question": "Bitcoin Up or Down - March 29, 1:55AM-2:00AM ET",
                "endDate": "2026-03-29T06:00:00Z",
                "events": [{"id": "evt-hot", "title": "Bitcoin Up or Down - hot"}],
            }
        ]

    async def fake_get_crypto_events(limit: int = 200):
        return [
            {
                "id": "evt-near",
                "title": "Ethereum Up or Down - March 29, 2:00AM-2:05AM ET",
                "markets": [
                    {
                        "id": "m-near",
                        "question": "Ethereum Up or Down - March 29, 2:00AM-2:05AM ET",
                        "endDate": "2026-03-29T06:05:00Z",
                    }
                ],
            }
        ]

    scanner.get_all_markets = fake_get_all_markets  # type: ignore[method-assign]
    scanner.get_crypto_events = fake_get_crypto_events  # type: ignore[method-assign]

    events = asyncio.run(scanner.get_all_events(limit=30, allowed_styles={"up_down"}))

    assert [event["markets"][0]["id"] for event in events] == ["m-hot", "m-near"]


def test_get_all_events_prefers_markets_payload_when_duplicate_market_exists() -> None:
    """同一 market 同時存在兩個來源時，應優先保留 `/markets` payload。"""
    scanner = PolymarketScannerV2(api_key="test")

    async def fake_get_all_markets(limit: int = 200):
        return [
            {
                "id": "m-dup",
                "slug": "btc-dup-market",
                "question": "Bitcoin Up or Down - Market Payload",
                "endDate": "2026-03-29T06:00:00Z",
                "events": [{"id": "evt-market", "title": "Bitcoin Up or Down - Market Payload"}],
            }
        ]

    async def fake_get_crypto_events(limit: int = 200):
        return [
            {
                "id": "evt-dup",
                "title": "Bitcoin Up or Down - Event Payload",
                "markets": [
                    {
                        "id": "m-dup",
                        "question": "Bitcoin Up or Down - Event Payload",
                        "endDate": "2026-03-29T06:00:00Z",
                    }
                ],
            }
        ]

    scanner.get_all_markets = fake_get_all_markets  # type: ignore[method-assign]
    scanner.get_crypto_events = fake_get_crypto_events  # type: ignore[method-assign]

    events = asyncio.run(scanner.get_all_events(limit=30, allowed_styles={"up_down"}))

    assert len(events) == 1
    assert events[0]["markets"][0]["question"] == "Bitcoin Up or Down - Market Payload"


def test_expand_markets_prefilters_up_down_style() -> None:
    """若只允許 up_down，discovery 階段只應展開 UP_DOWN 題型。"""
    scanner = PolymarketScannerV2(api_key="test")
    events = [
        {
            "title": "Crypto mixed markets",
            "markets": [
                {"question": "Will BTC be up in the next hour?"},
                {"question": "Will BTC be above $120k by Friday?"},
                {"question": "Will Kraken IPO in 2025?"},
            ],
        }
    ]

    markets = scanner.expand_markets(events, allowed_styles={"up_down"})

    assert len(markets) == 1
    assert markets[0][1]["question"] == "Will BTC be up in the next hour?"


def test_expand_markets_without_style_filter_keeps_all_markets() -> None:
    """未指定 style 過濾時，discovery 不應因前置變數未定義而漏市場。"""
    scanner = PolymarketScannerV2(api_key="test")
    events = [
        {
            "title": "Crypto mixed markets",
            "markets": [
                {"question": "Will BTC be up in the next hour?"},
                {"question": "Will BTC be above $120k by Friday?"},
            ],
        }
    ]

    markets = scanner.expand_markets(events, allowed_styles=None)

    assert len(markets) == 2


def test_parse_market_uses_primary_text_for_asset_and_detects_window_timeframe() -> None:
    """`Up or Down` 題型需以標題/題目主體識別資產，並從顯式時間窗推導 timeframe。"""
    scanner = PolymarketScannerV2(api_key="test")
    event = {
        "title": "Ethereum Up or Down - December 19, 11:30AM-11:35AM ET",
        "description": "Resolution source uses Chainlink pricing feed.",
    }
    market = {
        "question": "Ethereum Up or Down - December 19, 11:30AM-11:35AM ET",
        "endDate": "2026-12-19T16:35:00Z",
    }

    parsed, reject_reason, diagnostics = scanner.parse_market(event, market)

    assert reject_reason is None
    assert parsed is not None
    assert parsed.asset == "ETH"
    assert parsed.timeframe == "5m"
    assert diagnostics["detected_asset"] == "ETH"
    assert diagnostics["detected_timeframe"] == "5m"


def test_check_book_endpoint_uses_token_query_param() -> None:
    """book 檢查需走 `GET /book?token_id=...`，避免誤用舊路徑。"""

    class FakeResponse:
        """模擬 aiohttp 回應物件。"""

        def __init__(self, status: int) -> None:
            self.status = status

        async def __aenter__(self) -> "FakeResponse":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class FakeSession:
        """記錄最後一次請求的假 session。"""

        def __init__(self) -> None:
            self.last_url = None
            self.last_params = None
            self.last_timeout = None

        def get(self, url: str, params=None, timeout=None) -> FakeResponse:
            self.last_url = url
            self.last_params = params
            self.last_timeout = timeout
            return FakeResponse(status=200)

    scanner = PolymarketScannerV2(api_key="test")
    scanner.session = FakeSession()

    result = asyncio.run(scanner._check_book_endpoint("123"))

    assert result is True
    assert scanner.session.last_url.endswith("/book")
    assert scanner.session.last_params == {"token_id": "123"}
    assert scanner.session.last_timeout == 5


def test_check_tradability_requires_both_token_ids() -> None:
    """tradability 不得因單邊 token 存在就視為可交易。"""
    scanner = PolymarketScannerV2(api_key="test")
    market = {
        "conditionId": "m-single",
        "slug": "single-sided-market",
        "active": True,
        "closed": False,
        "archived": False,
        "enableOrderBook": True,
        "clobTokenIds": '["yes-only"]',
        "volume": 12,
    }

    tradability = asyncio.run(scanner.check_tradability(market))

    assert tradability.has_token_ids is False
    assert tradability.orderbook_reject == RejectReason.MISSING_CLOB_TOKEN_IDS
    assert tradability.is_clob_eligible is False


def test_check_tradability_requires_dual_orderbooks() -> None:
    """tradability 需同時驗證 YES / NO 雙邊深度。"""
    scanner = PolymarketScannerV2(api_key="test")
    market = {
        "conditionId": "m-depth",
        "slug": "dual-depth-market",
        "active": True,
        "closed": False,
        "archived": False,
        "enableOrderBook": True,
        "clobTokenIds": '["yes-token","no-token"]',
        "volume": 25,
    }

    async def fake_check_endpoint(_: str) -> bool:
        return True

    async def fake_fetch_orderbook(token_id: str):
        if token_id == "yes-token":
            return {"bids": [{"price": "0.45", "size": "10"}], "asks": []}
        return None

    scanner._check_price_endpoint = fake_check_endpoint  # type: ignore[method-assign]
    scanner._check_midpoint_endpoint = fake_check_endpoint  # type: ignore[method-assign]
    scanner._fetch_orderbook = fake_fetch_orderbook  # type: ignore[method-assign]

    tradability = asyncio.run(scanner.check_tradability(market))

    assert tradability.has_token_ids is True
    assert tradability.price_available is True
    assert tradability.midpoint_available is True
    assert tradability.book_available is False
    assert tradability.clob_reject == RejectReason.BOOK_NOT_FOUND
    assert tradability.is_book_verified is False


def test_check_tradability_can_skip_depth_until_second_stage() -> None:
    """第一段 tradability 不應過早抓取完整雙邊 order book。"""
    scanner = PolymarketScannerV2(api_key="test")
    market = {
        "conditionId": "m-light",
        "slug": "light-tradability-market",
        "active": True,
        "closed": False,
        "archived": False,
        "enableOrderBook": True,
        "clobTokenIds": '["yes-token","no-token"]',
        "volume": 25,
    }
    calls = {"orderbook": 0}

    async def fake_check_endpoint(_: str) -> bool:
        return True

    async def fake_fetch_orderbook(_: str):
        calls["orderbook"] += 1
        return {"bids": [], "asks": []}

    scanner._check_price_endpoint = fake_check_endpoint  # type: ignore[method-assign]
    scanner._check_midpoint_endpoint = fake_check_endpoint  # type: ignore[method-assign]
    scanner._fetch_orderbook = fake_fetch_orderbook  # type: ignore[method-assign]

    tradability = asyncio.run(scanner.check_tradability(market, verify_depth=False))

    assert tradability.has_token_ids is True
    assert tradability.price_available is True
    assert tradability.midpoint_available is True
    assert tradability.book_available is False
    assert tradability.is_book_verified is False
    assert calls["orderbook"] == 0


def test_prioritize_markets_for_analysis_prefers_near_expiry_for_up_down() -> None:
    """UP_DOWN 本地重排應先看近到期，再看成交量。"""
    scanner = PolymarketScannerV2(api_key="test")
    fixed_now = datetime(2026, 3, 29, 12, 0, tzinfo=timezone.utc)
    near_low_volume = (
        ParsedMarket(
            raw_event={},
            raw_market={},
            asset="BTC",
            style="UP_DOWN",
            timeframe="5m",
            strike=None,
            expiry=datetime(2026, 3, 29, 12, 5, tzinfo=timezone.utc),
            is_crypto=True,
            is_short_term=True,
        ),
        {"id": "near-low"},
        MarketTradability(
            market_id="near-low",
            slug="near-low",
            is_active=True,
            is_closed=False,
            is_archived=False,
            status_reject=None,
            enable_orderbook=True,
            has_token_ids=True,
            yes_token="yes-a",
            no_token="no-a",
            orderbook_reject=None,
            price_available=True,
            midpoint_available=True,
            book_available=True,
            clob_reject=None,
            is_clob_eligible=True,
            is_book_verified=True,
            volume=10.0,
        ),
    )
    far_high_volume = (
        ParsedMarket(
            raw_event={},
            raw_market={},
            asset="BTC",
            style="UP_DOWN",
            timeframe="5m",
            strike=None,
            expiry=datetime(2026, 3, 29, 13, 5, tzinfo=timezone.utc),
            is_crypto=True,
            is_short_term=True,
        ),
        {"id": "far-high"},
        MarketTradability(
            market_id="far-high",
            slug="far-high",
            is_active=True,
            is_closed=False,
            is_archived=False,
            status_reject=None,
            enable_orderbook=True,
            has_token_ids=True,
            yes_token="yes-b",
            no_token="no-b",
            orderbook_reject=None,
            price_available=True,
            midpoint_available=True,
            book_available=True,
            clob_reject=None,
            is_clob_eligible=True,
            is_book_verified=True,
            volume=999.0,
        ),
    )

    ordered = scanner.prioritize_markets_for_analysis(
        [far_high_volume, near_low_volume],
        allowed_styles={"up_down"},
        now=fixed_now,
    )

    assert [item[1]["id"] for item in ordered] == ["near-low", "far-high"]


def test_prioritize_markets_for_analysis_uses_volume_as_tiebreaker() -> None:
    """近到期相同時，成交量高者應排前。"""
    scanner = PolymarketScannerV2(api_key="test")
    fixed_now = datetime(2026, 3, 29, 12, 0, tzinfo=timezone.utc)
    same_expiry = datetime(2026, 3, 29, 12, 5, tzinfo=timezone.utc)
    low_volume = (
        ParsedMarket(
            raw_event={},
            raw_market={},
            asset="ETH",
            style="UP_DOWN",
            timeframe="5m",
            strike=None,
            expiry=same_expiry,
            is_crypto=True,
            is_short_term=True,
        ),
        {"id": "low-volume"},
        MarketTradability(
            market_id="low-volume",
            slug="low-volume",
            is_active=True,
            is_closed=False,
            is_archived=False,
            status_reject=None,
            enable_orderbook=True,
            has_token_ids=True,
            yes_token="yes-c",
            no_token="no-c",
            orderbook_reject=None,
            price_available=True,
            midpoint_available=True,
            book_available=True,
            clob_reject=None,
            is_clob_eligible=True,
            is_book_verified=True,
            volume=20.0,
        ),
    )
    high_volume = (
        ParsedMarket(
            raw_event={},
            raw_market={},
            asset="ETH",
            style="UP_DOWN",
            timeframe="5m",
            strike=None,
            expiry=same_expiry,
            is_crypto=True,
            is_short_term=True,
        ),
        {"id": "high-volume"},
        MarketTradability(
            market_id="high-volume",
            slug="high-volume",
            is_active=True,
            is_closed=False,
            is_archived=False,
            status_reject=None,
            enable_orderbook=True,
            has_token_ids=True,
            yes_token="yes-d",
            no_token="no-d",
            orderbook_reject=None,
            price_available=True,
            midpoint_available=True,
            book_available=True,
            clob_reject=None,
            is_clob_eligible=True,
            is_book_verified=True,
            volume=200.0,
        ),
    )

    ordered = scanner.prioritize_markets_for_analysis(
        [low_volume, high_volume],
        allowed_styles={"up_down"},
        now=fixed_now,
    )

    assert [item[1]["id"] for item in ordered] == ["high-volume", "low-volume"]


def test_prioritize_markets_for_analysis_prefers_closer_armed_window_over_earlier_expiry() -> None:
    """不同 timeframe 時，應優先排序更接近 `armed` 窗口的市場，而非只看較早到期。"""
    scanner = PolymarketScannerV2(api_key="test")
    fixed_now = datetime(2026, 3, 29, 4, 47, tzinfo=timezone.utc)
    earlier_expiry_but_farther_from_window = (
        ParsedMarket(
            raw_event={},
            raw_market={},
            asset="BTC",
            style="UP_DOWN",
            timeframe="5m",
            strike=None,
            expiry=datetime(2026, 3, 29, 4, 57, tzinfo=timezone.utc),
            is_crypto=True,
            is_short_term=True,
        ),
        {"id": "earlier-expiry"},
        MarketTradability(
            market_id="earlier-expiry",
            slug="earlier-expiry",
            is_active=True,
            is_closed=False,
            is_archived=False,
            status_reject=None,
            enable_orderbook=True,
            has_token_ids=True,
            yes_token="yes-a",
            no_token="no-a",
            orderbook_reject=None,
            price_available=True,
            midpoint_available=True,
            book_available=True,
            clob_reject=None,
            is_clob_eligible=True,
            is_book_verified=True,
            volume=100.0,
        ),
    )
    later_expiry_but_nearer_to_window = (
        ParsedMarket(
            raw_event={},
            raw_market={},
            asset="BTC",
            style="UP_DOWN",
            timeframe="1h",
            strike=None,
            expiry=datetime(2026, 3, 29, 5, 5, tzinfo=timezone.utc),
            is_crypto=True,
            is_short_term=True,
        ),
        {"id": "nearer-window"},
        MarketTradability(
            market_id="nearer-window",
            slug="nearer-window",
            is_active=True,
            is_closed=False,
            is_archived=False,
            status_reject=None,
            enable_orderbook=True,
            has_token_ids=True,
            yes_token="yes-b",
            no_token="no-b",
            orderbook_reject=None,
            price_available=True,
            midpoint_available=True,
            book_available=True,
            clob_reject=None,
            is_clob_eligible=True,
            is_book_verified=True,
            volume=50.0,
        ),
    )

    ordered = scanner.prioritize_markets_for_analysis(
        [earlier_expiry_but_farther_from_window, later_expiry_but_nearer_to_window],
        allowed_styles={"up_down"},
        now=fixed_now,
    )

    assert [item[1]["id"] for item in ordered] == ["nearer-window", "earlier-expiry"]


def test_filter_live_markets_for_analysis_keeps_open_markets_and_filters_only_expired() -> None:
    """`UP_DOWN` 主研究線前置只應擋掉 expired，observe / armed / attack 都需進研究。"""
    scanner = PolymarketScannerV2(api_key="test")
    fixed_now = datetime(2026, 3, 29, 12, 0, tzinfo=timezone.utc)

    observe_market = (
        ParsedMarket(
            raw_event={},
            raw_market={},
            asset="BTC",
            style="UP_DOWN",
            timeframe="5m",
            strike=None,
            expiry=datetime(2026, 3, 29, 12, 5, tzinfo=timezone.utc),
            is_crypto=True,
            is_short_term=True,
        ),
        {"id": "observe", "question": "Bitcoin Up or Down - Observe"},
        MarketTradability(
            market_id="observe",
            slug="observe",
            is_active=True,
            is_closed=False,
            is_archived=False,
            status_reject=None,
            enable_orderbook=True,
            has_token_ids=True,
            yes_token="yes-observe",
            no_token="no-observe",
            orderbook_reject=None,
            price_available=True,
            midpoint_available=True,
            book_available=True,
            clob_reject=None,
            is_clob_eligible=True,
            is_book_verified=True,
            volume=100.0,
        ),
    )
    armed_market = (
        ParsedMarket(
            raw_event={},
            raw_market={},
            asset="BTC",
            style="UP_DOWN",
            timeframe="5m",
            strike=None,
            expiry=datetime(2026, 3, 29, 12, 1, 20, tzinfo=timezone.utc),
            is_crypto=True,
            is_short_term=True,
        ),
        {"id": "armed", "question": "Bitcoin Up or Down - Armed"},
        MarketTradability(
            market_id="armed",
            slug="armed",
            is_active=True,
            is_closed=False,
            is_archived=False,
            status_reject=None,
            enable_orderbook=True,
            has_token_ids=True,
            yes_token="yes-armed",
            no_token="no-armed",
            orderbook_reject=None,
            price_available=True,
            midpoint_available=True,
            book_available=True,
            clob_reject=None,
            is_clob_eligible=True,
            is_book_verified=True,
            volume=100.0,
        ),
    )
    attack_market = (
        ParsedMarket(
            raw_event={},
            raw_market={},
            asset="BTC",
            style="UP_DOWN",
            timeframe="5m",
            strike=None,
            expiry=datetime(2026, 3, 29, 12, 0, 20, tzinfo=timezone.utc),
            is_crypto=True,
            is_short_term=True,
        ),
        {"id": "attack", "question": "Bitcoin Up or Down - Attack"},
        MarketTradability(
            market_id="attack",
            slug="attack",
            is_active=True,
            is_closed=False,
            is_archived=False,
            status_reject=None,
            enable_orderbook=True,
            has_token_ids=True,
            yes_token="yes-attack",
            no_token="no-attack",
            orderbook_reject=None,
            price_available=True,
            midpoint_available=True,
            book_available=True,
            clob_reject=None,
            is_clob_eligible=True,
            is_book_verified=True,
            volume=100.0,
        ),
    )
    expired_market = (
        ParsedMarket(
            raw_event={},
            raw_market={},
            asset="BTC",
            style="UP_DOWN",
            timeframe="5m",
            strike=None,
            expiry=datetime(2026, 3, 29, 11, 59, 50, tzinfo=timezone.utc),
            is_crypto=True,
            is_short_term=True,
        ),
        {"id": "expired", "question": "Bitcoin Up or Down - Expired"},
        MarketTradability(
            market_id="expired",
            slug="expired",
            is_active=True,
            is_closed=False,
            is_archived=False,
            status_reject=None,
            enable_orderbook=True,
            has_token_ids=True,
            yes_token="yes-expired",
            no_token="no-expired",
            orderbook_reject=None,
            price_available=True,
            midpoint_available=True,
            book_available=True,
            clob_reject=None,
            is_clob_eligible=True,
            is_book_verified=True,
            volume=100.0,
        ),
    )

    live_markets, filtered_out = scanner.filter_live_markets_for_analysis(
        [observe_market, armed_market, attack_market, expired_market],
        allowed_styles={"up_down"},
        now=fixed_now,
    )

    assert [item[1]["id"] for item in live_markets] == ["observe", "armed", "attack"]
    assert len(filtered_out) == 1
    assert filtered_out[0]["reason"] == "market_expired"
    assert filtered_out[0]["market_id"] == "expired"
    assert filtered_out[0]["window_state"] == "expired"


def test_filter_live_markets_for_analysis_splits_not_open_markets_from_anchor_failures() -> None:
    """尚未開盤的 `UP_DOWN` 市場應以前置拒絕 `market_not_open_yet` 分流。"""
    scanner = PolymarketScannerV2(api_key="test")
    fixed_now = datetime(2026, 3, 29, 11, 59, 50, tzinfo=timezone.utc)

    open_market = (
        ParsedMarket(
            raw_event={},
            raw_market={},
            asset="BTC",
            style="UP_DOWN",
            timeframe="5m",
            strike=None,
            expiry=datetime(2026, 3, 29, 12, 4, 50, tzinfo=timezone.utc),
            is_crypto=True,
            is_short_term=True,
        ),
        {"id": "open", "question": "Bitcoin Up or Down - Open"},
        MarketTradability(
            market_id="open",
            slug="open",
            is_active=True,
            is_closed=False,
            is_archived=False,
            status_reject=None,
            enable_orderbook=True,
            has_token_ids=True,
            yes_token="yes-open",
            no_token="no-open",
            orderbook_reject=None,
            price_available=True,
            midpoint_available=True,
            book_available=True,
            clob_reject=None,
            is_clob_eligible=True,
            is_book_verified=True,
            volume=100.0,
        ),
    )
    future_market = (
        ParsedMarket(
            raw_event={},
            raw_market={},
            asset="BTC",
            style="UP_DOWN",
            timeframe="5m",
            strike=None,
            expiry=datetime(2026, 3, 29, 12, 5, tzinfo=timezone.utc),
            is_crypto=True,
            is_short_term=True,
        ),
        {"id": "future", "question": "Bitcoin Up or Down - Future"},
        MarketTradability(
            market_id="future",
            slug="future",
            is_active=True,
            is_closed=False,
            is_archived=False,
            status_reject=None,
            enable_orderbook=True,
            has_token_ids=True,
            yes_token="yes-future",
            no_token="no-future",
            orderbook_reject=None,
            price_available=True,
            midpoint_available=True,
            book_available=True,
            clob_reject=None,
            is_clob_eligible=True,
            is_book_verified=True,
            volume=100.0,
        ),
    )

    live_markets, filtered_out = scanner.filter_live_markets_for_analysis(
        [open_market, future_market],
        allowed_styles={"up_down"},
        now=fixed_now,
    )

    assert [item[1]["id"] for item in live_markets] == ["open"]
    assert len(filtered_out) == 1
    assert filtered_out[0]["reason"] == "market_not_open_yet"
    assert filtered_out[0]["market_id"] == "future"
    assert filtered_out[0]["window_state"] == "not_open"
    assert filtered_out[0]["seconds_to_open"] == 10.0
