"""`realtime_orderbook_cache.py` 測試。"""

from __future__ import annotations

import asyncio

from polymarket_arbitrage.realtime_orderbook_cache import RealtimeOrderBookCache


def test_realtime_orderbook_cache_applies_book_and_price_change_events() -> None:
    """`book` 與 `price_change` 應共同維護最新本地深度。"""
    cache = RealtimeOrderBookCache(freshness_ttl_seconds=30.0)

    async def scenario() -> None:
        await cache._handle_payload(
            {
                "event_type": "book",
                "asset_id": "token-1",
                "market": "market-1",
                "bids": [
                    {"price": "0.48", "size": "30"},
                    {"price": "0.47", "size": "10"},
                ],
                "asks": [
                    {"price": "0.52", "size": "25"},
                    {"price": "0.53", "size": "60"},
                ],
                "timestamp": "1757908892351",
            }
        )
        await cache._handle_payload(
            {
                "event_type": "price_change",
                "market": "market-1",
                "price_changes": [
                    {
                        "asset_id": "token-1",
                        "price": "0.49",
                        "size": "50",
                        "side": "BUY",
                        "best_bid": "0.49",
                        "best_ask": "0.52",
                    },
                    {
                        "asset_id": "token-1",
                        "price": "0.52",
                        "size": "0",
                        "side": "SELL",
                        "best_bid": "0.49",
                        "best_ask": "0.53",
                    },
                ],
                "timestamp": "1757908892400",
            }
        )

    asyncio.run(scenario())

    cached = cache.get_cached_orderbook("token-1", max_age_seconds=30.0)

    assert cached is not None
    assert [level["price"] for level in cached["bids"]] == ["0.49", "0.48", "0.47"]
    assert [level["price"] for level in cached["asks"]] == ["0.53"]


def test_realtime_orderbook_cache_uses_rest_fallback_when_ws_not_ready() -> None:
    """若尚未收到 WebSocket 首筆快照，應允許以 REST fallback 補齊。"""
    cache = RealtimeOrderBookCache(freshness_ttl_seconds=30.0, initial_wait_seconds=0.0)

    async def fake_ensure_assets(asset_ids: list[str]) -> None:
        return None

    async def fake_rest_fallback():
        return {
            "bids": [{"price": "0.41", "size": "100"}],
            "asks": [{"price": "0.43", "size": "200"}],
        }

    cache.ensure_assets = fake_ensure_assets  # type: ignore[method-assign]

    result = asyncio.run(
        cache.get_orderbook(
            "token-rest",
            rest_fallback=fake_rest_fallback,
            max_wait_seconds=0.0,
        )
    )

    assert result is not None
    assert result["bids"][0]["price"] == "0.41"
    assert result["asks"][0]["price"] == "0.43"
