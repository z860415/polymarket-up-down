"""
Polymarket 即時訂單簿快取。
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Awaitable, Callable, Dict, Optional

import aiohttp

logger = logging.getLogger(__name__)


class RealtimeOrderBookCache:
    """透過官方 market WebSocket 維護 token 級別的即時訂單簿快取。"""

    WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def __init__(
        self,
        ws_url: str | None = None,
        freshness_ttl_seconds: float = 3.0,
        heartbeat_interval_seconds: float = 10.0,
        initial_wait_seconds: float = 1.2,
    ) -> None:
        self.ws_url = ws_url or self.WS_URL
        self.freshness_ttl_seconds = max(freshness_ttl_seconds, 0.0)
        self.heartbeat_interval_seconds = max(heartbeat_interval_seconds, 1.0)
        self.initial_wait_seconds = max(initial_wait_seconds, 0.0)

        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._connect_lock = asyncio.Lock()
        self._subscription_lock = asyncio.Lock()
        self._closed = False

        # 讀寫快取時可能跨 event loop thread / worker thread，因此需加同步鎖。
        self._book_lock = threading.RLock()
        self._books: Dict[str, Dict[str, Any]] = {}
        self._book_ready_events: Dict[str, asyncio.Event] = {}
        self._subscribed_assets: set[str] = set()

    async def ensure_assets(self, asset_ids: list[str]) -> None:
        """確保指定 token 已完成 WebSocket 訂閱。"""
        normalized_assets = sorted({asset_id for asset_id in asset_ids if asset_id})
        if not normalized_assets or self._closed:
            return

        async with self._subscription_lock:
            await self._ensure_connection()
            new_assets = [
                asset_id
                for asset_id in normalized_assets
                if asset_id not in self._subscribed_assets
            ]
            if not new_assets:
                return

            with self._book_lock:
                for asset_id in new_assets:
                    self._book_ready_events.setdefault(asset_id, asyncio.Event())

            if self._subscribed_assets:
                payload = {
                    "operation": "subscribe",
                    "assets_ids": new_assets,
                    "custom_feature_enabled": True,
                }
            else:
                payload = {
                    "assets_ids": new_assets,
                    "type": "market",
                    "custom_feature_enabled": True,
                }

            await self._send_json(payload)
            self._subscribed_assets.update(new_assets)

    def get_cached_orderbook(
        self,
        token_id: str,
        max_age_seconds: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        """同步讀取新鮮快取，供執行層在 worker thread 中直接使用。"""
        if not token_id:
            return None
        with self._book_lock:
            snapshot = self._books.get(token_id)
            if snapshot is None or not self._is_snapshot_fresh(
                snapshot,
                max_age_seconds=max_age_seconds,
            ):
                return None
            return deepcopy(snapshot)

    async def get_orderbook(
        self,
        token_id: str,
        rest_fallback: Optional[Callable[[], Awaitable[Optional[Dict[str, Any]]]]] = None,
        max_wait_seconds: Optional[float] = None,
        max_age_seconds: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        """優先等候 WebSocket 快取，必要時才 fallback REST。"""
        if not token_id:
            return None

        await self.ensure_assets([token_id])

        cached = self.get_cached_orderbook(
            token_id,
            max_age_seconds=max_age_seconds,
        )
        if cached is not None:
            return cached

        wait_seconds = (
            self.initial_wait_seconds if max_wait_seconds is None else max_wait_seconds
        )
        ready_event = self._get_or_create_ready_event(token_id)
        if wait_seconds > 0:
            try:
                await asyncio.wait_for(ready_event.wait(), timeout=wait_seconds)
            except asyncio.TimeoutError:
                logger.debug("等待 WebSocket 首筆 order book 超時: %s", token_id)

        cached = self.get_cached_orderbook(
            token_id,
            max_age_seconds=max_age_seconds,
        )
        if cached is not None:
            return cached

        if rest_fallback is None:
            return None

        payload = await rest_fallback()
        normalized = self._normalize_external_orderbook(payload)
        if normalized is None:
            return None

        self._store_snapshot(token_id, normalized)
        return self.get_cached_orderbook(token_id, max_age_seconds=None)

    async def close(self) -> None:
        """關閉 WebSocket 連線與背景任務。"""
        self._closed = True
        if self._reader_task is not None:
            self._reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reader_task
            self._reader_task = None

        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
            self._heartbeat_task = None

        if self._ws is not None:
            await self._ws.close()
            self._ws = None

        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _ensure_connection(self) -> None:
        """建立 WebSocket 連線並啟動背景任務。"""
        if self._closed:
            return

        async with self._connect_lock:
            if self._ws is not None and not self._ws.closed:
                return

            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession()

            self._ws = await self._session.ws_connect(self.ws_url, heartbeat=0)
            self._reader_task = asyncio.create_task(self._reader_loop())
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            logger.info("已建立 Polymarket market WebSocket 連線")

    async def _send_json(self, payload: Dict[str, Any]) -> None:
        """送出 JSON 訂閱訊息。"""
        if self._ws is None:
            raise RuntimeError("WebSocket 尚未建立")
        await self._ws.send_json(payload)

    async def _heartbeat_loop(self) -> None:
        """定期送出官方要求的 PING heartbeat。"""
        try:
            while not self._closed:
                await asyncio.sleep(self.heartbeat_interval_seconds)
                if self._ws is None or self._ws.closed:
                    return
                await self._ws.send_str("PING")
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.warning("WebSocket heartbeat 失敗: %s", error)

    async def _reader_loop(self) -> None:
        """持續接收 market channel 訊息並更新本地快取。"""
        try:
            while not self._closed and self._ws is not None:
                message = await self._ws.receive()
                if message.type == aiohttp.WSMsgType.TEXT:
                    if message.data == "PONG":
                        continue
                    await self._handle_payload(message.json())
                    continue
                if message.type == aiohttp.WSMsgType.BINARY:
                    continue
                if message.type in {
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.CLOSING,
                }:
                    logger.warning("market WebSocket 已關閉")
                    return
                if message.type == aiohttp.WSMsgType.ERROR:
                    logger.warning("market WebSocket 發生錯誤: %s", self._ws.exception())
                    return
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.warning("market WebSocket reader 異常: %s", error)
        finally:
            self._ws = None

    async def _handle_payload(self, payload: Any) -> None:
        """處理 WebSocket 訊息內容。"""
        if not isinstance(payload, dict):
            return

        event_type = payload.get("event_type")
        if event_type == "book":
            normalized = self._normalize_book_event(payload)
            asset_id = payload.get("asset_id")
            if normalized is not None and asset_id:
                self._store_snapshot(asset_id, normalized)
            return

        if event_type == "price_change":
            self._apply_price_change_event(payload)
            return

        if event_type == "best_bid_ask":
            self._apply_best_bid_ask_event(payload)

    def _apply_price_change_event(self, payload: Dict[str, Any]) -> None:
        """把 `price_change` 增量套用到本地 order book。"""
        price_changes = payload.get("price_changes") or []
        timestamp = payload.get("timestamp")
        fetched_at = self._utc_now_iso()

        with self._book_lock:
            for price_change in price_changes:
                if not isinstance(price_change, dict):
                    continue
                asset_id = price_change.get("asset_id")
                if not asset_id:
                    continue

                snapshot = deepcopy(
                    self._books.get(asset_id)
                    or {
                        "bids": [],
                        "asks": [],
                    }
                )
                self._apply_single_price_change(snapshot, price_change)
                snapshot["_fetched_at"] = fetched_at
                if timestamp is not None:
                    snapshot["timestamp"] = timestamp
                self._books[asset_id] = snapshot
                self._get_or_create_ready_event(asset_id).set()

    def _apply_best_bid_ask_event(self, payload: Dict[str, Any]) -> None:
        """以 best bid/ask 事件修補頂檔價格。"""
        asset_id = payload.get("asset_id")
        if not asset_id:
            return

        with self._book_lock:
            snapshot = deepcopy(self._books.get(asset_id) or {"bids": [], "asks": []})
            best_bid = payload.get("best_bid")
            best_ask = payload.get("best_ask")

            if best_bid not in (None, ""):
                self._replace_top_level(snapshot, "bids", best_bid)
            if best_ask not in (None, ""):
                self._replace_top_level(snapshot, "asks", best_ask)

            snapshot["_fetched_at"] = self._utc_now_iso()
            if payload.get("timestamp") is not None:
                snapshot["timestamp"] = payload["timestamp"]
            self._books[asset_id] = snapshot
            self._get_or_create_ready_event(asset_id).set()

    def _replace_top_level(
        self,
        snapshot: Dict[str, Any],
        side_key: str,
        price: Any,
    ) -> None:
        """用 best bid/ask 修補頂檔價位，保留原有深度資料。"""
        normalized_price = self._normalize_price_key(price)
        if normalized_price is None:
            return
        levels = list(snapshot.get(side_key) or [])
        if levels:
            levels[0] = {
                "price": normalized_price,
                "size": levels[0].get("size", "0"),
            }
        else:
            levels = [{"price": normalized_price, "size": "0"}]
        snapshot[side_key] = levels

    def _apply_single_price_change(
        self,
        snapshot: Dict[str, Any],
        price_change: Dict[str, Any],
    ) -> None:
        """套用單一價位增量更新。"""
        price_key = self._normalize_price_key(price_change.get("price"))
        if price_key is None:
            return

        side = (price_change.get("side") or "").upper()
        side_key = "bids" if side == "BUY" else "asks"
        levels = self._levels_to_map(snapshot.get(side_key) or [])

        size_value = self._parse_decimal(price_change.get("size"))
        if size_value is None or size_value <= 0:
            levels.pop(price_key, None)
        else:
            levels[price_key] = {
                "price": price_key,
                "size": self._normalize_size_value(size_value),
            }

        snapshot[side_key] = self._levels_from_map(
            levels,
            is_bid=(side_key == "bids"),
        )

    def _store_snapshot(self, asset_id: str, snapshot: Dict[str, Any]) -> None:
        """寫入快取並喚醒等待中的讀取方。"""
        with self._book_lock:
            normalized_snapshot = deepcopy(snapshot)
            normalized_snapshot["_fetched_at"] = self._utc_now_iso()
            self._books[asset_id] = normalized_snapshot
            self._get_or_create_ready_event(asset_id).set()

    def _normalize_book_event(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """把 `book` 訊息轉為專案內部的標準化 order book 結構。"""
        if payload.get("asset_id") in (None, ""):
            return None
        return {
            "bids": self._normalize_levels(payload.get("bids") or [], is_bid=True),
            "asks": self._normalize_levels(payload.get("asks") or [], is_bid=False),
            "market": payload.get("market"),
            "hash": payload.get("hash"),
            "timestamp": payload.get("timestamp"),
        }

    def _normalize_external_orderbook(
        self,
        payload: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """把 REST fallback 的 payload 轉成與 WebSocket 相同的結構。"""
        if not isinstance(payload, dict):
            return None
        return {
            "bids": self._normalize_levels(payload.get("bids") or [], is_bid=True),
            "asks": self._normalize_levels(payload.get("asks") or [], is_bid=False),
            "timestamp": payload.get("timestamp"),
        }

    def _normalize_levels(
        self,
        levels: list[Any],
        is_bid: bool,
    ) -> list[Dict[str, str]]:
        """把各種來源的 levels 統一成 `price/size` dict 陣列。"""
        normalized_levels = {}
        for level in levels:
            raw_price = getattr(level, "price", None)
            raw_size = getattr(level, "size", None)
            if isinstance(level, dict):
                raw_price = level.get("price", raw_price)
                raw_size = level.get("size", raw_size)
            price_key = self._normalize_price_key(raw_price)
            size_value = self._parse_decimal(raw_size)
            if price_key is None or size_value is None or size_value <= 0:
                continue
            normalized_levels[price_key] = {
                "price": price_key,
                "size": self._normalize_size_value(size_value),
            }
        return self._levels_from_map(normalized_levels, is_bid=is_bid)

    def _levels_to_map(self, levels: list[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
        """把 list 形式的 levels 轉為以 price 為 key 的 map。"""
        mapped_levels: Dict[str, Dict[str, str]] = {}
        for level in levels:
            if not isinstance(level, dict):
                continue
            price_key = self._normalize_price_key(level.get("price"))
            size_value = self._parse_decimal(level.get("size"))
            if price_key is None or size_value is None or size_value <= 0:
                continue
            mapped_levels[price_key] = {
                "price": price_key,
                "size": self._normalize_size_value(size_value),
            }
        return mapped_levels

    def _levels_from_map(
        self,
        levels: Dict[str, Dict[str, str]],
        is_bid: bool,
    ) -> list[Dict[str, str]]:
        """把 price map 重建為排序後的 level 陣列。"""
        sorted_prices = sorted(
            levels.keys(),
            key=lambda value: self._parse_decimal(value) or Decimal("0"),
            reverse=is_bid,
        )
        return [levels[price] for price in sorted_prices]

    def _get_or_create_ready_event(self, token_id: str) -> asyncio.Event:
        """取得指定 token 的首筆深度就緒事件。"""
        with self._book_lock:
            ready_event = self._book_ready_events.get(token_id)
            if ready_event is None:
                ready_event = asyncio.Event()
                self._book_ready_events[token_id] = ready_event
            return ready_event

    def _is_snapshot_fresh(
        self,
        snapshot: Dict[str, Any],
        max_age_seconds: Optional[float],
    ) -> bool:
        """判斷快取是否仍在可接受的新鮮度範圍內。"""
        threshold = (
            self.freshness_ttl_seconds
            if max_age_seconds is None
            else max(max_age_seconds, 0.0)
        )
        if threshold <= 0:
            return True

        fetched_at = snapshot.get("_fetched_at")
        if not isinstance(fetched_at, str):
            return False
        try:
            fetched_at_dt = datetime.fromisoformat(fetched_at)
        except ValueError:
            return False
        if fetched_at_dt.tzinfo is None:
            fetched_at_dt = fetched_at_dt.replace(tzinfo=timezone.utc)
        return (
            datetime.now(timezone.utc) - fetched_at_dt
        ).total_seconds() <= threshold

    def _normalize_price_key(self, raw_price: Any) -> Optional[str]:
        """把價格統一成可比較的 canonical string。"""
        price_decimal = self._parse_decimal(raw_price)
        if price_decimal is None:
            return None
        return format(price_decimal.normalize(), "f")

    def _normalize_size_value(self, raw_size: Decimal) -> str:
        """把 size 統一轉為標準字串格式。"""
        return format(raw_size.normalize(), "f")

    def _parse_decimal(self, raw_value: Any) -> Optional[Decimal]:
        """安全解析 Decimal，避免浮點誤差污染價位鍵。"""
        if raw_value in (None, ""):
            return None
        try:
            return Decimal(str(raw_value))
        except (InvalidOperation, ValueError, TypeError):
            return None

    def _utc_now_iso(self) -> str:
        """輸出統一的 UTC ISO timestamp。"""
        return datetime.now(timezone.utc).isoformat()
