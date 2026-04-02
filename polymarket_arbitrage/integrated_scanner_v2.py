#!/usr/bin/env python3
"""
Polymarket Integrated Scanner v2.0
三層漏斗架構：Discovery → Parse → Tradability

Author: Kimi Claw
Date: 2026-03-28
"""

import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum

import aiohttp
from py_clob_client.client import ClobClient

from .market_definition import (
    MarketDefinition,
    PayoffType,
    ResolutionOperator,
    StrikeType,
    SettlementRule,
    OracleFamily,
    TIMEFRAME_SECONDS,
)
from .signal_logger import SignalLogger, SignalObservation
from .updown_tail_pricer import TAIL_WINDOWS

try:
    from .live_executor import LiveExecutor, LiveRiskConfig
except ModuleNotFoundError:
    LiveExecutor = Any

    @dataclass
    class LiveRiskConfig:
        """研究模式下的輕量風控占位設定。"""

        min_edge_threshold: float = 0.03
        min_confidence_score: float = 0.3


# TradingOpportunity 定義 (從舊版複製)
@dataclass
class TradingOpportunity:
    """交易機會"""

    market_def: MarketDefinition
    signal: SignalObservation
    fair_prob: float
    market_price: float
    edge: float
    confidence: float
    yes_token_id: str
    no_token_id: str
    orderbook: Dict
    timestamp: datetime


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class RejectReason(Enum):
    """市場過濾拒絕原因 - 細分版本"""

    # Discovery Phase
    NO_MARKETS_IN_EVENT = "no_markets_in_event"

    # Parse Phase - Asset
    NON_CRYPTO_UNRELATED = "non_crypto_unrelated"  # 完全無關的非加密市場
    CRYPTO_UNSUPPORTED = "crypto_unsupported"  # 加密相關但暫不支援的資產
    ASSET_UNKNOWN = "asset_unknown"  # 無法識別的資產

    # Parse Phase - Style/Structure
    STYLE_UNKNOWN = "style_unknown"  # 無法識別 Above/Below/Up/Down
    TIMEFRAME_UNKNOWN = "timeframe_unknown"  # 無短期時間框架（可選標記）
    UNSUPPORTED_PAYOFF_TYPE = "unsupported_payoff_type"

    # Parse Phase - Data
    MISSING_END_DATE = "missing_end_date"
    INVALID_DATE_FORMAT = "invalid_date_format"

    # Tradability Phase - Status
    MARKET_CLOSED = "market_closed"
    MARKET_ARCHIVED = "market_archived"
    MARKET_NOT_ACTIVE = "market_not_active"

    # Tradability Phase - OrderBook
    ORDERBOOK_DISABLED = "orderbook_disabled"
    MISSING_CLOB_TOKEN_IDS = "missing_clob_token_ids"
    INVALID_TOKEN_IDS = "invalid_token_ids"

    # Tradability Phase - CLOB API
    PRICE_ENDPOINT_UNAVAILABLE = "price_endpoint_unavailable"
    BOOK_NOT_FOUND = "book_not_found"


@dataclass
class ParsedMarket:
    """解析後的市場結構"""

    # 原始資料
    raw_event: Dict
    raw_market: Dict

    # 解析結果
    asset: str
    style: str  # "ABOVE_BELOW" | "UP_DOWN"
    timeframe: Optional[str]  # "5m" | "15m" | "1h" | "4h" | "1d" | None
    strike: Optional[float]
    expiry: datetime

    # 分類標籤
    is_crypto: bool
    is_short_term: bool


@dataclass
class MarketTradability:
    """市場可交易性檢查結果"""

    market_id: str
    slug: str

    # Status 檢查
    is_active: bool
    is_closed: bool
    is_archived: bool
    status_reject: Optional[RejectReason]

    # OrderBook 檢查
    enable_orderbook: bool
    has_token_ids: bool
    yes_token: Optional[str]
    no_token: Optional[str]
    orderbook_reject: Optional[RejectReason]

    # CLOB API 驗證
    price_available: bool
    midpoint_available: bool
    book_available: bool
    clob_reject: Optional[RejectReason]

    # 綜合評估
    is_clob_eligible: bool  # Status OK + enableOrderBook + has tokens
    is_book_verified: bool  # CLOB-eligible + YES/NO 雙邊深度驗證通過

    # 活躍度代理
    volume: float
    yes_orderbook: Optional[Dict[str, Any]] = None
    no_orderbook: Optional[Dict[str, Any]] = None


@dataclass
class ScannerFunnelStats:
    """掃描漏斗統計"""

    # Discovery
    total_events: int = 0
    total_markets: int = 0

    # Parse
    parsed_count: int = 0
    crypto_count: int = 0
    short_term_count: int = 0
    above_below_count: int = 0
    up_down_count: int = 0

    # Tradability - CLOB Eligible (status OK + enableOrderBook + has tokens)
    clob_eligible_count: int = 0

    # Tradability - Pricing Verified (CLOB-eligible + Price/Midpoint API OK)
    pricing_verified_count: int = 0

    # Tradability - Book Verified (pricing verified + /book API OK) - 嚴格層級
    book_verified_count: int = 0

    # Reject reasons
    rejects: Dict[RejectReason, int] = field(default_factory=dict)

    def record_reject(self, reason: RejectReason):
        """記錄拒絕原因"""
        self.rejects[reason] = self.rejects.get(reason, 0) + 1

    def summary(self) -> str:
        """生成統計摘要"""
        lines = [
            "📊 Scanner Funnel Summary",
            f"   Discovery: {self.total_events} events → {self.total_markets} markets",
            f"   Parse: {self.parsed_count} parsed ({self.crypto_count} crypto, {self.short_term_count} short-term)",
            f"      Above/Below: {self.above_below_count}, Up/Down: {self.up_down_count}",
            f"   CLOB Eligible: {self.clob_eligible_count}",
            f"   Pricing Verified: {self.pricing_verified_count}",
            f"   Book Verified: {self.book_verified_count}",
            "",
            "   Reject Breakdown:",
        ]
        for reason, count in sorted(self.rejects.items(), key=lambda x: -x[1]):
            lines.append(f"      {reason.value}: {count}")
        return "\n".join(lines)


class PolymarketScannerV2:
    """
    Polymarket Scanner v2.0 - 三層漏斗架構
    """

    GAMMA_API = "https://gamma-api.polymarket.com"
    CLOB_API = "https://clob.polymarket.com"
    CRYPTO_TAG_SLUG = "crypto"
    DISCOVERY_MARKETS_LIMIT_MULTIPLIER = 10
    DISCOVERY_MARKETS_MIN_LIMIT = 100

    # 資產映射
    ASSET_MAPPING = {
        "bitcoin": "BTC",
        "btc": "BTC",
        "ethereum": "ETH",
        "eth": "ETH",
        "solana": "SOL",
        "sol": "SOL",
        "cardano": "ADA",
        "ada": "ADA",
        "ripple": "XRP",
        "xrp": "XRP",
        "polkadot": "DOT",
        "dot": "DOT",
        "polygon": "MATIC",
        "matic": "MATIC",
        "chainlink": "LINK",
        "link": "LINK",
        "avalanche": "AVAX",
        "avax": "AVAX",
        "litecoin": "LTC",
        "ltc": "LTC",
        "dogecoin": "DOGE",
        "doge": "DOGE",
        "shiba": "SHIB",
        "shib": "SHIB",
        "pepe": "PEPE",
        "bonk": "BONK",
        "wif": "WIF",
        "hyperliquid": "HYPE",
        "hype": "HYPE",
        "pump": "PUMP",
        "pumpfun": "PUMP",
        "megaeth": "MEGA",
        "mega": "MEGA",
        "virtual": "VIRTUAL",
        "aixbt": "AIXBT",
        "luna": "LUNA",
        "microstrategy": "MSTR",
        "mstr": "MSTR",
    }
    ASSET_KEYWORDS_SORTED = sorted(
        ASSET_MAPPING.items(), key=lambda item: len(item[0]), reverse=True
    )
    AMBIGUOUS_ASSET_KEYWORDS = {
        "avalanche",
        "avax",
        "link",
        "dot",
        "pump",
        "virtual",
        "luna",
    }

    # 短期時間框架關鍵詞
    TIMEFRAME_PATTERNS = {
        "1m": ["1 minute", "1 min", "1m "],
        "5m": ["5 minute", "5 min", "5m "],
        "15m": ["15 minute", "15 min", "15m "],
        "30m": ["30 minute", "30 min", "30m "],
        "1h": ["1 hour", "1h", "hourly", "next hour"],
        "4h": ["4 hour", "4h"],
        "12h": ["12 hour", "12h"],
        "1d": ["1 day", "1d", "daily", "24 hour", "24h"],
    }

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("POLYMARKET_API_KEY")
        self.session: Optional[aiohttp.ClientSession] = None
        self._public_clob_client: Optional[ClobClient] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
            self.session = None

    # =========================================================================
    # Phase 1: Discovery
    # =========================================================================

    async def get_crypto_tags(self) -> List[Dict]:
        """獲取 crypto 相關 tags"""
        url = f"{self.GAMMA_API}/tags"
        try:
            async with self.session.get(url) as resp:
                if resp.status == 200:
                    tags = await resp.json()
                    crypto_tags = []
                    for tag in tags:
                        name = tag.get("name", "").lower()
                        slug = tag.get("slug", "").lower()
                        if any(
                            kw in name or kw in slug
                            for kw in ["crypto", "bitcoin", "ethereum"]
                        ):
                            crypto_tags.append(tag)
                    return crypto_tags
                return []
        except Exception as e:
            logger.error(f"Error fetching tags: {e}")
            return []

    def _build_events_query_params(self, limit: int) -> Dict[str, Any]:
        """建立官方 events 查詢參數，優先只拉 crypto 類別事件。"""
        return {
            "tag_slug": self.CRYPTO_TAG_SLUG,
            "related_tags": "true",
            "active": "true",
            "closed": "false",
            "archived": "false",
            "order": "endDate",
            "ascending": "true",
            "limit": limit,
            "offset": 0,
        }

    def _build_market_fetch_limit(self, limit: int) -> int:
        """將較小的事件預算放大成 market discovery 視窗，避免樣本過稀。"""
        return max(
            limit * self.DISCOVERY_MARKETS_LIMIT_MULTIPLIER,
            self.DISCOVERY_MARKETS_MIN_LIMIT,
        )

    def _build_markets_query_params(self, limit: int) -> Dict[str, Any]:
        """建立可交易 market discovery 查詢參數。"""
        return {
            "active": "true",
            "closed": "false",
            "archived": "false",
            "enableOrderBook": "true",
            "liquidity_num_min": 1,
            "order": "volume",
            "ascending": "false",
            "limit": self._build_market_fetch_limit(limit),
            "offset": 0,
        }

    def _normalize_market_to_event(self, market: Dict[str, Any]) -> Dict[str, Any]:
        """將 `/markets` 回傳項目轉成 event-like 結構，重用既有 parse 流程。"""
        source_event = {}
        raw_events = market.get("events") or []
        if raw_events:
            source_event = raw_events[0] or {}
        return {
            "id": source_event.get("id") or market.get("id"),
            "slug": source_event.get("slug") or market.get("slug"),
            "title": source_event.get("title") or market.get("question", ""),
            "description": source_event.get("description")
            or market.get("description", ""),
            "startDate": source_event.get("startDate") or market.get("startDate"),
            "endDate": source_event.get("endDate") or market.get("endDate"),
            "markets": [market],
        }

    def _extract_market_id(self, market: Dict[str, Any]) -> Optional[str]:
        """提取 market 主鍵，供 discovery 去重使用。"""
        market_id = (
            market.get("id") or market.get("conditionId") or market.get("condition_id")
        )
        if market_id is None:
            return None
        return str(market_id)

    def _split_event_markets(self, event: Dict[str, Any]) -> List[Dict[str, Any]]:
        """把單一 event 拆成單 market 的 event-like 結構，便於跨來源去重。"""
        split_events: List[Dict[str, Any]] = []
        for market in event.get("markets", []):
            split_events.append(
                {
                    "id": event.get("id"),
                    "slug": event.get("slug"),
                    "title": event.get("title", ""),
                    "description": event.get("description", ""),
                    "startDate": event.get("startDate"),
                    "endDate": event.get("endDate"),
                    "markets": [market],
                }
            )
        return split_events

    def _merge_discovery_events(
        self,
        primary_events: List[Dict[str, Any]],
        secondary_events: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """按 `market_id` 合併 discovery 來源，優先保留主要來源內容。"""
        merged_events: List[Dict[str, Any]] = []
        seen_market_ids: set[str] = set()

        for source_events in (primary_events, secondary_events):
            for event in source_events:
                markets = event.get("markets", [])
                if not markets:
                    continue
                market_id = self._extract_market_id(markets[0])
                if market_id is None:
                    merged_events.append(event)
                    continue
                if market_id in seen_market_ids:
                    continue
                seen_market_ids.add(market_id)
                merged_events.append(event)

        return merged_events

    def _parse_discovery_expiry(
        self,
        event: Dict[str, Any],
    ) -> Optional[datetime]:
        """解析 discovery payload 的到期時間，優先使用 market endDate。"""
        markets = event.get("markets", [])
        market = markets[0] if markets else {}
        end_date_str = market.get("endDate") or event.get("endDate")
        if not end_date_str:
            return None
        try:
            return datetime.fromisoformat(str(end_date_str).replace("Z", "+00:00"))
        except ValueError:
            return None

    def _filter_expired_discovery_events(
        self,
        events: List[Dict[str, Any]],
        now: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """在 discovery 階段硬性剔除已過期 payload，避免髒資料占用後續配額。"""
        current_time = now or datetime.now(timezone.utc)
        filtered_events: List[Dict[str, Any]] = []

        for event in events:
            expiry = self._parse_discovery_expiry(event)
            if expiry is not None and expiry <= current_time:
                continue
            filtered_events.append(event)

        return filtered_events

    async def get_all_markets(self, limit: int = 200) -> List[Dict]:
        """獲取高流動性且可掛單的 market，作為 discovery 主來源。"""
        url = f"{self.GAMMA_API}/markets"
        params = self._build_markets_query_params(limit)

        try:
            async with self.session.get(url, params=params) as resp:
                if resp.status == 200:
                    return await resp.json()
                return []
        except Exception as e:
            logger.error(f"Error fetching markets: {e}")
            return []

    async def get_crypto_events(self, limit: int = 200) -> List[Dict]:
        """獲取近端到期的 crypto events，補足 `/markets` 熱門排序漏掉的樣本。"""
        url = f"{self.GAMMA_API}/events"
        params = self._build_events_query_params(limit)

        try:
            async with self.session.get(url, params=params) as resp:
                if resp.status == 200:
                    return await resp.json()
                return []
        except Exception as e:
            logger.error(f"Error fetching events: {e}")
            return []

    async def get_all_events(
        self,
        limit: int = 200,
        allowed_styles: Optional[set[str]] = None,
    ) -> List[Dict]:
        """建立 discovery 清單，`UP_DOWN` 需合併熱門 market 與近端 crypto events。"""
        markets = await self.get_all_markets(limit=limit)
        market_events = self._filter_expired_discovery_events(
            [self._normalize_market_to_event(market) for market in markets]
        )

        should_merge_crypto_events = allowed_styles == {"up_down"}
        if should_merge_crypto_events:
            crypto_events = await self.get_crypto_events(
                limit=self._build_market_fetch_limit(limit),
            )
            split_crypto_events: List[Dict[str, Any]] = []
            for event in crypto_events:
                split_crypto_events.extend(self._split_event_markets(event))
            return self._merge_discovery_events(
                market_events,
                self._filter_expired_discovery_events(split_crypto_events),
            )

        if market_events:
            return market_events

        fallback_events = await self.get_crypto_events(limit=limit)
        if allowed_styles == {"up_down"}:
            split_fallback_events: List[Dict[str, Any]] = []
            for event in fallback_events:
                split_fallback_events.extend(self._split_event_markets(event))
            return self._filter_expired_discovery_events(split_fallback_events)
        return self._filter_expired_discovery_events(fallback_events)

    def expand_markets(
        self,
        events: List[Dict],
        allowed_styles: Optional[set[str]] = None,
    ) -> List[Tuple[Dict, Dict]]:
        """將 events 展開為 (event, market) 對，必要時先做前置 style 過濾。"""
        markets = []
        for event in events:
            for market in event.get("markets", []):
                if allowed_styles is not None:
                    question = market.get("question", "")
                    detected_style = self._detect_style(question)
                    if detected_style == "UNKNOWN":
                        continue
                    if detected_style.lower() not in allowed_styles:
                        continue
                markets.append((event, market))
        return markets

    def prioritize_markets_for_analysis(
        self,
        candidates: List[Tuple[ParsedMarket, Dict, MarketTradability]],
        allowed_styles: Optional[set[str]] = None,
        now: Optional[datetime] = None,
    ) -> List[Tuple[ParsedMarket, Dict, MarketTradability]]:
        """依研究主線需求，對 `UP_DOWN` 候選做本地窗口距離優先重排。"""
        if not candidates:
            return []

        should_prioritize_up_down = allowed_styles == {"up_down"} or all(
            parsed.style == "UP_DOWN" for parsed, _, _ in candidates
        )
        if not should_prioritize_up_down:
            return list(candidates)

        current_time = now or datetime.now(timezone.utc)

        return sorted(
            candidates,
            key=lambda item: self._build_up_down_priority_key(
                parsed=item[0],
                tradability=item[2],
                now=current_time,
            ),
        )

    def filter_live_markets_for_analysis(
        self,
        candidates: List[Tuple[ParsedMarket, Dict, MarketTradability]],
        allowed_styles: Optional[set[str]] = None,
        now: Optional[datetime] = None,
    ) -> Tuple[
        List[Tuple[ParsedMarket, Dict, MarketTradability]], List[Dict[str, Any]]
    ]:
        """只前置擋掉已過期 `UP_DOWN` 市場，開盤中的 observe 仍需進研究評估。"""
        if not candidates:
            return [], []

        should_filter_up_down = allowed_styles == {"up_down"} or all(
            parsed.style == "UP_DOWN" for parsed, _, _ in candidates
        )
        if not should_filter_up_down:
            return list(candidates), []

        current_time = now or datetime.now(timezone.utc)
        live_candidates: List[Tuple[ParsedMarket, Dict, MarketTradability]] = []
        filtered_out: List[Dict[str, Any]] = []

        for parsed, market, tradability in candidates:
            if parsed.style != "UP_DOWN":
                live_candidates.append((parsed, market, tradability))
                continue

            market_start_timestamp = self._estimate_market_start_timestamp(parsed)
            if market_start_timestamp is not None and market_start_timestamp > current_time:
                # 尚未開盤的市場直接跳過，不記錄到 filtered_out
                continue

            tau_seconds = max((parsed.expiry - current_time).total_seconds(), 0.0)
            if tau_seconds > 0:
                live_candidates.append((parsed, market, tradability))
                continue

            # 已過期的市場直接跳過，不記錄到 filtered_out
            continue

        return live_candidates, filtered_out

    def _estimate_market_start_timestamp(
        self, parsed: ParsedMarket
    ) -> Optional[datetime]:
        """依 `UP_DOWN` 週期估算市場開盤時間。"""
        if parsed.style != "UP_DOWN" or not parsed.timeframe:
            return None

        timeframe_seconds = TIMEFRAME_SECONDS.get(parsed.timeframe)
        if timeframe_seconds is None:
            return None
        return parsed.expiry - timedelta(seconds=timeframe_seconds)

    def _build_up_down_priority_key(
        self,
        parsed: ParsedMarket,
        tradability: MarketTradability,
        now: datetime,
    ) -> Tuple[float, datetime, float]:
        """建立 `UP_DOWN` 市場的研究排序鍵，優先靠近 `armed` 窗口的候選。"""
        tau_seconds = max((parsed.expiry - now).total_seconds(), 0.0)
        armed_window_seconds = TAIL_WINDOWS.get(parsed.timeframe or "", {}).get("armed")
        if armed_window_seconds is None:
            time_to_armed = tau_seconds
        else:
            # 已進入 `armed / attack` 的市場視為 `time_to_armed=0`。
            time_to_armed = max(tau_seconds - armed_window_seconds, 0.0)
        return (
            time_to_armed,
            parsed.expiry,
            -tradability.volume,
        )

    def _resolve_up_down_window_state(
        self, timeframe: Optional[str], tau_seconds: float
    ) -> str:
        """依剩餘時間解析 `UP_DOWN` 市場的 live 窗口狀態。"""
        if tau_seconds <= 0:
            return "expired"
        config = TAIL_WINDOWS.get(timeframe or "")
        if config is None:
            return "observe"
        if tau_seconds <= config["attack"]:
            return "attack"
        if tau_seconds <= config["armed"]:
            return "armed"
        return "observe"

    # =========================================================================
    # Phase 2: Parse
    # =========================================================================

    def parse_market(
        self, event: Dict, market: Dict
    ) -> Tuple[Optional[ParsedMarket], Optional[RejectReason], Dict]:
        """解析單個市場 - 返回詳細診斷信息"""

        # 提取文字
        title = event.get("title", "")
        question = market.get("question", "")
        description = event.get("description", "")
        primary = (title + " " + question).lower()
        combined = (title + " " + question + " " + description).lower()

        # 診斷信息
        diagnostics = {
            "has_crypto_keyword": False,
            "detected_asset": None,
            "detected_style": None,
            "detected_timeframe": None,
            "price_mentioned": "$" in question or "usd" in question.lower(),
        }

        # 1. 檢查是否與加密相關（先檢查關鍵詞）
        crypto_keywords = [
            "bitcoin",
            "btc",
            "ethereum",
            "eth",
            "solana",
            "sol",
            "crypto",
            "cryptocurrency",
            "token",
            "blockchain",
            "defi",
            "nft",
            "altcoin",
            "stablecoin",
        ]
        has_crypto_keyword = any(
            self._contains_keyword(combined, kw) for kw in crypto_keywords
        )
        diagnostics["has_crypto_keyword"] = has_crypto_keyword

        # 2. 檢測資產
        asset, asset_keyword = self._detect_asset(primary)
        diagnostics["detected_asset"] = asset

        if not asset:
            # 細分 reject reason
            if has_crypto_keyword:
                # 有加密關鍵詞但無法映射到具體資產
                return None, RejectReason.CRYPTO_UNSUPPORTED, diagnostics
            else:
                # 完全無關
                return None, RejectReason.NON_CRYPTO_UNRELATED, diagnostics

        if (
            asset_keyword in self.AMBIGUOUS_ASSET_KEYWORDS
            and not has_crypto_keyword
            and not diagnostics["price_mentioned"]
        ):
            return None, RejectReason.NON_CRYPTO_UNRELATED, diagnostics

        # 3. 檢測風格
        style = self._detect_style(question)
        diagnostics["detected_style"] = style

        if style == "UNKNOWN":
            return None, RejectReason.STYLE_UNKNOWN, diagnostics

        # 4. 檢測時間框架
        timeframe = self._detect_timeframe(question)
        diagnostics["detected_timeframe"] = timeframe

        # 5. 提取 strike
        strike = self._extract_strike(question)

        # 6. 解析日期
        end_date_str = market.get("endDate")
        if not end_date_str:
            return None, RejectReason.MISSING_END_DATE, diagnostics

        try:
            expiry = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        except:
            return None, RejectReason.INVALID_DATE_FORMAT, diagnostics

        # 7. 分類標籤
        is_crypto = asset in self.ASSET_MAPPING.values()
        is_short_term = timeframe is not None

        return (
            ParsedMarket(
                raw_event=event,
                raw_market=market,
                asset=asset,
                style=style,
                timeframe=timeframe,
                strike=strike,
                expiry=expiry,
                is_crypto=is_crypto,
                is_short_term=is_short_term,
            ),
            None,
            diagnostics,
        )

    def _detect_asset(self, text: str) -> Tuple[Optional[str], Optional[str]]:
        """檢測資產類型，並返回命中的關鍵詞。"""
        text_lower = text.lower()
        for keyword, asset in self.ASSET_KEYWORDS_SORTED:
            if self._contains_keyword(text_lower, keyword):
                return asset, keyword
        return None, None

    @staticmethod
    def _contains_keyword(text: str, keyword: str) -> bool:
        """以單詞邊界檢查關鍵詞，避免子字串誤判。"""
        escaped_keyword = re.escape(keyword.lower())
        pattern = rf"(?<![a-z0-9]){escaped_keyword}(?![a-z0-9])"
        return re.search(pattern, text.lower()) is not None

    def _detect_style(self, question: str) -> str:
        """檢測市場風格"""
        q_lower = question.lower()

        # Above/Below 模式
        above_below_patterns = [
            "above",
            "below",
            "higher than",
            "lower than",
            "greater than",
            "less than",
            ">",
            "<",
        ]
        if any(
            self._contains_keyword(q_lower, p)
            for p in above_below_patterns
            if p not in {">", "<"}
        ):
            return "ABOVE_BELOW"
        if ">" in q_lower or "<" in q_lower:
            return "ABOVE_BELOW"

        # Up/Down 模式
        up_down_patterns = [
            "up",
            "down",
            "rise",
            "fall",
            "higher",
            "lower",
            "green",
            "red",
        ]
        if any(self._contains_keyword(q_lower, p) for p in up_down_patterns):
            return "UP_DOWN"

        return "UNKNOWN"

    def _detect_timeframe(self, question: str) -> Optional[str]:
        """檢測短期時間框架"""
        q_lower = question.lower()

        for tf, patterns in self.TIMEFRAME_PATTERNS.items():
            if any(p in q_lower for p in patterns):
                return tf
        explicit_window = self._detect_timeframe_from_window(question)
        if explicit_window is not None:
            return explicit_window
        return None

    def _detect_timeframe_from_window(self, question: str) -> Optional[str]:
        """從顯式時間區間推導 timeframe，例如 `11:30AM-11:35AM ET`。"""
        pattern = re.compile(
            r"(\d{1,2}):(\d{2})\s*(am|pm)\s*-\s*(\d{1,2}):(\d{2})\s*(am|pm)",
            re.IGNORECASE,
        )
        match = pattern.search(question)
        if not match:
            return None

        start_hour = int(match.group(1))
        start_minute = int(match.group(2))
        start_meridiem = match.group(3).lower()
        end_hour = int(match.group(4))
        end_minute = int(match.group(5))
        end_meridiem = match.group(6).lower()

        def to_minutes(hour: int, minute: int, meridiem: str) -> int:
            normalized_hour = hour % 12
            if meridiem == "pm":
                normalized_hour += 12
            return normalized_hour * 60 + minute

        start_total = to_minutes(start_hour, start_minute, start_meridiem)
        end_total = to_minutes(end_hour, end_minute, end_meridiem)
        if end_total <= start_total:
            end_total += 24 * 60

        duration_minutes = end_total - start_total
        mapping = {
            5: "5m",
            15: "15m",
            30: "30m",
            60: "1h",
            240: "4h",
            1440: "1d",
        }
        return mapping.get(duration_minutes)

    def _extract_strike(self, question: str) -> Optional[float]:
        """提取 strike price"""
        import re

        patterns = [
            r"\$([\d,]+(?:\.\d+)?)[kK]?",
            r"([\d,]+(?:\.\d+)?)\s*(?:USD|USDT)",
        ]

        for pattern in patterns:
            match = re.search(pattern, question, re.IGNORECASE)
            if match:
                value_str = match.group(1).replace(",", "")
                try:
                    value = float(value_str)
                    if "k" in question.lower() and value < 1000:
                        value *= 1000
                    return value
                except:
                    continue
        return None

    # =========================================================================
    # Phase 3: Tradability
    # =========================================================================

    async def check_tradability(
        self, market: Dict, verify_depth: bool = True
    ) -> MarketTradability:
        """檢查市場可交易性，可選擇是否立即驗證雙邊深度。"""

        market_id = market.get("conditionId", "")
        slug = market.get("slug", "")

        # 1. Status 檢查
        is_active = market.get("active", False)
        is_closed = market.get("closed", False)
        is_archived = market.get("archived", False)

        status_reject = None
        if is_closed:
            status_reject = RejectReason.MARKET_CLOSED
        elif is_archived:
            status_reject = RejectReason.MARKET_ARCHIVED
        elif not is_active:
            status_reject = RejectReason.MARKET_NOT_ACTIVE

        # 2. OrderBook 檢查
        enable_orderbook = market.get("enableOrderBook", False)
        orderbook_reject = None

        yes_token = None
        no_token = None
        has_token_ids = False

        if not enable_orderbook:
            orderbook_reject = RejectReason.ORDERBOOK_DISABLED
        else:
            # 解析 token IDs
            clob_token_ids = market.get("clobTokenIds")
            if clob_token_ids:
                try:
                    token_ids = json.loads(clob_token_ids)
                    yes_token = token_ids[0] if len(token_ids) > 0 else None
                    no_token = token_ids[1] if len(token_ids) > 1 else None
                    has_token_ids = yes_token is not None and no_token is not None
                    if not has_token_ids:
                        orderbook_reject = RejectReason.MISSING_CLOB_TOKEN_IDS
                except:
                    orderbook_reject = RejectReason.INVALID_TOKEN_IDS
            else:
                orderbook_reject = RejectReason.MISSING_CLOB_TOKEN_IDS

        # 3. CLOB API 驗證
        price_available = False
        midpoint_available = False
        book_available = False
        clob_reject = None
        yes_orderbook = None
        no_orderbook = None

        if enable_orderbook and has_token_ids and yes_token and no_token:
            yes_price_available, no_price_available = await asyncio.gather(
                self._check_price_endpoint(yes_token),
                self._check_price_endpoint(no_token),
            )
            price_available = yes_price_available and no_price_available

            yes_midpoint_available, no_midpoint_available = await asyncio.gather(
                self._check_midpoint_endpoint(yes_token),
                self._check_midpoint_endpoint(no_token),
            )
            midpoint_available = yes_midpoint_available and no_midpoint_available

            if verify_depth:
                yes_orderbook, no_orderbook = await asyncio.gather(
                    self._fetch_orderbook(yes_token),
                    self._fetch_orderbook(no_token),
                )
                book_available = yes_orderbook is not None and no_orderbook is not None

            if not price_available and not midpoint_available and not book_available:
                clob_reject = RejectReason.PRICE_ENDPOINT_UNAVAILABLE
            elif verify_depth and not book_available:
                clob_reject = RejectReason.BOOK_NOT_FOUND

        # 4. 綜合評估
        is_clob_eligible = (
            is_active
            and not is_closed
            and not is_archived
            and enable_orderbook
            and has_token_ids
        )

        is_book_verified = is_clob_eligible and book_available

        # 5. 活躍度代理
        volume = float(market.get("volume", 0) or 0)

        return MarketTradability(
            market_id=market_id,
            slug=slug,
            is_active=is_active,
            is_closed=is_closed,
            is_archived=is_archived,
            status_reject=status_reject,
            enable_orderbook=enable_orderbook,
            has_token_ids=has_token_ids,
            yes_token=yes_token,
            no_token=no_token,
            orderbook_reject=orderbook_reject,
            price_available=price_available,
            midpoint_available=midpoint_available,
            book_available=book_available,
            clob_reject=clob_reject,
            is_clob_eligible=is_clob_eligible,
            is_book_verified=is_book_verified,
            volume=volume,
            yes_orderbook=yes_orderbook,
            no_orderbook=no_orderbook,
        )

    async def verify_orderbook_depth(
        self, tradability: MarketTradability
    ) -> MarketTradability:
        """在第二階段為 tradability 補做 YES/NO 雙邊深度驗證。"""
        if (
            not tradability.is_clob_eligible
            or not tradability.yes_token
            or not tradability.no_token
        ):
            return tradability
        if tradability.yes_orderbook is not None and tradability.no_orderbook is not None:
            tradability.book_available = True
            tradability.is_book_verified = True
            return tradability

        tradability.yes_orderbook, tradability.no_orderbook = await asyncio.gather(
            self._fetch_orderbook(tradability.yes_token),
            self._fetch_orderbook(tradability.no_token),
        )
        tradability.book_available = (
            tradability.yes_orderbook is not None
            and tradability.no_orderbook is not None
        )
        tradability.is_book_verified = (
            tradability.is_clob_eligible and tradability.book_available
        )
        if tradability.book_available:
            if tradability.clob_reject == RejectReason.BOOK_NOT_FOUND:
                tradability.clob_reject = None
        else:
            tradability.clob_reject = RejectReason.BOOK_NOT_FOUND
        return tradability

    async def _fetch_orderbook(self, token_id: str) -> Optional[Dict[str, Any]]:
        """透過官方 SDK 抓取單一 token 的標準化訂單簿。"""
        try:
            payload = await asyncio.to_thread(
                self._get_public_clob_client().get_order_book,
                token_id,
            )
        except Exception:
            return None
        return self._normalize_orderbook_payload(payload)

    def _normalize_orderbook_payload(self, payload: Any) -> Optional[Dict[str, Any]]:
        """把官方 SDK 回傳的訂單簿統一轉成 dict 結構。"""
        if isinstance(payload, dict):
            return payload

        bids = getattr(payload, "bids", None)
        asks = getattr(payload, "asks", None)
        if bids is None and asks is None:
            return None
        return {
            "bids": list(bids or []),
            "asks": list(asks or []),
        }

    def _get_public_clob_client(self) -> ClobClient:
        """取得 scanner 共用的公開 CLOB client。"""
        if self._public_clob_client is None:
            self._public_clob_client = ClobClient(host=self.CLOB_API)
        return self._public_clob_client

    async def _check_price_endpoint(self, token_id: str) -> bool:
        """檢查 /price 端點"""
        url = f"{self.CLOB_API}/price"
        params = {"token_id": token_id, "side": "buy"}
        try:
            async with self.session.get(url, params=params, timeout=5) as resp:
                return resp.status == 200
        except:
            return False

    async def _check_midpoint_endpoint(self, token_id: str) -> bool:
        """檢查 /midpoint 端點"""
        url = f"{self.CLOB_API}/midpoint"
        params = {"token_id": token_id}
        try:
            async with self.session.get(url, params=params, timeout=5) as resp:
                return resp.status == 200
        except:
            return False

    async def _check_book_endpoint(self, token_id: str) -> bool:
        """檢查 /book 端點"""
        url = f"{self.CLOB_API}/book"
        params = {"token_id": token_id}
        try:
            async with self.session.get(url, params=params, timeout=5) as resp:
                return resp.status == 200
        except:
            return False


class IntegratedScannerV2:
    """
    集成掃描器 v2.0 - 完整三層漏斗
    """

    def __init__(
        self,
        signal_logger: SignalLogger,
        live_executor: LiveExecutor,
        risk_config: Optional[LiveRiskConfig] = None,
        api_key: Optional[str] = None,
    ):
        self.logger = signal_logger
        self.executor = live_executor
        self.risk = risk_config or LiveRiskConfig()
        self.api_key = api_key or os.getenv("POLYMARKET_API_KEY")

        self.scanner = PolymarketScannerV2(self.api_key)
        self.fair_model = None  # 暫時未實現

        # 結果存儲
        self.found_markets: List[Tuple[Dict, Dict]] = []
        self.parsed_markets: List[Tuple[ParsedMarket, Dict]] = []
        self.clob_eligible: List[Tuple[ParsedMarket, MarketTradability]] = []
        self.pricing_verified: List[Tuple[ParsedMarket, MarketTradability]] = []
        self.book_verified: List[Tuple[ParsedMarket, MarketTradability]] = []
        self.reject_reasons: Dict[RejectReason, List[str]] = {}

    async def run_full_scan(self) -> ScannerFunnelStats:
        """執行完整掃描流程"""
        stats = ScannerFunnelStats()

        async with self.scanner:
            # Phase 1: Discovery
            logger.info("🔍 Phase 1: Discovery")
            events = await self.scanner.get_all_events(limit=200)
            self.found_markets = self.scanner.expand_markets(events)

            stats.total_events = len(events)
            stats.total_markets = len(self.found_markets)
            logger.info(
                f"   Found {stats.total_events} events → {stats.total_markets} markets"
            )

            # Phase 2: Parse
            logger.info("🔍 Phase 2: Parse")
            self.parsed_markets = []
            for event, market in self.found_markets:
                parsed, reject, diagnostics = self.scanner.parse_market(event, market)

                if parsed:
                    self.parsed_markets.append((parsed, market))
                    stats.parsed_count += 1
                    if parsed.is_crypto:
                        stats.crypto_count += 1
                    if parsed.is_short_term:
                        stats.short_term_count += 1
                    if parsed.style == "ABOVE_BELOW":
                        stats.above_below_count += 1
                    elif parsed.style == "UP_DOWN":
                        stats.up_down_count += 1
                else:
                    stats.record_reject(reject)
                    if reject not in self.reject_reasons:
                        self.reject_reasons[reject] = []
                    self.reject_reasons[reject].append(market.get("slug", "unknown"))

            logger.info(f"   Parsed {stats.parsed_count} markets")
            logger.info(
                f"      Crypto: {stats.crypto_count}, Short-term: {stats.short_term_count}"
            )
            logger.info(
                f"      Above/Below: {stats.above_below_count}, Up/Down: {stats.up_down_count}"
            )

            # Phase 3: Tradability
            logger.info("🔍 Phase 3: Tradability")
            self.clob_eligible = []
            self.pricing_verified = []
            self.book_verified = []

            for parsed, market in self.parsed_markets:
                tradability = await self.scanner.check_tradability(market)

                # 記錄 reject reasons
                if tradability.status_reject:
                    stats.record_reject(tradability.status_reject)
                if tradability.orderbook_reject:
                    stats.record_reject(tradability.orderbook_reject)
                if tradability.clob_reject:
                    stats.record_reject(tradability.clob_reject)

                # 三層 tradability
                if tradability.is_clob_eligible:
                    self.clob_eligible.append((parsed, tradability))
                    stats.clob_eligible_count += 1

                    # Pricing verified: CLOB-eligible + pricing API OK
                    if tradability.is_book_verified:
                        self.pricing_verified.append((parsed, tradability))
                        stats.pricing_verified_count += 1

                        # Book verified: pricing verified + /book OK (嚴格層級)
                        self.book_verified.append((parsed, tradability))
                        stats.book_verified_count += 1

            logger.info(f"   CLOB Eligible: {stats.clob_eligible_count}")
            logger.info(f"   Pricing Verified: {stats.pricing_verified_count}")
            logger.info(f"   Book Verified: {stats.book_verified_count}")

        return stats

    def export_results(self, filepath: str):
        """導出掃描結果"""
        results = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "found_count": len(self.found_markets),
            "parsed_count": len(self.parsed_markets),
            "clob_eligible_count": len(self.clob_eligible),
            "pricing_verified_count": len(self.pricing_verified),
            "book_verified_count": len(self.book_verified),
            "clob_eligible_summary": [
                {
                    "asset": p.asset,
                    "style": p.style,
                    "timeframe": p.timeframe,
                    "strike": p.strike,
                    "slug": t.slug,
                    "volume": t.volume,
                    "price_available": t.price_available,
                    "midpoint_available": t.midpoint_available,
                    "book_available": t.book_available,
                }
                for p, t in self.clob_eligible[:20]  # 只導出前 20 個
            ],
            "reject_summary": {k.value: len(v) for k, v in self.reject_reasons.items()},
        }

        with open(filepath, "w") as f:
            json.dump(results, f, indent=2, default=str)

        logger.info(f"💾 Results exported to {filepath}")


# 向後兼容：保留舊的類名
PolymarketScanner = PolymarketScannerV2
IntegratedScanner = IntegratedScannerV2
