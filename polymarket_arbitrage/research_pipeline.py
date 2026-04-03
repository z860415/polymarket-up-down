"""
研究主線：把市場掃描、現貨價格、公平機率與 edge 串成可驗證流程。
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from py_clob_client.client import ClobClient

from binance_client import BinanceClient

from .fair_prob_model import FairProbEstimate
from .fair_prob_model import FairProbabilityModel
from .integrated_scanner_v2 import (
    MarketTradability,
    ParsedMarket,
    PolymarketScannerV2,
    ScannerFunnelStats,
)
from .market_definition import (
    MarketDefinition,
    OracleFamily,
    PayoffType,
    ResolutionOperator,
    SettlementRule,
    StrikeType,
    extract_oracle_config,
)
from .opening_anchor_store import OpeningAnchorStore
from .reference_builder import ReferenceMethod, ReferencePrice, ReferenceStatus
from .signal_logger import SignalLogger, SignalObservation
from .updown_tail_pricer import (
    TAIL_WINDOWS,
    TAIL_SIGMA_WINDOWS,
    MarketRuntimeSnapshot,
    TailStrategyEstimate,
    UpDownTailPricer,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ResearchOpportunity:
    """研究模式產出的交易機會。"""

    market_id: str
    slug: str
    asset: str
    market_style: str
    timeframe: Optional[str]
    question: str
    selected_side: str
    selected_edge: float
    fair_yes: float
    fair_no: float
    yes_bid: Optional[float]
    yes_ask: Optional[float]
    no_bid: Optional[float]
    no_ask: Optional[float]
    anchor_price: Optional[float]
    anchor_timestamp: Optional[datetime]
    spot_price: float
    strike_price: float
    tau_seconds: float
    sigma_tail: Optional[float]
    lead_z: Optional[float]
    window_state: Optional[str]
    time_to_expiry_sec: float
    confidence_score: float
    spread_pct: Optional[float]
    volume: float
    yes_token_id: str
    no_token_id: str
    observation_id: str
    selected_execution_mode: Optional[str] = None


@dataclass(frozen=True)
class TradingCandidate:
    """可直接餵給風控與下單層的候選機會。"""

    opportunity: ResearchOpportunity
    market_definition: MarketDefinition
    reference_price: ReferencePrice
    fair_probability: FairProbEstimate
    observation: SignalObservation
    raw_market: Dict[str, Any]
    parsed_market: ParsedMarket
    tradability: MarketTradability
    runtime_snapshot: Optional[MarketRuntimeSnapshot] = None
    tail_estimate: Optional[TailStrategyEstimate] = None

    @property
    def tick_size(self) -> str:
        """取得官方 SDK 下單需要的 tick size。"""
        tick_size = (
            self.raw_market.get("minimum_tick_size")
            or self.raw_market.get("minimumTickSize")
            or 0.01
        )
        return str(tick_size)

    @property
    def neg_risk(self) -> bool:
        """取得市場是否為 neg risk。"""
        return bool(
            self.raw_market.get("negRisk")
            or self.raw_market.get("neg_risk")
            or self.raw_market.get("enableNegRisk")
        )

    @property
    def selected_net_edge(self) -> float:
        """取得候選機會的淨 edge。"""
        if self.tail_estimate is not None:
            return self.tail_estimate.selected_net_edge
        return self.opportunity.selected_edge

    @property
    def selected_window_state(self) -> Optional[str]:
        """取得尾盤狀態。"""
        if self.runtime_snapshot is not None:
            return self.runtime_snapshot.window_state
        return self.opportunity.window_state

    @property
    def selected_execution_mode(self) -> Optional[str]:
        """取得研究層選定的執行模式。"""
        if self.tail_estimate is not None:
            return self.tail_estimate.selected_execution_mode
        return self.opportunity.selected_execution_mode


@dataclass(frozen=True)
class ResearchScanResult:
    """研究主線掃描結果摘要。"""

    scanned_event_count: int
    discovered_market_count: int
    parsed_market_count: int
    pricing_verified_count: int
    analyzed_market_count: int
    opportunity_count: int
    opportunities: List[ResearchOpportunity]
    candidates: List[TradingCandidate]
    reject_summary: Dict[str, int]
    reject_samples: List[Dict[str, Any]]


class ResearchPipeline:
    """聚焦加密固定 strike 市場的研究掃描主線。"""

    def __init__(
        self,
        signal_logger: SignalLogger,
        min_edge_threshold: float = 0.03,
        min_confidence_score: float = 0.30,
        api_key: Optional[str] = None,
        max_spread_pct: float = 0.10,
        min_market_volume: float = 0.0,
        default_styles: Optional[List[str]] = None,
        anchor_source: str = "settlement_oracle",
        tail_mode: str = "adaptive",
        effective_cost_notional_usdc: float = 1.0,
        market_data_cache_ttl_seconds: float = 10.0,
    ) -> None:
        self.signal_logger = signal_logger
        self.min_edge_threshold = min_edge_threshold
        self.min_confidence_score = min_confidence_score
        self.max_spread_pct = max_spread_pct
        self.min_market_volume = min_market_volume
        self.effective_cost_notional_usdc = max(effective_cost_notional_usdc, 0.1)
        self.market_data_cache_ttl_seconds = max(market_data_cache_ttl_seconds, 0.0)
        self.default_styles = default_styles or ["up_down"]
        self.anchor_source = anchor_source
        self.tail_mode = tail_mode
        self.scanner = PolymarketScannerV2(api_key)
        self.binance_client = BinanceClient()
        self.fair_model = FairProbabilityModel()
        self.anchor_store = OpeningAnchorStore(db_path=signal_logger.db_path)
        self.tail_pricer = UpDownTailPricer()
        self._public_clob_client: Optional[ClobClient] = None
        self._scanner_session_ready = False
        # 研究層行情快取，降低 observe 市場密集重複查詢的 HTTP 成本。
        self._spot_price_cache: Dict[str, Tuple[datetime, Optional[float]]] = {}
        self._volatility_cache: Dict[
            Tuple[str, int], Tuple[datetime, Optional[float]]
        ] = {}

    async def _ensure_scanner_session(self) -> None:
        """確保 scanner 的 aiohttp session 已初始化且可重用。"""
        if self._scanner_session_ready:
            return
        await self.scanner.__aenter__()
        self._scanner_session_ready = True

    async def close(self) -> None:
        """釋放研究層持有的長連線資源。"""
        if not self._scanner_session_ready:
            return
        await self.scanner.__aexit__(None, None, None)
        self._scanner_session_ready = False

    def _is_market_data_cache_fresh(self, cached_at: datetime) -> bool:
        """判斷行情快取是否仍在短 TTL 內。"""
        if self.market_data_cache_ttl_seconds <= 0:
            return False
        return (
            datetime.now(timezone.utc) - cached_at
        ).total_seconds() <= self.market_data_cache_ttl_seconds

    async def _get_spot_price(self, oracle_symbol: str) -> Optional[float]:
        """取得現貨價，並在短時間內重用快取。"""
        cached = self._spot_price_cache.get(oracle_symbol)
        if cached is not None:
            cached_at, cached_value = cached
            if self._is_market_data_cache_fresh(cached_at):
                return cached_value

        spot_price = await asyncio.to_thread(
            self.binance_client.get_spot_price, oracle_symbol
        )
        self._spot_price_cache[oracle_symbol] = (
            datetime.now(timezone.utc),
            spot_price,
        )
        return spot_price

    async def _get_relative_volatility(
        self,
        oracle_symbol: str,
        lookback_minutes: Optional[int] = None,
    ) -> Optional[float]:
        """取得相對波動率，並按資產與視窗做短 TTL 快取。"""
        normalized_lookback_minutes = max(int(lookback_minutes or 0), 0)
        cache_key = (oracle_symbol, normalized_lookback_minutes)
        cached = self._volatility_cache.get(cache_key)
        if cached is not None:
            cached_at, cached_value = cached
            if self._is_market_data_cache_fresh(cached_at):
                return cached_value

        if normalized_lookback_minutes > 0:
            relative_volatility = await asyncio.to_thread(
                self.binance_client.calculate_volatility,
                oracle_symbol,
                normalized_lookback_minutes,
            )
        else:
            relative_volatility = await asyncio.to_thread(
                self.binance_client.calculate_volatility,
                oracle_symbol,
            )
        self._volatility_cache[cache_key] = (
            datetime.now(timezone.utc),
            relative_volatility,
        )
        return relative_volatility

    async def run(
        self,
        limit_events: int = 200,
        allowed_timeframes: Optional[List[str]] = None,
        allowed_assets: Optional[List[str]] = None,
        allowed_styles: Optional[List[str]] = None,
    ) -> ResearchScanResult:
        """執行完整研究掃描。"""
        stats = ScannerFunnelStats()
        tradable_markets: List[
            Tuple[ParsedMarket, Dict[str, Any], MarketTradability]
        ] = []
        analyzed_market_count = 0
        reject_summary: Dict[str, int] = {}
        reject_samples: List[Dict[str, Any]] = []
        normalized_timeframes = self._normalize_timeframes(allowed_timeframes)
        normalized_assets = self._normalize_assets(allowed_assets)
        normalized_styles = self._normalize_styles(
            allowed_styles or self.default_styles
        )
        depth_verified_markets: List[
            Tuple[ParsedMarket, Dict[str, Any], MarketTradability]
        ] = []

        await self._ensure_scanner_session()
        events = await self.scanner.get_all_events(
            limit=limit_events,
            allowed_styles=normalized_styles,
        )
        found_markets = self.scanner.expand_markets(
            events,
            allowed_styles=normalized_styles,
        )

        stats.total_events = len(events)
        stats.total_markets = len(found_markets)

        parsed_markets: List[Tuple[ParsedMarket, Dict[str, Any]]] = []
        for event, market in found_markets:
            parsed, reject_reason, _ = self.scanner.parse_market(event, market)
            if parsed is None:
                if reject_reason is not None:
                    stats.record_reject(reject_reason)
                    reject_summary[reject_reason.value] = (
                        reject_summary.get(reject_reason.value, 0) + 1
                    )
                continue

            if not self._should_include_market(
                parsed=parsed,
                tradability=None,
                allowed_timeframes=normalized_timeframes,
                allowed_assets=normalized_assets,
                allowed_styles=normalized_styles,
            ):
                continue

            parsed_markets.append((parsed, market))
            stats.parsed_count += 1
            if parsed.is_crypto:
                stats.crypto_count += 1
            if parsed.is_short_term:
                stats.short_term_count += 1
            if parsed.style == "ABOVE_BELOW":
                stats.above_below_count += 1
            if parsed.style == "UP_DOWN":
                stats.up_down_count += 1

        for parsed, market in parsed_markets:
            tradability = await self.scanner.check_tradability(
                market,
                verify_depth=False,
            )
            if not self._should_include_market(
                parsed=parsed,
                tradability=tradability,
                allowed_timeframes=normalized_timeframes,
                allowed_assets=normalized_assets,
                allowed_styles=normalized_styles,
            ):
                continue

            if tradability.status_reject is not None:
                stats.record_reject(tradability.status_reject)
                reject_summary[tradability.status_reject.value] = (
                    reject_summary.get(tradability.status_reject.value, 0) + 1
                )
            if tradability.orderbook_reject is not None:
                stats.record_reject(tradability.orderbook_reject)
                reject_summary[tradability.orderbook_reject.value] = (
                    reject_summary.get(tradability.orderbook_reject.value, 0) + 1
                )
            if tradability.clob_reject is not None:
                stats.record_reject(tradability.clob_reject)
                reject_summary[tradability.clob_reject.value] = (
                    reject_summary.get(tradability.clob_reject.value, 0) + 1
                )

            if not tradability.is_clob_eligible:
                continue

            stats.clob_eligible_count += 1

            if not (tradability.price_available or tradability.midpoint_available):
                continue

            stats.pricing_verified_count += 1
            tradable_markets.append((parsed, market, tradability))

        analysis_started_at = datetime.now(timezone.utc)
        tradable_markets = self.scanner.prioritize_markets_for_analysis(
            tradable_markets,
            allowed_styles=normalized_styles,
            now=analysis_started_at,
        )
        tradable_markets, prefiltered_rejects = (
            self.scanner.filter_live_markets_for_analysis(
                tradable_markets,
                allowed_styles=normalized_styles,
                now=analysis_started_at,
            )
        )
        for parsed, market, tradability in tradable_markets:
            tradability = await self.scanner.verify_orderbook_depth(tradability)
            if not tradability.is_book_verified:
                if tradability.clob_reject is not None:
                    stats.record_reject(tradability.clob_reject)
                    reject_summary[tradability.clob_reject.value] = (
                        reject_summary.get(tradability.clob_reject.value, 0) + 1
                    )
                    if len(reject_samples) < 10:
                        reject_samples.append(
                            {
                                "reason": tradability.clob_reject.value,
                                "market_id": str(market.get("id", "")),
                                "question": market.get("question", ""),
                                "asset": parsed.asset,
                                "style": parsed.style,
                                "timeframe": parsed.timeframe,
                            }
                        )
                continue
            depth_verified_markets.append((parsed, market, tradability))

        tradable_markets = depth_verified_markets
        for reject_detail in prefiltered_rejects:
            reject_reason = reject_detail["reason"]
            # 跳過已過期和尚未開盤的市場記錄，只記錄真正有分析價值的拒絕
            if reject_reason in ("market_expired", "market_not_open_yet"):
                continue
            reject_summary[reject_reason] = reject_summary.get(reject_reason, 0) + 1
            if len(reject_samples) < 20:
                reject_samples.append(reject_detail)

        executable_candidates: List[TradingCandidate] = []
        for parsed, market, tradability in tradable_markets:
            analyzed_market_count += 1
            candidate, reject_reason, reject_detail = await self._analyze_market(
                parsed,
                market,
                tradability,
            )
            if candidate is None:
                if reject_reason:
                    reject_summary[reject_reason] = (
                        reject_summary.get(reject_reason, 0) + 1
                    )
                    if reject_detail is not None and len(reject_samples) < 20:
                        reject_samples.append(reject_detail)
                continue
            executable_candidates.append(candidate)

        executable_candidates.sort(
            key=lambda item: item.opportunity.selected_edge,
            reverse=True,
        )
        self.signal_logger.flush()

        ranked_opportunities = [
            candidate.opportunity for candidate in executable_candidates
        ]
        return ResearchScanResult(
            scanned_event_count=stats.total_events,
            discovered_market_count=stats.total_markets,
            parsed_market_count=stats.parsed_count,
            pricing_verified_count=stats.pricing_verified_count,
            analyzed_market_count=analyzed_market_count,
            opportunity_count=len(ranked_opportunities),
            opportunities=ranked_opportunities,
            candidates=executable_candidates,
            reject_summary=reject_summary,
            reject_samples=reject_samples,
        )

    async def export_opportunities_json(
        self,
        filepath: str,
        opportunities: List[ResearchOpportunity],
    ) -> None:
        """把機會清單輸出成 JSON。"""
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "opportunities": [asdict(opportunity) for opportunity in opportunities],
        }
        with open(filepath, "w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)

    async def _analyze_market(
        self,
        parsed: ParsedMarket,
        market: Dict[str, Any],
        tradability: MarketTradability,
    ) -> Tuple[Optional[TradingCandidate], Optional[str], Optional[Dict[str, Any]]]:
        """分析單一市場是否具有研究價值。"""
        if parsed.style != "ABOVE_BELOW" or parsed.strike is None:
            if parsed.style == "UP_DOWN":
                return await self._analyze_up_down_market(parsed, market, tradability)
            return self._build_reject(
                "style_not_supported",
                parsed,
                market,
                detail={"style": parsed.style},
            )

        if not tradability.yes_token or not tradability.no_token:
            return self._build_reject(
                "missing_token_ids",
                parsed,
                market,
            )

        now = datetime.now(timezone.utc)
        question = market.get("question", "")

        yes_orderbook, no_orderbook = await self._get_market_orderbooks(tradability)
        if yes_orderbook is None or no_orderbook is None:
            return self._build_reject(
                "orderbook_unavailable",
                parsed,
                market,
            )

        yes_bid = self._extract_best_price(yes_orderbook.get("bids", []))
        yes_ask = self._extract_best_price(yes_orderbook.get("asks", []))
        no_bid = self._extract_best_price(no_orderbook.get("bids", []))
        no_ask = self._extract_best_price(no_orderbook.get("asks", []))

        # 檢查可疑的陳舊/異常價格（ABOVE_BELOW 市場也需要）
        if self._is_suspicious_price(yes_ask, no_ask):
            logger.warning(
                "ABOVE_BELOW 檢測到可疑價格 - market_id=%s | yes_ask=%s | no_ask=%s",
                tradability.market_id, yes_ask, no_ask
            )
            return self._build_reject(
                "suspicious_stale_prices",
                parsed,
                market,
                detail={
                    "yes_ask": yes_ask,
                    "no_ask": no_ask,
                    "reason": "價格接近 0.99/0.01，可能是流動性枯竭或陳舊數據",
                },
            )

        if yes_ask is None or no_ask is None:
            return self._build_reject(
                "ask_quote_missing",
                parsed,
                market,
            )
        spread_pct = self._calculate_spread_pct(yes_bid, yes_ask, no_bid, no_ask)
        if spread_pct is not None and spread_pct > self.max_spread_pct:
            return self._build_reject(
                "spread_too_wide",
                parsed,
                market,
                detail={
                    "spread_pct": spread_pct,
                    "max_spread_pct": self.max_spread_pct,
                },
            )
        if tradability.volume < self.min_market_volume:
            return self._build_reject(
                "volume_too_low",
                parsed,
                market,
                detail={
                    "volume": tradability.volume,
                    "min_market_volume": self.min_market_volume,
                },
            )

        oracle_symbol = self._build_oracle_symbol(parsed.asset)
        spot_price = await self._get_spot_price(oracle_symbol)
        if spot_price is None:
            return self._build_reject(
                "spot_price_unavailable",
                parsed,
                market,
                detail={"oracle_symbol": oracle_symbol},
            )

        relative_volatility = await self._get_relative_volatility(oracle_symbol)
        annualized_volatility = self._annualize_volatility(relative_volatility)

        try:
            market_definition = self._build_market_definition(
                parsed=parsed,
                market=market,
                tradability=tradability,
                oracle_symbol=oracle_symbol,
            )
        except Exception as error:
            logger.debug("市場定義建構失敗 %s: %s", tradability.slug, error)
            return self._build_reject(
                "market_definition_failed",
                parsed,
                market,
                detail={"error": str(error)},
            )
        reference_price = self._build_reference_price(
            oracle_symbol=oracle_symbol,
            spot_price=spot_price,
            as_of=now,
        )

        try:
            fair_prob = self.fair_model.estimate_settlement_probability(
                market_def=market_definition,
                reference_price=reference_price,
                spot_price=spot_price,
                spot_timestamp=now,
                vol_input=annualized_volatility,
                as_of=now,
            )
        except Exception as error:
            logger.debug("公平機率計算失敗 %s: %s", tradability.slug, error)
            return self._build_reject(
                "fair_probability_failed",
                parsed,
                market,
                detail={"error": str(error)},
            )

        selected_side, selected_edge = self._select_best_edge(
            fair_prob.p_yes,
            fair_prob.p_no,
            yes_ask,
            no_ask,
        )
        # 選項 2: 統一使用 abs() 篩選，並拒絕負 edge
        if abs(selected_edge) < self.min_edge_threshold:
            return self._build_reject(
                "edge_too_low",
                parsed,
                market,
                detail={
                    "selected_edge": selected_edge,
                    "min_edge_threshold": self.min_edge_threshold,
                },
            )
        if selected_edge < 0:
            return self._build_reject(
                "both_edges_negative",
                parsed,
                market,
                detail={
                    "selected_edge": selected_edge,
                    "edge_yes": fair_prob.p_yes - yes_ask,
                    "edge_no": fair_prob.p_no - no_ask,
                },
            )

        if fair_prob.model_confidence_score < self.min_confidence_score:
            return self._build_reject(
                "confidence_too_low",
                parsed,
                market,
                detail={
                    "confidence_score": fair_prob.model_confidence_score,
                    "min_confidence_score": self.min_confidence_score,
                },
            )

        observation = self.signal_logger.log_signal(
            market_def=market_definition,
            ref_price=reference_price,
            fair_prob=fair_prob,
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            no_bid=no_bid,
            no_ask=no_ask,
            as_of=now,
        )

        opportunity = ResearchOpportunity(
            market_id=tradability.market_id,
            slug=tradability.slug,
            asset=parsed.asset,
            market_style=parsed.style,
            timeframe=parsed.timeframe,
            question=question,
            selected_side=selected_side,
            selected_edge=selected_edge,
            fair_yes=fair_prob.p_yes,
            fair_no=fair_prob.p_no,
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            no_bid=no_bid,
            no_ask=no_ask,
            anchor_price=None,
            anchor_timestamp=None,
            spot_price=spot_price,
            strike_price=parsed.strike,
            tau_seconds=fair_prob.time_to_expiry_sec,
            sigma_tail=None,
            lead_z=None,
            window_state=None,
            time_to_expiry_sec=fair_prob.time_to_expiry_sec,
            confidence_score=fair_prob.model_confidence_score,
            spread_pct=spread_pct,
            volume=tradability.volume,
            yes_token_id=tradability.yes_token,
            no_token_id=tradability.no_token,
            observation_id=observation.observation_id,
        )
        return (
            TradingCandidate(
                opportunity=opportunity,
                market_definition=market_definition,
                reference_price=reference_price,
                fair_probability=fair_prob,
                observation=observation,
                raw_market=market,
                parsed_market=parsed,
                tradability=tradability,
            ),
            None,
            None,
        )

    async def _analyze_up_down_market(
        self,
        parsed: ParsedMarket,
        market: Dict[str, Any],
        tradability: MarketTradability,
    ) -> Tuple[Optional[TradingCandidate], Optional[str], Optional[Dict[str, Any]]]:
        """分析 UP / DOWN 尾盤市場。"""
        if parsed.timeframe is None:
            return self._build_reject(
                "timeframe_missing",
                parsed,
                market,
            )
        if not tradability.yes_token or not tradability.no_token:
            return self._build_reject(
                "missing_token_ids",
                parsed,
                market,
            )

        now = datetime.now(timezone.utc)
        question = market.get("question", "")
        sigma_window_seconds = TAIL_SIGMA_WINDOWS.get(parsed.timeframe)
        if sigma_window_seconds is None:
            return self._build_reject(
                "sigma_window_unsupported",
                parsed,
                market,
                detail={"timeframe": parsed.timeframe},
            )
        tau_seconds = max((parsed.expiry - now).total_seconds(), 0.0)
        window_state = self.tail_pricer.resolve_window_state(
            parsed.timeframe, tau_seconds
        ).value
        if window_state == "observe":
            return self._build_reject(
                "window_not_open",
                parsed,
                market,
                detail=self._build_window_not_open_detail(
                    parsed.timeframe,
                    window_state,
                    tau_seconds,
                ),
            )
        if window_state not in {"armed", "attack"}:
            return self._build_reject(
                "window_not_open",
                parsed,
                market,
                detail=self._build_window_not_open_detail(
                    parsed.timeframe,
                    window_state,
                    tau_seconds,
                ),
            )
        if tradability.volume < self.min_market_volume:
            return self._build_reject(
                "volume_too_low",
                parsed,
                market,
                detail={
                    "volume": tradability.volume,
                    "min_market_volume": self.min_market_volume,
                },
            )
        yes_orderbook, no_orderbook = await self._get_market_orderbooks(tradability)
        if yes_orderbook is None or no_orderbook is None:
            return self._build_reject(
                "orderbook_unavailable",
                parsed,
                market,
            )

        yes_bid = self._extract_best_price(yes_orderbook.get("bids", []))
        yes_ask = self._extract_best_price(yes_orderbook.get("asks", []))
        no_bid = self._extract_best_price(no_orderbook.get("bids", []))
        no_ask = self._extract_best_price(no_orderbook.get("asks", []))

        # 檢查可疑的陳舊/異常價格（如 0.99 表示流動性枯竭或數據錯誤）
        if self._is_suspicious_price(yes_ask, no_ask):
            logger.warning(
                "檢測到可疑價格 - market_id=%s | yes_ask=%s | no_ask=%s | "
                "可能是流動性枯竭或陳舊數據，拒絕交易",
                tradability.market_id, yes_ask, no_ask
            )
            return self._build_reject(
                "suspicious_stale_prices",
                parsed,
                market,
                detail={
                    "yes_ask": yes_ask,
                    "no_ask": no_ask,
                    "reason": "價格接近 0.99/0.01，可能是流動性枯竭或陳舊數據",
                },
            )

        yes_effective_ask = self._estimate_effective_buy_price(
            yes_orderbook.get("asks", []),
            self.effective_cost_notional_usdc,
        )
        no_effective_ask = self._estimate_effective_buy_price(
            no_orderbook.get("asks", []),
            self.effective_cost_notional_usdc,
        )
        yes_execution_cost_pct = self._calculate_execution_cost_pct(
            yes_ask, yes_effective_ask
        )
        no_execution_cost_pct = self._calculate_execution_cost_pct(
            no_ask, no_effective_ask
        )
        if yes_bid is None and no_bid is None:
            return self._build_reject(
                "bid_quote_missing",
                parsed,
                market,
                detail={
                    "effective_cost_notional_usdc": self.effective_cost_notional_usdc
                },
            )

        oracle_symbol = self._build_oracle_symbol(parsed.asset)
        spot_price = await self._get_spot_price(oracle_symbol)
        if spot_price is None:
            return self._build_reject(
                "spot_price_unavailable",
                parsed,
                market,
                detail={"oracle_symbol": oracle_symbol},
            )

        try:
            market_definition = self._build_market_definition(
                parsed=parsed,
                market=market,
                tradability=tradability,
                oracle_symbol=oracle_symbol,
            )
        except Exception as error:
            logger.debug("市場定義建構失敗 %s: %s", tradability.slug, error)
            return self._build_reject(
                "market_definition_failed",
                parsed,
                market,
                detail={"error": str(error)},
            )
        if self.anchor_source != "settlement_oracle":
            return self._build_reject(
                "anchor_source_unsupported",
                parsed,
                market,
                detail={"anchor_source": self.anchor_source},
            )

        anchor_record = await asyncio.to_thread(
            self.anchor_store.capture_anchor, market_definition
        )
        if anchor_record is None:
            return self._build_reject(
                "anchor_unavailable",
                parsed,
                market,
            )

        relative_volatility = await self._get_relative_volatility(
            oracle_symbol,
            max(1, math.ceil(sigma_window_seconds / 60)),
        )
        sigma_tail = self._annualize_window_volatility(
            relative_volatility, sigma_window_seconds
        )

        runtime_snapshot = MarketRuntimeSnapshot(
            market_id=tradability.market_id,
            asset=parsed.asset,
            timeframe=parsed.timeframe,
            anchor_price=anchor_record.anchor_price,
            spot_price=spot_price,
            tau_seconds=tau_seconds,
            sigma_tail=sigma_tail,
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            no_bid=no_bid,
            no_ask=no_ask,
            best_depth=self._estimate_best_depth(yes_orderbook, no_orderbook),
            fees_enabled=market_definition.fee_enabled,
            window_state=window_state,
            taker_fee_rate=self._estimate_market_taker_fee_rate(parsed, market),
        )
        tail_estimate = self.tail_pricer.estimate(
            runtime_snapshot,
            yes_execution_cost_pct=yes_execution_cost_pct,
            no_execution_cost_pct=no_execution_cost_pct,
        )
        spread_pct = self._calculate_selected_side_spread_pct(
            selected_side=tail_estimate.selected_side,
            selected_execution_mode=tail_estimate.selected_execution_mode,
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            no_bid=no_bid,
            no_ask=no_ask,
            yes_execution_cost_pct=yes_execution_cost_pct,
            no_execution_cost_pct=no_execution_cost_pct,
        )
        if spread_pct is not None and spread_pct > self.max_spread_pct:
            return self._build_reject(
                "spread_too_wide",
                parsed,
                market,
                detail={
                    "selected_side": tail_estimate.selected_side,
                    "selected_execution_mode": tail_estimate.selected_execution_mode,
                    "spread_pct": spread_pct,
                    "yes_execution_cost_pct": yes_execution_cost_pct,
                    "no_execution_cost_pct": no_execution_cost_pct,
                    "max_spread_pct": self.max_spread_pct,
                    "effective_cost_notional_usdc": self.effective_cost_notional_usdc,
                },
            )
        # NOTE: lead_z check disabled - strategy now purely edge-based
        # if abs(tail_estimate.lead_z) < self.tail_pricer.minimum_lead_z(
        #     parsed.timeframe, window_state
        # ):
        #     return self._build_reject(
        #         "lead_z_too_low",
        #         parsed,
        #         market,
        #         detail={
        #             "lead_z": tail_estimate.lead_z,
        #             "minimum_lead_z": self.tail_pricer.minimum_lead_z(
        #                 parsed.timeframe, window_state
        #             ),
        #             "window_state": window_state,
        #         },
        #     )
        min_edge = self.tail_pricer.minimum_net_edge(parsed.timeframe, window_state)
        if abs(tail_estimate.selected_net_edge) < min_edge:
            return self._build_reject(
                "edge_too_low",
                parsed,
                market,
                detail={
                    "selected_edge": tail_estimate.selected_net_edge,
                    "min_edge_threshold": min_edge,
                    "window_state": window_state,
                    "selected_execution_mode": tail_estimate.selected_execution_mode,
                },
            )
        # 選項 1: 兩邊都負時拒絕交易 (selected_net_edge < 0 表示兩邊都是負 edge)
        if tail_estimate.selected_net_edge < 0:
            return self._build_reject(
                "both_edges_negative",
                parsed,
                market,
                detail={
                    "selected_edge": tail_estimate.selected_net_edge,
                    "net_edge_up": tail_estimate.net_edge_up,
                    "net_edge_down": tail_estimate.net_edge_down,
                    "maker_net_edge_up": tail_estimate.maker_net_edge_up,
                    "maker_net_edge_down": tail_estimate.maker_net_edge_down,
                    "taker_net_edge_up": tail_estimate.taker_net_edge_up,
                    "taker_net_edge_down": tail_estimate.taker_net_edge_down,
                    "window_state": window_state,
                    "selected_execution_mode": tail_estimate.selected_execution_mode,
                },
            )
        if tail_estimate.confidence_score < self.min_confidence_score:
            return self._build_reject(
                "confidence_too_low",
                parsed,
                market,
                detail={
                    "confidence_score": tail_estimate.confidence_score,
                    "min_confidence_score": self.min_confidence_score,
                },
            )

        reference_price = self._build_reference_price(
            oracle_symbol=oracle_symbol,
            spot_price=spot_price,
            as_of=now,
        )
        fair_prob = self._build_tail_fair_probability(
            snapshot=runtime_snapshot,
            estimate=tail_estimate,
            anchor_price=anchor_record.anchor_price,
        )
        observation = self.signal_logger.log_signal(
            market_def=market_definition,
            ref_price=reference_price,
            fair_prob=fair_prob,
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            no_bid=no_bid,
            no_ask=no_ask,
            as_of=now,
            market_style=parsed.style,
            anchor_price=anchor_record.anchor_price,
            anchor_timestamp=anchor_record.anchor_timestamp,
            lead_z=tail_estimate.lead_z,
            sigma_tail=sigma_tail,
            window_state=window_state,
            net_edge_selected=tail_estimate.selected_net_edge,
        )
        opportunity = ResearchOpportunity(
            market_id=tradability.market_id,
            slug=tradability.slug,
            asset=parsed.asset,
            market_style=parsed.style,
            timeframe=parsed.timeframe,
            question=question,
            selected_side=tail_estimate.selected_side,
            selected_edge=tail_estimate.selected_net_edge,
            fair_yes=tail_estimate.p_up,
            fair_no=tail_estimate.p_down,
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            no_bid=no_bid,
            no_ask=no_ask,
            anchor_price=anchor_record.anchor_price,
            anchor_timestamp=anchor_record.anchor_timestamp,
            spot_price=spot_price,
            strike_price=anchor_record.anchor_price,
            tau_seconds=tau_seconds,
            sigma_tail=sigma_tail,
            lead_z=tail_estimate.lead_z,
            window_state=window_state,
            time_to_expiry_sec=tau_seconds,
            confidence_score=tail_estimate.confidence_score,
            spread_pct=spread_pct,
            volume=tradability.volume,
            yes_token_id=tradability.yes_token,
            no_token_id=tradability.no_token,
            observation_id=observation.observation_id,
            selected_execution_mode=tail_estimate.selected_execution_mode,
        )
        return (
            TradingCandidate(
                opportunity=opportunity,
                market_definition=market_definition,
                reference_price=reference_price,
                fair_probability=fair_prob,
                observation=observation,
                raw_market=market,
                parsed_market=parsed,
                tradability=tradability,
                runtime_snapshot=runtime_snapshot,
                tail_estimate=tail_estimate,
            ),
            None,
            None,
        )

    def _build_reject(
        self,
        reason: str,
        parsed: ParsedMarket,
        market: Dict[str, Any],
        detail: Optional[Dict[str, Any]] = None,
    ) -> Tuple[None, str, Dict[str, Any]]:
        """建立研究層拒絕原因與樣本摘要。"""
        payload: Dict[str, Any] = {
            "reason": reason,
            "market_id": str(market.get("id", "")),
            "question": market.get("question", ""),
            "asset": parsed.asset,
            "style": parsed.style,
            "timeframe": parsed.timeframe,
        }
        if detail:
            payload.update(detail)
        return None, reason, payload

    def _build_window_not_open_detail(
        self,
        timeframe: Optional[str],
        window_state: str,
        tau_seconds: float,
    ) -> Dict[str, Any]:
        """建立 `window_not_open` 的統一補充欄位，供監控與日誌使用。"""
        window_config = TAIL_WINDOWS.get(timeframe or "", {})
        armed_seconds = window_config.get("armed")
        attack_seconds = window_config.get("attack")

        if window_state == "observe":
            window_label = "已開盤未進尾盤"
        elif window_state == "armed":
            window_label = "已進入尾盤觀察窗"
        elif window_state == "attack":
            window_label = "已進入攻擊窗"
        elif window_state == "expired":
            window_label = "已過期"
        else:
            window_label = window_state

        return {
            "window_state": window_state,
            "window_label": window_label,
            "tau_seconds": round(tau_seconds, 1),
            "seconds_to_armed": (
                round(max(tau_seconds - armed_seconds, 0.0), 1)
                if armed_seconds is not None
                else None
            ),
            "seconds_to_attack": (
                round(max(tau_seconds - attack_seconds, 0.0), 1)
                if attack_seconds is not None
                else None
            ),
        }

    async def _fetch_orderbook(self, token_id: str) -> Optional[Dict[str, Any]]:
        """抓取單一 token 的訂單簿。"""
        try:
            payload = await asyncio.to_thread(
                self._get_public_clob_client().get_order_book,
                token_id,
            )
        except Exception as error:
            logger.debug("訂單簿抓取失敗 %s: %s", token_id, error)
            return None

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

    async def _get_market_orderbooks(
        self, tradability: MarketTradability
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        始終獲取實時訂單簿數據，確保決策基於最新市場價格。
        不重用舊數據，避免陳舊價格導致錯誤決策。
        """
        # 強制重新獲取實時訂單簿，不使用緩存
        yes_book, no_book = await asyncio.gather(
            self._fetch_orderbook(tradability.yes_token),
            self._fetch_orderbook(tradability.no_token),
        )

        # 記錄獲取時間戳，用於數據新鮮度驗證
        now = datetime.now(timezone.utc).isoformat()
        if yes_book:
            yes_book["_fetched_at"] = now
        if no_book:
            no_book["_fetched_at"] = now

        return yes_book, no_book

    def _get_public_clob_client(self) -> ClobClient:
        """取得研究層使用的公開 CLOB client。"""
        if self._public_clob_client is None:
            self._public_clob_client = ClobClient(host=self.scanner.CLOB_API)
        return self._public_clob_client

    def _build_market_definition(
        self,
        parsed: ParsedMarket,
        market: Dict[str, Any],
        tradability: MarketTradability,
        oracle_symbol: str,
    ) -> MarketDefinition:
        """把 parse 結果轉成公平機率模型可用的市場定義。"""
        question = market.get("question", "")
        is_below = self._is_below_market(question)
        payoff_type = PayoffType.DIGITAL_BELOW if is_below else PayoffType.DIGITAL_ABOVE
        resolution_operator = (
            ResolutionOperator.LT if is_below else ResolutionOperator.GT
        )
        timeframe_seconds = self._timeframe_to_seconds(parsed.timeframe)
        primary_oracle, fallback_oracle = extract_oracle_config(parsed.asset, market)
        market_start_timestamp = (
            parsed.expiry - timedelta(seconds=timeframe_seconds)
            if parsed.style == "UP_DOWN" and timeframe_seconds is not None
            else None
        )
        strike_type = (
            StrikeType.OPEN_PRICE
            if parsed.style == "UP_DOWN"
            else StrikeType.FIXED_PRICE
        )

        return MarketDefinition(
            market_id=tradability.market_id,
            asset=parsed.asset,
            payoff_type=payoff_type,
            resolution_operator=resolution_operator,
            strike_type=strike_type,
            strike_value=parsed.strike if parsed.style == "ABOVE_BELOW" else None,
            upper_strike_value=None,
            strike_timestamp=market_start_timestamp or parsed.expiry,
            strike_window_seconds=60,
            expiry_timestamp=parsed.expiry,
            settlement_rule=SettlementRule.TERMINAL_PRICE,
            oracle_family=primary_oracle.family,
            oracle_symbol=primary_oracle.symbol,
            oracle_decimals=primary_oracle.decimals,
            fallback_oracle_family=fallback_oracle.family if fallback_oracle else None,
            fallback_oracle_symbol=fallback_oracle.symbol if fallback_oracle else None,
            fallback_oracle_decimals=fallback_oracle.decimals
            if fallback_oracle
            else None,
            fee_enabled=bool(market.get("feesEnabled", True)),
            yes_token_id=tradability.yes_token or "",
            no_token_id=tradability.no_token or "",
            raw_question=question,
            raw_description=market.get("description", ""),
            market_style=parsed.style,
            timeframe=parsed.timeframe,
            timeframe_seconds=timeframe_seconds,
            market_start_timestamp=market_start_timestamp,
            settlement_source_descriptor=market.get("oracleSource")
            or f"{primary_oracle.family.value}:{primary_oracle.symbol}",
            anchor_required=parsed.style == "UP_DOWN",
        )

    def _build_reference_price(
        self,
        oracle_symbol: str,
        spot_price: float,
        as_of: datetime,
    ) -> ReferencePrice:
        """建立研究模式使用的即時參考價格。"""
        return ReferencePrice(
            value=spot_price,
            source=OracleFamily.BINANCE,
            symbol=oracle_symbol,
            method=ReferenceMethod.WINDOW_NEAREST_TICK,
            status=ReferenceStatus.PROVISIONAL,
            target_timestamp=as_of,
            source_timestamp=as_of,
            left_timestamp=None,
            right_timestamp=None,
            window_start=as_of,
            window_end=as_of,
            num_ticks_in_window=1,
            num_ticks_total=1,
            quality_score=0.6,
            quality_components={
                "temporal_proximity": 1.0,
                "tick_density": 0.2,
                "method_score": 0.7,
                "freshness_score": 1.0,
            },
            warnings=[],
            prefer_method=ReferenceMethod.WINDOW_NEAREST_TICK,
            allow_interpolation=False,
        )

    def _build_oracle_symbol(self, asset: str) -> str:
        """把資產代碼轉成 Binance 現貨 symbol。"""
        return f"{asset}USDT"

    def _annualize_volatility(self, relative_volatility: float) -> float:
        """把 15 分鐘相對波動率近似轉成年化波動率。"""
        intraday_volatility = max(relative_volatility, 0.002)
        annualization_factor = math.sqrt(365 * 24 * 4)
        annualized_volatility = intraday_volatility * annualization_factor
        return min(max(annualized_volatility, 0.15), 3.0)

    def _annualize_window_volatility(
        self, relative_volatility: float, window_seconds: int
    ) -> float:
        """把任意短窗相對波動率近似轉成年化波動率。"""
        intraday_volatility = max(relative_volatility, 0.0005)
        annualization_factor = math.sqrt(31536000 / max(window_seconds, 1))
        annualized_volatility = intraday_volatility * annualization_factor
        return min(max(annualized_volatility, 0.15), 3.0)

    def _select_best_edge(
        self,
        fair_yes: float,
        fair_no: float,
        yes_ask: float,
        no_ask: float,
    ) -> Tuple[str, float]:
        """計算 YES / NO 兩邊哪一邊 edge 更好。"""
        edge_yes = fair_yes - yes_ask
        edge_no = fair_no - no_ask
        if edge_yes >= edge_no:
            return "YES", edge_yes
        return "NO", edge_no

    def _extract_best_price(self, levels: List[Any]) -> Optional[float]:
        """從不同格式的 orderbook level 中提取最佳價格。"""
        if not levels:
            return None

        first_level = levels[0]
        raw_price: Optional[Any] = None
        if isinstance(first_level, dict):
            raw_price = first_level.get("price")
        elif isinstance(first_level, (list, tuple)) and first_level:
            raw_price = first_level[0]
        elif hasattr(first_level, "price"):
            raw_price = getattr(first_level, "price")

        if raw_price is None:
            return None

        try:
            price = float(raw_price)
        except (TypeError, ValueError):
            return None

        if price > 1:
            return price / 100
        return price

    def _is_suspicious_price(self, yes_ask: Optional[float], no_ask: Optional[float]) -> bool:
        """
        檢測可疑的陳舊或異常價格。
        
        當 yes_ask 和 no_ask 都接近 0.99 時，表示：
        1. 流動性完全枯竭（做市商撤離）
        2. 數據陳舊（使用舊的快照）
        3. 市場即將結算或已暫停交易
        
        這種情況下應拒絕交易，避免基於錯誤價格做決策。
        
        修復 (2026-04-03): 放寬閾值，因 Polymarket API 常返回 yes_ask=no_ask=0.99
        這實際上是訂單簿深度不足，而非數據錯誤。改用 0.995 閾值並增加總和檢查。
        """
        if yes_ask is None or no_ask is None:
            return False
        
        # 閾值放寬：當價格高於 0.995 時視為可疑（原 0.95）
        SUSPICIOUS_THRESHOLD = 0.995
        
        # 如果兩邊價格都極接近 1.0，表示流動性完全枯竭
        if yes_ask >= SUSPICIOUS_THRESHOLD and no_ask >= SUSPICIOUS_THRESHOLD:
            return True
        
        # 如果 yes_ask + no_ask > 1.99，價格明顯異常（原 1.9）
        # 注意：Polymarket 常返回 yes_ask=no_ask=0.99，總和 1.98，這應允許通過
        if (yes_ask + no_ask) > 1.99:
            return True
        
        return False

    def _calculate_spread_pct(
        self,
        yes_bid: Optional[float],
        yes_ask: Optional[float],
        no_bid: Optional[float],
        no_ask: Optional[float],
    ) -> Optional[float]:
        """計算可交易候選機會的最大相對 spread。"""
        spreads: List[float] = []
        if yes_bid is not None and yes_ask is not None and yes_ask > 0:
            spreads.append(max(yes_ask - yes_bid, 0.0) / yes_ask)
        if no_bid is not None and no_ask is not None and no_ask > 0:
            spreads.append(max(no_ask - no_bid, 0.0) / no_ask)
        if not spreads:
            return None
        return max(spreads)

    def _calculate_execution_cost_pct(
        self,
        best_ask: Optional[float],
        effective_ask: Optional[float],
    ) -> Optional[float]:
        """計算單邊有效成交成本比率。"""
        if best_ask is None or effective_ask is None or effective_ask <= 0:
            return None
        return max(effective_ask - best_ask, 0.0) / effective_ask

    def _calculate_selected_side_spread_pct(
        self,
        selected_side: str,
        selected_execution_mode: Optional[str],
        yes_bid: Optional[float],
        yes_ask: Optional[float],
        no_bid: Optional[float],
        no_ask: Optional[float],
        yes_execution_cost_pct: Optional[float],
        no_execution_cost_pct: Optional[float],
    ) -> Optional[float]:
        """依選定方向與執行模式計算對應的 friction 比率。"""
        if selected_execution_mode == "taker":
            return (
                yes_execution_cost_pct
                if selected_side == "YES"
                else no_execution_cost_pct
            )

        if selected_side == "YES":
            return self._calculate_quote_spread_pct(yes_bid, yes_ask)
        return self._calculate_quote_spread_pct(no_bid, no_ask)

    def _calculate_quote_spread_pct(
        self,
        best_bid: Optional[float],
        best_ask: Optional[float],
    ) -> Optional[float]:
        """計算單邊最佳 bid/ask 的相對 spread。"""
        if best_bid is None or best_ask is None or best_ask <= 0:
            return None
        return max(best_ask - best_bid, 0.0) / best_ask

    def _estimate_market_taker_fee_rate(
        self,
        parsed: ParsedMarket,
        market: Dict[str, Any],
    ) -> float:
        """估算市場 taker 費率，優先使用市場欄位，否則回退至 crypto 預設。"""
        if not bool(market.get("feesEnabled", True)):
            return 0.0

        ratio_keys = (
            "takerFeeRate",
            "taker_fee_rate",
            "feeRate",
            "fee_rate",
        )
        for key in ratio_keys:
            raw_value = market.get(key)
            if raw_value in (None, ""):
                continue
            try:
                value = float(raw_value)
            except (TypeError, ValueError):
                continue
            if value > 1:
                value /= 10000.0
            return max(value, 0.0)

        bps_keys = (
            "takerFeeRateBps",
            "feeRateBps",
            "base_fee",
            "baseFee",
        )
        for key in bps_keys:
            raw_value = market.get(key)
            if raw_value in (None, ""):
                continue
            try:
                return max(float(raw_value) / 10000.0, 0.0)
            except (TypeError, ValueError):
                continue

        if parsed.is_crypto:
            return 0.072
        return 0.0

    def _normalize_timeframes(
        self, allowed_timeframes: Optional[List[str]]
    ) -> Optional[set[str]]:
        """正規化 timeframe 過濾條件。"""
        if not allowed_timeframes:
            return None
        return {
            timeframe.strip().lower()
            for timeframe in allowed_timeframes
            if timeframe.strip()
        }

    def _normalize_assets(
        self, allowed_assets: Optional[List[str]]
    ) -> Optional[set[str]]:
        """正規化資產過濾條件。"""
        if not allowed_assets:
            return None
        return {asset.strip().upper() for asset in allowed_assets if asset.strip()}

    def _normalize_styles(
        self, allowed_styles: Optional[List[str]]
    ) -> Optional[set[str]]:
        """正規化 style 過濾條件。"""
        if not allowed_styles:
            return None
        return {style.strip().lower() for style in allowed_styles if style.strip()}

    def _should_include_market(
        self,
        parsed: ParsedMarket,
        tradability: Optional[MarketTradability],
        allowed_timeframes: Optional[set[str]],
        allowed_assets: Optional[set[str]],
        allowed_styles: Optional[set[str]],
    ) -> bool:
        """根據新主線要求過濾市場。"""
        if allowed_styles is not None and parsed.style.lower() not in allowed_styles:
            return False
        if parsed.style == "UP_DOWN":
            if parsed.expiry <= datetime.now(timezone.utc):
                return False
            if parsed.timeframe is None:
                return False
            if (
                allowed_timeframes is not None
                and parsed.timeframe.lower() not in allowed_timeframes
            ):
                return False
        if allowed_assets is not None and parsed.asset.upper() not in allowed_assets:
            return False
        if tradability is not None and tradability.volume < self.min_market_volume:
            return False
        return True

    def _is_below_market(self, question: str) -> bool:
        """判斷問題語意是否為看跌 / 跌破型事件。"""
        question_lower = question.lower()
        below_keywords = [
            "below",
            "lower than",
            "less than",
            "under",
            "dip to",
            "fall to",
            "<",
        ]
        return any(keyword in question_lower for keyword in below_keywords)

    def _timeframe_to_seconds(self, timeframe: Optional[str]) -> Optional[int]:
        """將 timeframe 轉成秒數。"""
        mapping = {
            "1m": 1 * 60,
            "5m": 5 * 60,
            "15m": 15 * 60,
            "30m": 30 * 60,
            "1h": 60 * 60,
            "4h": 4 * 60 * 60,
            "12h": 12 * 60 * 60,
            "1d": 24 * 60 * 60,
        }
        if timeframe is None:
            return None
        return mapping.get(timeframe)

    def _estimate_best_depth(
        self,
        yes_orderbook: Dict[str, Any],
        no_orderbook: Dict[str, Any],
    ) -> float:
        """估算最佳檔位深度。"""
        levels = []
        for book in (yes_orderbook, no_orderbook):
            top_ask = self._extract_top_level(book.get("asks", []))
            top_bid = self._extract_top_level(book.get("bids", []))
            for top_level in (top_ask, top_bid):
                if top_level is not None:
                    levels.append(top_level[0] * top_level[1])
        if not levels:
            return 0.0
        return max(levels)

    def _extract_top_level(self, levels: List[Any]) -> Optional[Tuple[float, float]]:
        """提取訂單簿第一檔的價格與數量。"""
        if not levels:
            return None
        first_level = levels[0]
        raw_price = None
        raw_size = None
        if isinstance(first_level, dict):
            raw_price = first_level.get("price")
            raw_size = first_level.get("size") or first_level.get("quantity")
        elif isinstance(first_level, (list, tuple)) and len(first_level) >= 2:
            raw_price = first_level[0]
            raw_size = first_level[1]
        if raw_price is None or raw_size is None:
            return None
        try:
            price = float(raw_price)
            size = float(raw_size)
        except (TypeError, ValueError):
            return None
        if price > 1:
            price = price / 100
        return price, size

    def _estimate_effective_buy_price(
        self,
        levels: List[Any],
        target_notional_usdc: float,
    ) -> Optional[float]:
        """估算固定 notional 在 ask 盤逐檔成交的加權平均價格。"""
        if not levels:
            return None

        remaining_notional = target_notional_usdc
        filled_notional = 0.0
        filled_size = 0.0

        for level in levels:
            normalized_level = self._normalize_orderbook_level(level)
            if normalized_level is None:
                continue
            price, size = normalized_level
            if price <= 0 or size <= 0:
                continue

            level_notional = price * size
            take_notional = min(level_notional, remaining_notional)
            if take_notional <= 0:
                continue

            filled_notional += take_notional
            filled_size += take_notional / price
            remaining_notional -= take_notional
            if remaining_notional <= 1e-9:
                break

        if filled_notional + 1e-9 < target_notional_usdc or filled_size <= 0:
            return None
        return filled_notional / filled_size

    def _normalize_orderbook_level(self, level: Any) -> Optional[Tuple[float, float]]:
        """把不同格式的 order book 檔位正規化為 `(price, size)`。"""
        raw_price = None
        raw_size = None
        if isinstance(level, dict):
            raw_price = level.get("price")
            raw_size = level.get("size") or level.get("quantity")
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            raw_price = level[0]
            raw_size = level[1]
        elif hasattr(level, "price"):
            raw_price = getattr(level, "price")
            raw_size = getattr(level, "size", None) or getattr(level, "quantity", None)

        if raw_price is None or raw_size is None:
            return None
        try:
            price = float(raw_price)
            size = float(raw_size)
        except (TypeError, ValueError):
            return None
        if price > 1:
            price = price / 100
        return price, size

    def _build_tail_fair_probability(
        self,
        snapshot: MarketRuntimeSnapshot,
        estimate: TailStrategyEstimate,
        anchor_price: float,
    ) -> FairProbEstimate:
        """把尾盤定價結果轉成共用的公平機率結構。"""
        return FairProbEstimate(
            p_yes=estimate.p_up,
            p_no=estimate.p_down,
            fair_yes_price=estimate.p_up,
            fair_no_price=estimate.p_down,
            model_version="updown_tail_pricer_v1",
            assumptions={
                "market_style": "UP_DOWN",
                "timeframe": snapshot.timeframe,
                "window_state": snapshot.window_state,
                "selected_execution_mode": estimate.selected_execution_mode,
                "fee_cost": estimate.fee_cost,
                "slippage_cost": estimate.slippage_cost,
                "slippage_cost_up": estimate.slippage_cost_up,
                "slippage_cost_down": estimate.slippage_cost_down,
                "fill_penalty": estimate.fill_penalty,
                "net_edge_up": estimate.net_edge_up,
                "net_edge_down": estimate.net_edge_down,
                "maker_net_edge_up": estimate.maker_net_edge_up,
                "maker_net_edge_down": estimate.maker_net_edge_down,
                "taker_net_edge_up": estimate.taker_net_edge_up,
                "taker_net_edge_down": estimate.taker_net_edge_down,
            },
            model_confidence_score=estimate.confidence_score,
            input_quality_score=0.95,
            input_freshness_ms=0.0,
            strike_price=anchor_price,
            spot_price=snapshot.spot_price,
            time_to_expiry_sec=snapshot.tau_seconds,
            volatility=snapshot.sigma_tail,
            drift=0.0,
            warning_flags=[],
        )
