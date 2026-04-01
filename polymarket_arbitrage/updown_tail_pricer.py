"""
UP / DOWN 尾盤定價器。
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum
from statistics import NormalDist
from typing import Optional


class WindowState(str, Enum):
    """尾盤狀態。"""

    OBSERVE = "observe"
    ARMED = "armed"
    ATTACK = "attack"
    EXPIRED = "expired"


TAIL_SIGMA_WINDOWS = {
    "1m": 30,
    "5m": 120,
    "15m": 300,
    "1h": 900,
    "4h": 3600,
    "12h": 7200,
    "1d": 10800,
}

TAIL_WINDOWS = {
    "1m": {"armed": 20, "attack": 8},
    "5m": {"armed": 90, "attack": 35},
    "15m": {"armed": 180, "attack": 75},
    "1h": {"armed": 720, "attack": 240},
    "4h": {"armed": 2400, "attack": 900},
    "12h": {"armed": 4200, "attack": 1500},
    "1d": {"armed": 5400, "attack": 1800},
}

MIN_NET_EDGE = {
    "1m": 0.08,    # was 0.12
    "5m": 0.06,    # was 0.10
    "15m": 0.05,   # was 0.08 → 0.05 (用戶要求 0.02, 但考慮成本設 0.05)
    "1h": 0.05,    # was 0.08
    "4h": 0.04,    # was 0.06 → 0.04 (用戶要求 0.025)
    "12h": 0.05,   # was 0.07
    "1d": 0.05,    # was 0.07
}

MIN_LEAD_Z = {
    "1m": 2.5,
    "5m": 0.3,      # 下調: 1.5 → 0.3 (用戶要求)
    "15m": 0.8,     # 下調: 1.0 → 0.8 (attack 用固定值 0.8)
    "1h": 1.9,
    "4h": 1.4,
    "12h": 1.7,
    "1d": 1.7,
}

TIMEFRAME_POSITION_BUCKET = {
    "1m": 0.02,    # 2% (was 0.75%)
    "5m": 0.025,   # 2.5% (was 1%)
    "15m": 0.03,   # 3% (was 1.25%)
    "1h": 0.035,   # 3.5% (was 1.5%)
    "4h": 0.04,    # 4% (was 2%)
    "12h": 0.035,  # 3.5% (was 1.75%)
    "1d": 0.035,   # 3.5% (was 1.5%)
}

# ---------------------------------------------------------------------------
# Observe 階段（盤口定價套利）：門檻比尾盤低，只用 maker 掛單
# ---------------------------------------------------------------------------
OBSERVE_MIN_NET_EDGE = {
    "1m": 0.05,    # was 0.08
    "5m": 0.04,    # was 0.06 → 0.04
    "15m": 0.03,   # was 0.05 → 0.03 (用戶要求 0.02)
    "1h": 0.03,    # was 0.05
    "4h": 0.03,    # was 0.05 → 0.025 (用戶要求)
    "12h": 0.03,   # was 0.05
    "1d": 0.03,    # was 0.05
}

OBSERVE_MIN_LEAD_Z = {
    "1m": 0.8,
    "5m": 0.5,
    "15m": 0.3,
    "1h": 0.3,
    "4h": 0.3,
    "12h": 0.3,
    "1d": 0.3,
}

OBSERVE_POSITION_BUCKET = {
    "1m": 0.015,   # 1.5% (was 0.5%)
    "5m": 0.015,   # 1.5% (was 0.5%)
    "15m": 0.02,   # 2% (was 0.75%)
    "1h": 0.025,   # 2.5% (was 1%)
    "4h": 0.03,    # 3% (was 1%)
    "12h": 0.025,  # 2.5% (was 1%)
    "1d": 0.025,   # 2.5% (was 1%)
}


@dataclass(frozen=True)
class MarketRuntimeSnapshot:
    """研究與執行共用的市場即時狀態。"""

    market_id: str
    asset: str
    timeframe: str
    anchor_price: float
    spot_price: float
    tau_seconds: float
    sigma_tail: float
    yes_bid: Optional[float]
    yes_ask: Optional[float]
    no_bid: Optional[float]
    no_ask: Optional[float]
    best_depth: float
    fees_enabled: bool
    window_state: str


@dataclass(frozen=True)
class TailStrategyEstimate:
    """尾盤策略估計結果。"""

    p_up: float
    p_down: float
    lead_z: float
    gross_edge_up: float
    gross_edge_down: float
    fee_cost: float
    slippage_cost_up: float
    slippage_cost_down: float
    slippage_cost: float
    fill_penalty: float
    net_edge_up: float
    net_edge_down: float
    selected_side: str
    selected_net_edge: float
    window_state: str
    confidence_score: float


class UpDownTailPricer:
    """以剩餘波動率與尾盤窗口估算 UP / DOWN 公平勝率。"""

    def resolve_window_state(self, timeframe: str, tau_seconds: float) -> WindowState:
        """依剩餘時間判定尾盤狀態。"""
        if tau_seconds <= 0:
            return WindowState.EXPIRED
        config = TAIL_WINDOWS.get(timeframe)
        if config is None:
            return WindowState.OBSERVE
        if tau_seconds <= config["attack"]:
            return WindowState.ATTACK
        if tau_seconds <= config["armed"]:
            return WindowState.ARMED
        return WindowState.OBSERVE

    def estimate(
        self,
        snapshot: MarketRuntimeSnapshot,
        yes_execution_cost_pct: Optional[float] = None,
        no_execution_cost_pct: Optional[float] = None,
    ) -> TailStrategyEstimate:
        """計算 UP / DOWN 尾盤勝率與淨 edge。"""
        tau_years = max(snapshot.tau_seconds, 0.0) / 31536000
        if snapshot.anchor_price <= 0 or snapshot.spot_price <= 0:
            lead_z = 0.0
            p_up = 0.5
        elif tau_years <= 0 or snapshot.sigma_tail <= 0:
            if snapshot.spot_price > snapshot.anchor_price:
                p_up = 1.0
            elif snapshot.spot_price < snapshot.anchor_price:
                p_up = 0.0
            else:
                p_up = 0.5
            lead_z = 0.0
        else:
            lead_z = math.log(snapshot.spot_price / snapshot.anchor_price) / (
                snapshot.sigma_tail * math.sqrt(tau_years)
            )
            p_up = NormalDist().cdf(lead_z)
        p_down = 1.0 - p_up

        gross_edge_up = p_up - (snapshot.yes_ask or 1.0)
        gross_edge_down = p_down - (snapshot.no_ask or 1.0)
        fee_cost = self._estimate_fee_cost(snapshot)
        slippage_cost_up = self._estimate_slippage_cost(yes_execution_cost_pct)
        slippage_cost_down = self._estimate_slippage_cost(no_execution_cost_pct)
        fill_penalty = self._estimate_fill_penalty(snapshot)

        net_edge_up = gross_edge_up - fee_cost - slippage_cost_up - fill_penalty
        net_edge_down = gross_edge_down - fee_cost - slippage_cost_down - fill_penalty

        if net_edge_up >= net_edge_down:
            selected_side = "YES"
            selected_net_edge = net_edge_up
            slippage_cost = slippage_cost_up
        else:
            selected_side = "NO"
            selected_net_edge = net_edge_down
            slippage_cost = slippage_cost_down

        confidence_score = max(min((abs(lead_z) / 3.0), 1.0), 0.35)

        return TailStrategyEstimate(
            p_up=p_up,
            p_down=p_down,
            lead_z=lead_z,
            gross_edge_up=gross_edge_up,
            gross_edge_down=gross_edge_down,
            fee_cost=fee_cost,
            slippage_cost_up=slippage_cost_up,
            slippage_cost_down=slippage_cost_down,
            slippage_cost=slippage_cost,
            fill_penalty=fill_penalty,
            net_edge_up=net_edge_up,
            net_edge_down=net_edge_down,
            selected_side=selected_side,
            selected_net_edge=selected_net_edge,
            window_state=snapshot.window_state,
            confidence_score=confidence_score,
        )

    def minimum_net_edge(self, timeframe: str, window_state: str = "armed") -> float:
        """取得各週期最低淨 edge。"""
        if window_state == "observe":
            return OBSERVE_MIN_NET_EDGE.get(timeframe, 0.03)
        return MIN_NET_EDGE.get(timeframe, 0.05)

    def minimum_lead_z(self, timeframe: str, window_state: str = "armed") -> float:
        """取得各週期最低 lead_z。"""
        # attack 窗口使用固定門檻，不適用 adaptive 縮放
        if window_state == "attack":
            if timeframe in ("5m", "15m"):
                return 0.8
            # 其他時間框架 attack 狀態也設上限
            computed = MIN_LEAD_Z.get(timeframe, 2.0)
            return min(computed, 1.2)
        
        if window_state == "observe":
            return OBSERVE_MIN_LEAD_Z.get(timeframe, 0.5)
        
        # armed 狀態：正常取值但設硬上限 1.2
        computed = MIN_LEAD_Z.get(timeframe, 2.0)
        return min(computed, 1.2)

    def position_bucket(self, timeframe: str, window_state: str = "armed") -> float:
        """取得各週期最大倉位比例。"""
        if window_state == "observe":
            return OBSERVE_POSITION_BUCKET.get(timeframe, 0.005)
        return TIMEFRAME_POSITION_BUCKET.get(timeframe, 0.01)

    def is_observe_eligible(
        self, timeframe: str, lead_z: float, net_edge: float
    ) -> bool:
        """判斷 observe 階段的市場是否達到掛單門檻。"""
        min_edge = self.minimum_net_edge(timeframe, "observe")
        # NOTE: lead_z check removed - purely edge-based strategy
        # Use abs() to allow both positive (long) and negative (short) edges
        return abs(net_edge) >= min_edge

    def _estimate_fee_cost(self, snapshot: MarketRuntimeSnapshot) -> float:
        """估算成本中的手續費部分。"""
        if not snapshot.fees_enabled:
            return 0.0
        if snapshot.timeframe in {"1m", "5m", "15m"}:
            return 0.008
        return 0.004

    def _estimate_slippage_cost(self, spread_pct: Optional[float]) -> float:
        """估算滑價成本。"""
        normalized_spread = max(spread_pct or 0.0, 0.0)
        return min(normalized_spread * 0.35, 0.03)

    def _estimate_fill_penalty(self, snapshot: MarketRuntimeSnapshot) -> float:
        """估算成交風險懲罰。"""
        if snapshot.best_depth >= 500:
            return 0.0
        if snapshot.best_depth >= 100:
            return 0.005
        return 0.012
