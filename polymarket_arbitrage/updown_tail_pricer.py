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
    "1m": {"armed": 15, "attack": 10},
    "5m": {"armed": 60, "attack": 120},  # 收盤前2分鐘實際下單: armed=60s準備, attack=120s交易
    "15m": {"armed": 120, "attack": 90},
    "1h": {"armed": 600, "attack": 300},
    "4h": {"armed": 1800, "attack": 1200},
    "12h": {"armed": 3600, "attack": 1800},
    "1d": {"armed": 4800, "attack": 2400},
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
    taker_fee_rate: float = 0.0


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
    maker_net_edge_up: float = 0.0
    maker_net_edge_down: float = 0.0
    taker_net_edge_up: float = 0.0
    taker_net_edge_down: float = 0.0
    selected_execution_mode: str = "maker"


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

        # 檢查價格數據有效性 - 若為 None 則無法計算 Edge
        if snapshot.yes_bid is None or snapshot.no_bid is None:
            return TailStrategyEstimate(
                p_up=p_up,
                p_down=p_down,
                selected_side="YES",
                selected_net_edge=-1.0,
                selected_execution_mode="maker",
                confidence_score=0.0,
                maker_fill_penalty=0.0,
                fee_cost=0.0,
                slippage_cost=0.0,
                fill_penalty=0.0,
            )

        gross_edge_up = p_up - snapshot.yes_bid
        gross_edge_down = p_down - snapshot.no_bid
        maker_fill_penalty = self._estimate_fill_penalty(snapshot)
        maker_net_edge_up = gross_edge_up - maker_fill_penalty
        maker_net_edge_down = gross_edge_down - maker_fill_penalty

        # 檢查 ask 價格有效性
        if snapshot.yes_ask is None or snapshot.no_ask is None:
            # 無 taker 價格，僅使用 maker 邏輯
            taker_net_edge_up = -1.0
            taker_net_edge_down = -1.0
        else:
            taker_gross_edge_up = p_up - snapshot.yes_ask
            taker_gross_edge_down = p_down - snapshot.no_ask
            taker_fee_cost_up = self._estimate_taker_fee_cost(snapshot, snapshot.yes_ask)
            taker_fee_cost_down = self._estimate_taker_fee_cost(snapshot, snapshot.no_ask)
            slippage_cost_up = self._estimate_slippage_cost(yes_execution_cost_pct)
            slippage_cost_down = self._estimate_slippage_cost(no_execution_cost_pct)
            taker_net_edge_up = (
                taker_gross_edge_up - taker_fee_cost_up - slippage_cost_up
            )
            taker_net_edge_down = (
                taker_gross_edge_down - taker_fee_cost_down - slippage_cost_down
            )

        if maker_net_edge_up >= maker_net_edge_down:
            selected_side = "YES"
            selected_net_edge = maker_net_edge_up
            fee_cost = 0.0
            slippage_cost = 0.0
        else:
            selected_side = "NO"
            selected_net_edge = maker_net_edge_down
            fee_cost = 0.0
            slippage_cost = 0.0

        selected_execution_mode = "maker"
        fill_penalty = maker_fill_penalty

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
            net_edge_up=maker_net_edge_up,
            net_edge_down=maker_net_edge_down,
            selected_side=selected_side,
            selected_net_edge=selected_net_edge,
            window_state=snapshot.window_state,
            confidence_score=confidence_score,
            maker_net_edge_up=maker_net_edge_up,
            maker_net_edge_down=maker_net_edge_down,
            taker_net_edge_up=taker_net_edge_up,
            taker_net_edge_down=taker_net_edge_down,
            selected_execution_mode=selected_execution_mode,
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

    def _estimate_taker_fee_cost(
        self,
        snapshot: MarketRuntimeSnapshot,
        execution_price: Optional[float],
    ) -> float:
        """依 Polymarket 官方公式估算 taker 相對成本。"""
        if (
            not snapshot.fees_enabled
            or execution_price is None
            or execution_price <= 0
            or snapshot.taker_fee_rate <= 0
        ):
            return 0.0
        normalized_price = min(max(execution_price, 0.0), 1.0)
        return snapshot.taker_fee_rate * (1.0 - normalized_price)

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
