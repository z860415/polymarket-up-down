"""`auto_trading.py` 主迴圈測試。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace

from polymarket_arbitrage.auto_trading import AutoTradingPipeline
from polymarket_arbitrage.live_executor import LiveExecutionResult, LiveExecutionStatus
from polymarket_arbitrage.research_pipeline import ResearchScanResult


@dataclass
class FakeCandidate:
    """模擬研究候選物件。"""

    opportunity: SimpleNamespace
    selected_window_state: str = "attack"


class FakeResearchPipeline:
    """模擬研究主線。"""

    def __init__(self, scan_result: ResearchScanResult) -> None:
        self.scan_result = scan_result

    async def run(self, **kwargs) -> ResearchScanResult:
        return self.scan_result


class FakeLiveExecutor:
    """模擬實盤執行器。"""

    def __init__(
        self,
        execution_result: LiveExecutionResult,
        polled_result: LiveExecutionResult,
    ) -> None:
        self.execution_result = execution_result
        self.polled_result = polled_result
        self.executed_market_ids: list[str] = []
        self.polled_order_ids: list[str] = []

    def get_pending_orders(self) -> list[LiveExecutionResult]:
        return []

    def execute_candidate(self, candidate: FakeCandidate) -> LiveExecutionResult:
        self.executed_market_ids.append(candidate.opportunity.market_id)
        return self.execution_result

    def poll_order_status(self, order_id: str) -> LiveExecutionResult:
        self.polled_order_ids.append(order_id)
        return self.polled_result


def test_run_cycle_polls_newly_submitted_order_once() -> None:
    """live 模式送單後應立即補做一次短延遲輪詢。"""
    candidate = FakeCandidate(
        opportunity=SimpleNamespace(
            market_id="market-1",
            asset="BTC",
            selected_side="YES",
            selected_edge=0.08,
            timeframe="5m",
        )
    )
    scan_result = ResearchScanResult(
        scanned_event_count=1,
        discovered_market_count=1,
        parsed_market_count=1,
        pricing_verified_count=1,
        analyzed_market_count=1,
        opportunity_count=1,
        opportunities=[],
        candidates=[candidate],
        reject_summary={},
        reject_samples=[],
    )
    submitted = LiveExecutionResult(
        order_id="order-1",
        market_id="market-1",
        observation_id="obs-1",
        side="YES",
        size=2.0,
        price=0.52,
        filled_size=0.0,
        avg_fill_price=0.0,
        fee_paid=0.0,
        status=LiveExecutionStatus.SUBMITTED,
        created_at=datetime.now(timezone.utc),
    )
    filled = LiveExecutionResult(
        order_id="order-1",
        market_id="market-1",
        observation_id="obs-1",
        side="YES",
        size=2.0,
        price=0.52,
        filled_size=2.0,
        avg_fill_price=0.52,
        fee_paid=0.0,
        status=LiveExecutionStatus.FILLED,
        created_at=submitted.created_at,
        filled_at=datetime.now(timezone.utc),
    )
    live_executor = FakeLiveExecutor(submitted, filled)
    pipeline = AutoTradingPipeline(
        research_pipeline=FakeResearchPipeline(scan_result),
        live_executor=live_executor,
        max_candidates_per_cycle=1,
        post_submit_poll_delay_seconds=0.0,
    )

    result = asyncio.run(
        pipeline.run_cycle(
            mode="live",
            limit_events=1,
            allowed_timeframes=["5m"],
            allowed_assets=["BTC"],
            allowed_styles=["up_down"],
            max_candidates=1,
        )
    )

    assert live_executor.executed_market_ids == ["market-1"]
    assert live_executor.polled_order_ids == ["order-1"]
    assert result.executions[0].status == LiveExecutionStatus.FILLED
    assert result.executed_count == 1
