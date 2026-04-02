"""
自動交易主線。

把研究掃描、候選機會排序、風控執行與訂單結果整合為同一條路徑。
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, List, Optional

from .research_pipeline import ResearchPipeline, ResearchScanResult, TradingCandidate

if TYPE_CHECKING:
    from .live_executor import LiveExecutionResult, LiveExecutor
    from .settlement_claimer import SettlementClaimResult, SettlementClaimer

logger = logging.getLogger(__name__)
candidate_logger = logging.getLogger("polymarket.candidate")
error_logger = logging.getLogger("polymarket.error")
lifecycle_logger = logging.getLogger("polymarket.lifecycle")


@dataclass(frozen=True)
class AutoTradingCycleResult:
    """單次自動交易循環的摘要結果。"""

    started_at: datetime
    finished_at: datetime
    mode: str
    scanned_event_count: int
    candidate_count: int
    selected_count: int
    executed_count: int
    rejected_count: int
    failed_count: int
    claim_submitted_count: int
    claim_failed_count: int
    claim_dry_run_count: int
    scan_result: ResearchScanResult
    selected_candidates: List[TradingCandidate]
    executions: List[Any]
    claim_results: List[Any]


class AutoTradingPipeline:
    """研究與實盤共用的自動交易主線。"""

    def __init__(
        self,
        research_pipeline: ResearchPipeline,
        live_executor: Optional["LiveExecutor"] = None,
        settlement_claimer: Optional["SettlementClaimer"] = None,
        max_candidates_per_cycle: int = 5,
        post_submit_poll_delay_seconds: float = 2.0,
    ) -> None:
        self.research_pipeline = research_pipeline
        self.live_executor = live_executor
        self.settlement_claimer = settlement_claimer
        self.max_candidates_per_cycle = max_candidates_per_cycle
        self.post_submit_poll_delay_seconds = max(post_submit_poll_delay_seconds, 0.0)

    async def _poll_submitted_execution_once(self, execution: Any) -> Any:
        """對新送出的訂單做一次短延遲即時輪詢。"""
        if self.live_executor is None:
            return execution

        order_id = getattr(execution, "order_id", "")
        status_value = getattr(getattr(execution, "status", None), "value", "")
        if not order_id or status_value not in {
            "submitted",
            "pending",
            "partially_filled",
        }:
            return execution

        if self.post_submit_poll_delay_seconds > 0:
            await asyncio.sleep(self.post_submit_poll_delay_seconds)

        try:
            polled = await asyncio.to_thread(
                self.live_executor.poll_order_status, order_id
            )
        except Exception as exc:
            error_logger.error(
                "新單即時輪詢失敗 | order_id=%s | error=%s",
                order_id,
                exc,
            )
            return execution

        if polled is None:
            return execution

        lifecycle_logger.info(
            "新單即時輪詢 | order_id=%s | status=%s | filled=%.2f",
            polled.order_id,
            polled.status.value,
            polled.filled_size,
        )
        return polled

    async def run_cycle(
        self,
        mode: str,
        limit_events: int,
        allowed_timeframes: Optional[List[str]] = None,
        allowed_assets: Optional[List[str]] = None,
        allowed_styles: Optional[List[str]] = None,
        max_candidates: Optional[int] = None,
        run_auto_claim: bool = False,
        claim_dry_run: bool = False,
    ) -> AutoTradingCycleResult:
        """執行單次掃描或交易循環。"""
        started_at = datetime.now(timezone.utc)

        # 0. 輪詢已提交訂單的成交狀態（release exposure keys for filled/cancelled）
        if mode == "live" and self.live_executor is not None:
            pending = self.live_executor.get_pending_orders()
            for pending_result in pending:
                if pending_result.order_id:
                    try:
                        polled = await asyncio.to_thread(
                            self.live_executor.poll_order_status,
                            pending_result.order_id,
                        )
                        if polled:
                            lifecycle_logger.info(
                                "訂單輪詢 | order_id=%s | status=%s | filled=%.2f",
                                polled.order_id,
                                polled.status.value,
                                polled.filled_size,
                            )
                    except Exception as exc:
                        error_logger.error(
                            "訂單輪詢失敗 | order_id=%s | error=%s",
                            pending_result.order_id,
                            exc,
                        )

        scan_result = await self.research_pipeline.run(
            limit_events=limit_events,
            allowed_timeframes=allowed_timeframes,
            allowed_assets=allowed_assets,
            allowed_styles=allowed_styles,
        )

        candidate_limit = max_candidates or self.max_candidates_per_cycle
        selected_candidates = scan_result.candidates[:candidate_limit]
        rejected_count = max(
            0, scan_result.opportunity_count - len(selected_candidates)
        )
        executions: List[Any] = []
        claim_results: List[Any] = []

        lifecycle_logger.info("[DEBUG] run_cycle mode=%s | selected_candidates=%d", mode, len(selected_candidates))
        if mode == "live":
            if self.live_executor is None:
                raise ValueError("live 模式需要提供 LiveExecutor")

            for candidate in selected_candidates:
                candidate_logger.info(
                    "候選機會 | market=%s | asset=%s | side=%s | edge=%.4f | timeframe=%s | window_state=%s | execution_mode=%s",
                    candidate.opportunity.market_id,
                    candidate.opportunity.asset,
                    candidate.opportunity.selected_side,
                    candidate.opportunity.selected_edge,
                    candidate.opportunity.timeframe,
                    candidate.selected_window_state,
                    candidate.selected_execution_mode,
                )
                try:
                    execution = await asyncio.to_thread(
                        self.live_executor.execute_candidate,
                        candidate,
                    )
                except Exception as exc:
                    error_logger.error(
                        "下單流程例外 | market=%s | error=%s",
                        candidate.opportunity.market_id,
                        exc,
                    )
                    raise
                execution = await self._poll_submitted_execution_once(execution)
                executions.append(execution)

            if run_auto_claim and self.settlement_claimer is not None:
                try:
                    claim_results = await asyncio.to_thread(
                        self.settlement_claimer.scan_and_claim,
                        claim_dry_run,
                    )
                except Exception as exc:
                    error_logger.error("claim 流程例外 | error=%s", exc)
                    raise

        executed_count = sum(
            1
            for execution in executions
            if getattr(getattr(execution, "status", None), "value", "")
            in {"submitted", "filled"}
        )
        failed_count = sum(
            1
            for execution in executions
            if getattr(getattr(execution, "status", None), "value", "") == "failed"
        )
        claim_submitted_count = sum(
            1
            for claim in claim_results
            if getattr(getattr(claim, "status", None), "value", "") == "submitted"
        )
        claim_failed_count = sum(
            1
            for claim in claim_results
            if getattr(getattr(claim, "status", None), "value", "") == "failed"
        )
        claim_dry_run_count = sum(
            1
            for claim in claim_results
            if getattr(getattr(claim, "status", None), "value", "") == "dry_run"
        )
        finished_at = datetime.now(timezone.utc)

        return AutoTradingCycleResult(
            started_at=started_at,
            finished_at=finished_at,
            mode=mode,
            scanned_event_count=scan_result.scanned_event_count,
            candidate_count=scan_result.opportunity_count,
            selected_count=len(selected_candidates),
            executed_count=executed_count,
            rejected_count=rejected_count,
            failed_count=failed_count,
            claim_submitted_count=claim_submitted_count,
            claim_failed_count=claim_failed_count,
            claim_dry_run_count=claim_dry_run_count,
            scan_result=scan_result,
            selected_candidates=selected_candidates,
            executions=executions,
            claim_results=claim_results,
        )

    async def run_forever(
        self,
        mode: str,
        limit_events: int,
        allowed_timeframes: Optional[List[str]] = None,
        allowed_assets: Optional[List[str]] = None,
        allowed_styles: Optional[List[str]] = None,
        max_candidates: Optional[int] = None,
        scan_interval_seconds: int = 10,
        enable_auto_claim: bool = False,
        claim_interval_seconds: int = 300,
        claim_dry_run: bool = False,
    ) -> None:
        """持續輪詢市場並執行交易。"""
        last_claim_scan_at: Optional[datetime] = None
        consecutive_failures = 0
        while True:
            cycle_start = datetime.now(timezone.utc)
            should_run_auto_claim = False
            if enable_auto_claim:
                now = datetime.now(timezone.utc)
                if (
                    last_claim_scan_at is None
                    or (now - last_claim_scan_at).total_seconds()
                    >= claim_interval_seconds
                ):
                    should_run_auto_claim = True
                    last_claim_scan_at = now
            try:
                result = await self.run_cycle(
                    mode=mode,
                    limit_events=limit_events,
                    allowed_timeframes=allowed_timeframes,
                    allowed_assets=allowed_assets,
                    allowed_styles=allowed_styles,
                    max_candidates=max_candidates,
                    run_auto_claim=should_run_auto_claim,
                    claim_dry_run=claim_dry_run,
                )
                consecutive_failures = 0
                lifecycle_logger.info(
                    "循環完成 | mode=%s | scanned=%s | candidates=%s | selected=%s | executed=%s | failed=%s | claim_submitted=%s | claim_failed=%s | claim_dry_run=%s",
                    result.mode,
                    result.scanned_event_count,
                    result.candidate_count,
                    result.selected_count,
                    result.executed_count,
                    result.failed_count,
                    result.claim_submitted_count,
                    result.claim_failed_count,
                    result.claim_dry_run_count,
                )
                candidate_logger.info(
                    "候選拒絕摘要 | %s",
                    result.scan_result.reject_summary,
                )
                for sample in result.scan_result.reject_samples[:10]:
                    candidate_logger.info("候選拒絕樣本 | %s", sample)
                elapsed = (datetime.now(timezone.utc) - cycle_start).total_seconds()
                await asyncio.sleep(max(scan_interval_seconds - elapsed, 0))
            except Exception as exc:
                consecutive_failures += 1
                backoff_seconds = min(
                    scan_interval_seconds * max(consecutive_failures, 1), 300
                )
                error_logger.error(
                    "live loop 失敗 | failure_count=%s | backoff=%ss | error=%s",
                    consecutive_failures,
                    backoff_seconds,
                    exc,
                )
                await asyncio.sleep(backoff_seconds)
