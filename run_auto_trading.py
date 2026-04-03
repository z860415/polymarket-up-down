#!/usr/bin/env python3
"""
自動交易主入口。
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

try:
    from dotenv import load_dotenv

    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
except ModuleNotFoundError:
    pass

logger = logging.getLogger(__name__)
lifecycle_logger = logging.getLogger("polymarket.lifecycle")
preflight_logger = logging.getLogger("polymarket.preflight")


def print_preflight_report(report) -> None:
    """輸出 preflight 摘要。"""
    print("=" * 80)
    print(f"preflight status={report.status.value} | ready={report.ready}")
    if getattr(report, "signer_address", None):
        print(f"signer={report.signer_address}")
    if getattr(report, "proxy_wallet", None):
        print(f"proxy_wallet={report.proxy_wallet}")
    if getattr(report, "funder_address", None):
        print(f"funder={report.funder_address}")
    print("-" * 80)
    for item in report.checks:
        print(f"{item.name}: passed={item.passed} | {item.message}")
    print("=" * 80)


def build_argument_parser() -> argparse.ArgumentParser:
    """建立自動交易 CLI 參數。"""
    parser = argparse.ArgumentParser(description="Polymarket 自動交易主線")
    parser.add_argument("--mode", choices=["research", "live"], default="live", help="執行模式")
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help="只執行正式版啟動前檢查，不進入研究或 live loop",
    )
    parser.add_argument("--limit-events", type=int, default=200, help="Gamma events 掃描上限")
    parser.add_argument(
        "--timeframes",
        default="5m,15m,1h,4h,1d",
        help="允許週期，逗號分隔，例如 5m,15m,1h",
    )
    parser.add_argument(
        "--styles",
        default="up_down",
        help="允許市場風格，逗號分隔，例如 up_down,above_below",
    )
    parser.add_argument(
        "--assets",
        default="",
        help="允許資產，逗號分隔，例如 BTC,ETH,SOL；留空代表全部",
    )
    parser.add_argument("--min-edge", type=float, default=0.03, help="最小 edge 門檻")
    parser.add_argument("--min-confidence", type=float, default=0.30, help="最小信心分數")
    parser.add_argument("--max-spread", type=float, default=0.10, help="最大允許 spread 比率")
    parser.add_argument("--min-volume", type=float, default=0.0, help="最小市場成交量")
    parser.add_argument("--min-position-usdc", type=float, default=1.0, help="單筆最小下單金額（USDC）")
    parser.add_argument("--max-position-usdc", type=float, default=5.0, help="單筆最大下單金額（USDC）")
    parser.add_argument(
        "--min-marketable-buy-usdc",
        type=float,
        default=1.0,
        help="可立即成交 BUY 單的最小名義金額（USDC）",
    )
    parser.add_argument("--max-candidates", type=int, default=3, help="每輪最多執行幾筆")
    parser.add_argument("--tail-mode", default="adaptive", help="尾盤狀態機模式")
    parser.add_argument(
        "--allow-taker-fallback",
        action="store_true",
        help="長週期進入 attack 後允許 taker fallback",
    )
    parser.add_argument("--scan-interval", type=int, default=10, help="輪詢秒數")
    parser.add_argument(
        "--log-dir",
        default=os.getenv("POLY_LOG_DIR", "logs"),
        help="正式版檔案日誌輸出目錄",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("POLY_LOG_LEVEL", "INFO"),
        help="日誌等級",
    )
    parser.add_argument(
        "--enable-auto-claim",
        action="store_true",
        help="live 模式下啟用結算後自動領取",
    )
    parser.add_argument("--claim-interval", type=int, default=300, help="自動領取掃描秒數")
    parser.add_argument(
        "--claim-dry-run",
        action="store_true",
        help="只掃描可領取倉位，不提交領取交易",
    )
    parser.add_argument(
        "--db-path",
        default=f"research_signals_{datetime.now(timezone.utc).strftime('%Y%m%d')}.db",
        help="SQLite 輸出路徑",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="持續輪詢模式；未指定時只跑一輪",
    )
    return parser


def parse_csv_list(raw_value: str, uppercase: bool = False) -> Optional[List[str]]:
    """解析逗號分隔字串。"""
    if not raw_value.strip():
        return None
    values = [item.strip() for item in raw_value.split(",") if item.strip()]
    if uppercase:
        return [value.upper() for value in values]
    return values


def print_cycle_result(result) -> None:
    """輸出單次循環摘要。"""
    print("=" * 80)
    print(
        f"模式={result.mode} | 掃描事件={result.scanned_event_count} | 候選機會={result.candidate_count} | "
        f"入選={result.selected_count} | 已送單={result.executed_count} | 失敗={result.failed_count} | "
        f"領取送出={result.claim_submitted_count} | 領取失敗={result.claim_failed_count} | "
        f"領取試跑={result.claim_dry_run_count}"
    )
    print("=" * 80)

    if result.scan_result.opportunity_count == 0:
        print("本輪沒有找到符合門檻的機會。")
        return

    for index, candidate in enumerate(result.selected_candidates, start=1):
        opportunity = candidate.opportunity
        print(
            f"{index:02d}. [{opportunity.asset}/{opportunity.timeframe}] {opportunity.selected_side} "
            f"| edge={opportunity.selected_edge:.4f} | spread={opportunity.spread_pct} "
            f"| spot={opportunity.spot_price:.4f} | strike={opportunity.strike_price:.4f}"
        )
        print(
            f"    yes_ask={opportunity.yes_ask} | no_ask={opportunity.no_ask} | "
            f"confidence={opportunity.confidence_score:.4f} | volume={opportunity.volume:.2f}"
        )
        print(f"    {opportunity.question}")

    if result.executions:
        print("-" * 80)
        print("執行結果")
        print("-" * 80)
        for execution in result.executions:
            print(
                f"{execution.market_id} | {execution.side} | status={execution.status.value} | "
                f"amount={execution.size:.2f} | price={execution.price:.4f} | error={execution.error_message}"
            )

    if result.claim_results:
        print("-" * 80)
        print("領取結果")
        print("-" * 80)
        for claim in result.claim_results:
            print(
                f"{claim.market_id or '-'} | condition={claim.condition_id} | status={claim.status.value} | "
                f"tx={claim.transaction_hash or claim.transaction_id} | error={claim.error_message}"
            )


async def run() -> None:
    """執行自動交易主流程。"""
    parser = build_argument_parser()
    args = parser.parse_args()

    from polymarket_arbitrage.logging_setup import configure_application_logging

    log_dir = configure_application_logging(args.log_dir, args.log_level)
    lifecycle_logger.info(
        "程序啟動 | mode=%s | preflight_only=%s | db_path=%s | log_dir=%s",
        args.mode,
        args.preflight_only,
        args.db_path,
        log_dir,
    )

    from polymarket_arbitrage.auto_trading import AutoTradingPipeline
    from polymarket_arbitrage.research_pipeline import ResearchPipeline
    from polymarket_arbitrage.realtime_orderbook_cache import RealtimeOrderBookCache
    from polymarket_arbitrage.signal_logger import SignalLogger

    allowed_timeframes = parse_csv_list(args.timeframes)
    allowed_styles = parse_csv_list(args.styles)
    allowed_assets = parse_csv_list(args.assets, uppercase=True)

    signal_logger = SignalLogger(db_path=args.db_path)
    orderbook_cache = RealtimeOrderBookCache()
    research_pipeline = ResearchPipeline(
        signal_logger=signal_logger,
        min_edge_threshold=args.min_edge,
        min_confidence_score=args.min_confidence,
        max_spread_pct=args.max_spread,
        min_market_volume=args.min_volume,
        default_styles=allowed_styles,
        tail_mode=args.tail_mode,
        orderbook_cache=orderbook_cache,
    )

    live_executor = None
    settlement_claimer = None
    should_enable_auto_claim = args.enable_auto_claim or args.claim_dry_run
    should_prepare_live_stack = args.mode == "live" or args.preflight_only

    if should_prepare_live_stack:
        from polymarket_arbitrage.live_executor import LiveExecutor, LiveRiskConfig
        from polymarket_arbitrage.settlement_claimer import SettlementClaimer

        risk_config = LiveRiskConfig(
            min_edge_threshold=args.min_edge,
            min_confidence_score=args.min_confidence,
            max_spread_pct=args.max_spread,
            allow_taker_fallback=args.allow_taker_fallback,
            min_position_per_trade=args.min_position_usdc,
            max_position_per_trade=args.max_position_usdc,
            min_marketable_buy_notional=args.min_marketable_buy_usdc,
        )
        live_executor = LiveExecutor(
            signal_logger=signal_logger,
            risk_config=risk_config,
            orderbook_cache=orderbook_cache,
        )
        settlement_claimer = SettlementClaimer(db_path=args.db_path)
    elif should_enable_auto_claim:
        logger.warning("--enable-auto-claim / --claim-dry-run 只在 --mode live 生效，已忽略")

    try:
        if args.preflight_only:
            if live_executor is None:
                raise SystemExit("preflight-only 需要可用的 live 執行堆疊")
            report = live_executor.run_preflight(settlement_claimer=settlement_claimer)
            print_preflight_report(report)
            if not report.ready:
                lifecycle_logger.error("preflight-only 失敗，程序退出")
                raise SystemExit(1)
            lifecycle_logger.info("preflight-only 通過")
            return

        if args.mode == "live" and live_executor is not None:
            report = live_executor.run_preflight(
                settlement_claimer=settlement_claimer
            )
            print_preflight_report(report)
            if not report.ready:
                lifecycle_logger.error("live preflight 失敗，拒絕啟動")
                raise SystemExit(1)
            restored = live_executor.restore_runtime_state()
            preflight_logger.info(
                "live restore | pending=%s | exposures=%s | remote_open_orders=%s | remote_positions=%s",
                restored.pending_order_count,
                restored.directional_exposure_count,
                restored.remote_open_order_count,
                restored.remote_position_count,
            )

        pipeline = AutoTradingPipeline(
            research_pipeline=research_pipeline,
            live_executor=live_executor,
            settlement_claimer=settlement_claimer if should_enable_auto_claim else None,
            max_candidates_per_cycle=args.max_candidates,
        )

        if args.continuous:
            logger.info("啟動持續輪詢模式")
            await pipeline.run_forever(
                mode=args.mode,
                limit_events=args.limit_events,
                allowed_timeframes=allowed_timeframes,
                allowed_assets=allowed_assets,
                allowed_styles=allowed_styles,
                max_candidates=args.max_candidates,
                scan_interval_seconds=args.scan_interval,
                enable_auto_claim=should_enable_auto_claim,
                claim_interval_seconds=args.claim_interval,
                claim_dry_run=args.claim_dry_run,
            )
            lifecycle_logger.info("持續輪詢模式正常結束")
            return

        result = await pipeline.run_cycle(
            mode=args.mode,
            limit_events=args.limit_events,
            allowed_timeframes=allowed_timeframes,
            allowed_assets=allowed_assets,
            allowed_styles=allowed_styles,
            max_candidates=args.max_candidates,
            run_auto_claim=should_enable_auto_claim and args.mode == "live",
            claim_dry_run=args.claim_dry_run,
        )
        print_cycle_result(result)
        lifecycle_logger.info(
            "程序完成 | mode=%s | executed=%s | failed=%s | claim_submitted=%s",
            result.mode,
            result.executed_count,
            result.failed_count,
            result.claim_submitted_count,
        )
    finally:
        await orderbook_cache.close()
        await research_pipeline.close()


if __name__ == "__main__":
    asyncio.run(run())
