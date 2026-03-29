#!/usr/bin/env python3
"""
研究模式入口：掃描 Polymarket 加密市場並輸出 edge 機會。
"""

import argparse
import asyncio
from datetime import datetime, timezone
from pathlib import Path

try:
    from dotenv import load_dotenv

    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
except ModuleNotFoundError:
    pass

def build_argument_parser() -> argparse.ArgumentParser:
    """建立 CLI 參數。"""
    parser = argparse.ArgumentParser(description="Polymarket 研究模式掃描器")
    parser.add_argument("--limit-events", type=int, default=200, help="Gamma events 掃描上限")
    parser.add_argument("--min-edge", type=float, default=0.03, help="最小 edge 門檻")
    parser.add_argument("--min-confidence", type=float, default=0.30, help="最小模型信心")
    parser.add_argument("--max-spread", type=float, default=0.10, help="最大允許 spread 比率")
    parser.add_argument("--min-volume", type=float, default=0.0, help="最小市場成交量")
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
        help="允許資產，逗號分隔，例如 BTC,ETH；留空代表全部",
    )
    parser.add_argument("--anchor-source", default="settlement_oracle", help="開盤錨點來源策略")
    parser.add_argument("--tail-mode", default="adaptive", help="尾盤狀態機模式")
    parser.add_argument("--top", type=int, default=20, help="顯示前幾筆最佳機會")
    parser.add_argument(
        "--db-path",
        default=f"research_signals_{datetime.now(timezone.utc).strftime('%Y%m%d')}.db",
        help="SQLite 輸出路徑",
    )
    parser.add_argument("--export-json", help="機會清單 JSON 匯出路徑")
    return parser


def parse_csv_list(raw_value: str, uppercase: bool = False) -> list[str] | None:
    """解析逗號分隔的參數。"""
    if not raw_value.strip():
        return None
    values = [item.strip() for item in raw_value.split(",") if item.strip()]
    if uppercase:
        return [value.upper() for value in values]
    return values


async def run() -> None:
    """執行研究模式主流程。"""
    parser = build_argument_parser()
    args = parser.parse_args()

    from polymarket_arbitrage.research_pipeline import ResearchPipeline
    from polymarket_arbitrage.signal_logger import SignalLogger

    allowed_timeframes = parse_csv_list(args.timeframes)
    allowed_styles = parse_csv_list(args.styles)
    allowed_assets = parse_csv_list(args.assets, uppercase=True)

    signal_logger = SignalLogger(db_path=args.db_path)
    pipeline = ResearchPipeline(
        signal_logger=signal_logger,
        min_edge_threshold=args.min_edge,
        min_confidence_score=args.min_confidence,
        max_spread_pct=args.max_spread,
        min_market_volume=args.min_volume,
        default_styles=allowed_styles,
        anchor_source=args.anchor_source,
        tail_mode=args.tail_mode,
    )

    try:
        result = await pipeline.run(
            limit_events=args.limit_events,
            allowed_timeframes=allowed_timeframes,
            allowed_assets=allowed_assets,
            allowed_styles=allowed_styles,
        )

        print("=" * 72)
        print("Polymarket 研究模式掃描結果")
        print("=" * 72)
        print(f"掃描事件數: {result.scanned_event_count}")
        print(f"發現市場數: {result.discovered_market_count}")
        print(f"可解析市場數: {result.parsed_market_count}")
        print(f"Pricing Verified 數: {result.pricing_verified_count}")
        print(f"完成分析數: {result.analyzed_market_count}")
        print(f"符合門檻機會數: {result.opportunity_count}")
        print(f"SQLite: {args.db_path}")
        print("=" * 72)

        if result.opportunity_count == 0:
            print("本次掃描沒有找到符合門檻的機會。")
        else:
            for index, opportunity in enumerate(
                result.opportunities[: args.top], start=1
            ):
                print(
                    f"{index:02d}. [{opportunity.asset}/{opportunity.timeframe}] {opportunity.selected_side} | "
                    f"edge={opportunity.selected_edge:.4f} | "
                    f"spread={opportunity.spread_pct} | "
                    f"spot={opportunity.spot_price:.4f} | "
                    f"strike={opportunity.strike_price:.4f}"
                )
                print(
                    f"    yes_bid={opportunity.yes_bid} yes_ask={opportunity.yes_ask} | "
                    f"no_bid={opportunity.no_bid} no_ask={opportunity.no_ask}"
                )
                print(
                    f"    style={opportunity.market_style} | fair_yes={opportunity.fair_yes:.4f} fair_no={opportunity.fair_no:.4f} | "
                    f"confidence={opportunity.confidence_score:.4f} | "
                    f"ttl={opportunity.time_to_expiry_sec:.0f}s | window={opportunity.window_state} | lead_z={opportunity.lead_z}"
                )
                if opportunity.anchor_price is not None:
                    print(
                        f"    anchor={opportunity.anchor_price:.4f} | anchor_ts={opportunity.anchor_timestamp.isoformat() if opportunity.anchor_timestamp else 'n/a'}"
                    )
                print(f"    {opportunity.question}")

        if args.export_json:
            await pipeline.export_opportunities_json(
                args.export_json, result.opportunities
            )
            print(f"JSON 匯出: {args.export_json}")
    finally:
        await pipeline.close()


if __name__ == "__main__":
    asyncio.run(run())
