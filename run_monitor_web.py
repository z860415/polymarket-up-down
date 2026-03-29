#!/usr/bin/env python3
"""
本地只讀監控 Web 入口。
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from polymarket_arbitrage.monitor_web import MonitorConfig, serve_monitor


def build_argument_parser() -> argparse.ArgumentParser:
    """建立監控 Web CLI 參數。"""
    parser = argparse.ArgumentParser(description="Polymarket 本地監控 Web")
    parser.add_argument("--host", default="127.0.0.1", help="綁定主機")
    parser.add_argument("--port", type=int, default=8787, help="綁定埠號")
    parser.add_argument(
        "--db-path",
        default=f"research_signals_{datetime.now(timezone.utc).strftime('%Y%m%d')}.db",
        help="SQLite 路徑",
    )
    parser.add_argument("--log-dir", default="logs", help="日誌目錄")
    parser.add_argument("--refresh-seconds", type=int, default=5, help="前端自動刷新秒數")
    return parser


def main() -> None:
    """啟動監控頁。"""
    args = build_argument_parser().parse_args()
    config = MonitorConfig(
        db_path=Path(args.db_path).expanduser().resolve(),
        log_dir=Path(args.log_dir).expanduser().resolve(),
        refresh_seconds=args.refresh_seconds,
    )
    serve_monitor(args.host, args.port, config)


if __name__ == "__main__":
    main()
