"""`run_auto_trading.py` 入口測試。"""

from __future__ import annotations

from run_auto_trading import build_argument_parser


def test_build_argument_parser_supports_preflight_only() -> None:
    """正式版應提供 preflight-only 啟動前檢查模式。"""
    parser = build_argument_parser()

    args = parser.parse_args(["--preflight-only", "--log-dir", "logs/runtime", "--log-level", "DEBUG"])

    assert args.preflight_only is True
    assert args.log_dir == "logs/runtime"
    assert args.log_level == "DEBUG"


def test_build_argument_parser_supports_runtime_risk_overrides() -> None:
    """正式版應支持常駐風控參數覆寫。"""
    parser = build_argument_parser()

    args = parser.parse_args(
        [
            "--min-position-usdc", "1.5",
            "--max-position-usdc", "4.0",
            "--min-marketable-buy-usdc", "1.0",
        ]
    )

    assert args.min_position_usdc == 1.5
    assert args.max_position_usdc == 4.0
    assert args.min_marketable_buy_usdc == 1.0


def test_build_argument_parser_defaults_scan_interval_to_ten_seconds() -> None:
    """短週期常駐預設輪詢應固定為 10 秒。"""
    parser = build_argument_parser()

    args = parser.parse_args([])

    assert args.scan_interval == 10
