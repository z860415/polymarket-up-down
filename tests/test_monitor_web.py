from pathlib import Path

from polymarket_arbitrage.monitor_web import (
    MonitorConfig,
    _build_alerts,
    _build_buy_block_breakdown,
    _build_parameter_snapshot,
    _build_status_payload,
    _latest_reject_summary,
    _parse_cycle_line,
    _recent_reject_hotlist,
    _render_html,
)


def test_parse_cycle_line_extracts_metrics():
    line = (
        "2026-03-29 03:28:16,304 | polymarket.lifecycle | INFO | "
        "循環完成 | mode=live | scanned=500 | candidates=0 | selected=0 | "
        "executed=0 | failed=0 | claim_submitted=0 | claim_failed=0 | claim_dry_run=0"
    )
    payload = _parse_cycle_line(line)
    assert payload is not None
    assert payload["mode"] == "live"
    assert payload["scanned"] == 500
    assert payload["candidates"] == 0
    assert payload["executed"] == 0


def test_latest_reject_summary_reads_summary_and_samples(tmp_path: Path):
    log_dir = tmp_path
    candidate_log = log_dir / "candidate.log"
    candidate_log.write_text(
        "\n".join(
            [
                "2026-03-29 | polymarket.candidate | INFO | 候選拒絕摘要 | {'window_not_open': 3, 'edge_too_low': 2}",
                "2026-03-29 | polymarket.candidate | INFO | 候選拒絕樣本 | {'reason': 'window_not_open', 'asset': 'BTC', 'style': 'UP_DOWN', 'timeframe': '1h', 'question': 'BTC up?'}",
            ]
        ),
        encoding="utf-8",
    )

    payload = _latest_reject_summary(log_dir)
    assert payload["summary"]["window_not_open"] == 3
    assert payload["summary"]["edge_too_low"] == 2
    assert payload["samples"][0]["reason"] == "window_not_open"


def test_build_status_payload_reverses_lifecycle_log(tmp_path: Path):
    log_dir = tmp_path
    (log_dir / "lifecycle.log").write_text(
        "\n".join(
            [
                "2026-03-29 10:00:00,000 | polymarket.lifecycle | INFO | 第一筆",
                "2026-03-29 10:00:01,000 | polymarket.lifecycle | INFO | 第二筆",
            ]
        ),
        encoding="utf-8",
    )
    payload = _build_status_payload(MonitorConfig(db_path=tmp_path / "missing.db", log_dir=log_dir))
    assert payload["logs"]["lifecycle"][0].endswith("第二筆")
    assert payload["logs"]["lifecycle"][1].endswith("第一筆")


def test_render_html_uses_chinese_labels(tmp_path: Path):
    html = _render_html(MonitorConfig(db_path=tmp_path / "signals.db", log_dir=tmp_path))
    assert "生命週期日誌" in html
    assert "候選日誌" in html
    assert "訂單日誌" in html
    assert "錯誤日誌" in html
    assert "Lifecycle Log" not in html


def test_render_html_includes_refresh_countdown(tmp_path: Path):
    html = _render_html(MonitorConfig(db_path=tmp_path / "signals.db", log_dir=tmp_path, refresh_seconds=5))
    assert 'id="refresh-countdown"' in html
    assert "更新刷新倒數" in html
    assert "排程下一次刷新" in html


def test_build_buy_block_breakdown_splits_four_layers():
    breakdown = _build_buy_block_breakdown(
        latest_cycle={"candidates": 4, "selected": 1, "executed": 0, "failed": 1},
        reject_summary={
            "style_unknown": 8,
            "edge_too_low": 2,
        },
    )
    items = {item["key"]: item for item in breakdown["items"]}
    assert items["market_filter"]["count"] == 8
    assert items["research_reject"]["count"] == 2
    assert items["risk_reject"]["count"] == 3
    assert items["execution_reject"]["count"] == 1


def test_build_parameter_snapshot_extracts_live_flags():
    snapshot = _build_parameter_snapshot(
        {
            "processes": [
                {
                    "command": "python3 run_auto_trading.py --mode live --limit-events 30 --scan-interval 10 --assets BTC,ETH --styles above_below --max-candidates 1",
                }
            ]
        }
    )
    assert snapshot["mode"] == "live"
    assert snapshot["limit_events"] == "30"
    assert snapshot["scan_interval"] == "10"
    assert snapshot["assets"] == "BTC,ETH"
    assert snapshot["styles"] == "above_below"


def test_recent_reject_hotlist_aggregates_same_market(tmp_path: Path):
    log_dir = tmp_path
    (log_dir / "candidate.log").write_text(
        "\n".join(
            [
                "2026-03-29 10:00:00,000 | polymarket.candidate | INFO | 候選拒絕樣本 | {'reason': 'window_not_open', 'asset': 'BTC', 'style': 'UP_DOWN', 'timeframe': '1h', 'question': 'BTC 1h up?', 'window_state': 'observe', 'window_label': '已開盤未進尾盤', 'tau_seconds': 420.0, 'seconds_to_armed': 120.0}",
                "2026-03-29 10:01:00,000 | polymarket.candidate | INFO | 候選拒絕樣本 | {'reason': 'window_not_open', 'asset': 'BTC', 'style': 'UP_DOWN', 'timeframe': '1h', 'question': 'BTC 1h up?', 'window_state': 'observe', 'window_label': '已開盤未進尾盤', 'tau_seconds': 360.0, 'seconds_to_armed': 60.0}",
            ]
        ),
        encoding="utf-8",
    )
    hotlist = _recent_reject_hotlist(log_dir)
    assert hotlist[0]["count"] == 2
    assert hotlist[0]["reason"] == "window_not_open"
    assert hotlist[0]["window_state"] == "observe"
    assert hotlist[0]["window_label"] == "已開盤未進尾盤"
    assert hotlist[0]["seconds_to_armed"] == 60.0


def test_build_alerts_prioritizes_zero_candidates_and_missing_observations(tmp_path: Path):
    log_dir = tmp_path
    (log_dir / "error.log").write_text("", encoding="utf-8")
    (log_dir / "lifecycle.log").write_text(
        "\n".join(
            [
                "2026-03-29 10:00:00,000 | polymarket.lifecycle | INFO | 循環完成 | mode=live | scanned=300 | candidates=0 | selected=0 | executed=0 | failed=0 | claim_submitted=0 | claim_failed=0 | claim_dry_run=0",
                "2026-03-29 10:00:10,000 | polymarket.lifecycle | INFO | 循環完成 | mode=live | scanned=300 | candidates=0 | selected=0 | executed=0 | failed=0 | claim_submitted=0 | claim_failed=0 | claim_dry_run=0",
                "2026-03-29 10:00:20,000 | polymarket.lifecycle | INFO | 循環完成 | mode=live | scanned=300 | candidates=0 | selected=0 | executed=0 | failed=0 | claim_submitted=0 | claim_failed=0 | claim_dry_run=0",
            ]
        ),
        encoding="utf-8",
    )

    alerts = _build_alerts(
        config=MonitorConfig(db_path=tmp_path / "missing.db", log_dir=log_dir),
        process_info={"running": True, "processes": []},
        recent_cycles=[
            {"candidates": 0},
            {"candidates": 0},
            {"candidates": 0},
        ],
    )

    assert alerts[0]["title"] == "連續零候選"
    assert alerts[0]["detail"] == "最近已連續 3 輪 candidates=0，建議先看市場過濾與研究拒絕主因。"
    assert "候選拒絕摘要" in alerts[0]["action"]
    assert alerts[1]["title"] == "缺少觀測資料"
    assert alerts[1]["severity"] == "medium"
    assert "目前 SQLite 沒有新的 observations" in alerts[1]["detail"]
    assert "研究層" in alerts[1]["action"]


def test_render_html_includes_alert_action_text(tmp_path: Path):
    html = _render_html(MonitorConfig(db_path=tmp_path / "signals.db", log_dir=tmp_path))
    assert "優先處理：" in html
    assert "已開盤未進尾盤" in html
