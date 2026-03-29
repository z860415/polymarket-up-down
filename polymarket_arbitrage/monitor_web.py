"""
本地只讀監控 Web。

用途：
- 顯示交易主程序是否存活
- 顯示最近循環摘要
- 顯示未買入原因彙總與樣本
- 顯示關鍵日誌尾部
"""

from __future__ import annotations

import ast
import json
import re
import shlex
import sqlite3
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse


@dataclass(frozen=True)
class MonitorConfig:
    """監控服務設定。"""

    db_path: Path
    log_dir: Path
    refresh_seconds: int = 5


_CYCLE_PATTERN = re.compile(
    r"循環完成 \| mode=(?P<mode>\w+) \| scanned=(?P<scanned>\d+) \| candidates=(?P<candidates>\d+) "
    r"\| selected=(?P<selected>\d+) \| executed=(?P<executed>\d+) \| failed=(?P<failed>\d+) "
    r"\| claim_submitted=(?P<claim_submitted>\d+) \| claim_failed=(?P<claim_failed>\d+) "
    r"\| claim_dry_run=(?P<claim_dry_run>\d+)"
)
_LOG_TIMESTAMP_PATTERN = re.compile(r"^(?P<timestamp>\d{{4}}-\d{{2}}-\d{{2}} \d{{2}}:\d{{2}}:\d{{2}},\d{{3}})")

_MARKET_FILTER_REASONS = {
    "style_unknown",
    "crypto_unsupported",
    "non_crypto_unrelated",
    "missing_end_date",
    "timeframe_missing",
}
_RESEARCH_REJECT_REASONS = {
    "style_not_supported",
    "missing_token_ids",
    "orderbook_unavailable",
    "ask_quote_missing",
    "spread_too_wide",
    "volume_too_low",
    "spot_price_unavailable",
    "fair_probability_failed",
    "edge_too_low",
    "confidence_too_low",
    "anchor_source_unsupported",
    "anchor_unavailable",
    "sigma_window_unsupported",
    "window_not_open",
    "lead_z_too_low",
    "clob_reject",
    "orderbook_reject",
    "status_reject",
}
_RISK_REJECT_REASONS = {
    "insufficient_balance",
    "position_limit",
    "exposure_limit",
    "marketable_buy_below_min_notional",
    "time_to_expiry_too_short",
    "duplicate_directional_exposure",
    "min_notional_not_met",
    "risk_reject",
}
_EXECUTION_REJECT_REASONS = {
    "order_rejected",
    "order_submit_failed",
    "execution_failed",
    "api_error",
    "order_status_failed",
}
_BLOCK_LAYER_LABELS = {
    "market_filter": "市場過濾",
    "research_reject": "研究拒絕",
    "risk_reject": "風控拒絕",
    "execution_reject": "執行拒絕",
}
_ALERT_SEVERITY_RANK = {
    "high": 3,
    "medium": 2,
    "low": 1,
    "ok": 0,
}
_ALERT_TITLE_PRIORITY = {
    "主程序未執行": 100,
    "近期錯誤偏高": 90,
    "連續零候選": 80,
    "缺少觀測資料": 70,
    "觀測資料停滯": 60,
    "目前未發現明顯異常": 0,
}


def _parse_log_timestamp(line: str) -> Optional[datetime]:
    """解析日誌行開頭時間。"""
    match = _LOG_TIMESTAMP_PATTERN.match(line)
    if match is None:
        return None
    try:
        return datetime.strptime(match.group("timestamp"), "%Y-%m-%d %H:%M:%S,%f")
    except ValueError:
        return None


def _parse_generic_timestamp(value: Optional[str]) -> Optional[datetime]:
    """解析通用時間字串。"""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _tail_lines(path: Path, limit: int = 40) -> List[str]:
    """讀取檔案尾部數行。"""
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8", errors="ignore").splitlines()[-limit:]


def _tail_lines_latest_first(path: Path, limit: int = 40) -> List[str]:
    """讀取檔案尾部數行，並以最新紀錄在最上方排序。"""
    return list(reversed(_tail_lines(path, limit=limit)))


def _find_bot_process() -> Dict[str, Any]:
    """查詢當前交易主進程。"""
    result = subprocess.run(
        ["ps", "-ax", "-o", "pid=,etime=,command="],
        capture_output=True,
        text=True,
        check=False,
    )
    processes: List[Dict[str, str]] = []
    for line in result.stdout.splitlines():
        if "run_auto_trading.py" not in line:
            continue
        if "run_monitor_web.py" in line:
            continue
        stripped = line.strip()
        if not stripped:
            continue
        parts = stripped.split(None, 2)
        if len(parts) < 3:
            continue
        processes.append(
            {
                "pid": parts[0],
                "etime": parts[1],
                "command": parts[2],
            }
        )
    return {
        "running": bool(processes),
        "count": len(processes),
        "processes": processes,
    }


def _parse_cycle_line(line: str) -> Optional[Dict[str, Any]]:
    """解析循環完成日誌。"""
    match = _CYCLE_PATTERN.search(line)
    if match is None:
        return None
    payload = match.groupdict()
    for key, value in list(payload.items()):
        if key != "mode":
            payload[key] = int(value)
    payload["raw"] = line
    return payload


def _latest_cycle(log_dir: Path) -> Optional[Dict[str, Any]]:
    """抓取最近一輪循環摘要。"""
    for line in reversed(_tail_lines(log_dir / "lifecycle.log", limit=200)):
        payload = _parse_cycle_line(line)
        if payload is not None:
            timestamp = _parse_log_timestamp(line)
            payload["timestamp"] = timestamp.isoformat() if timestamp is not None else None
            return payload
    return None


def _recent_cycles(log_dir: Path, minutes: int = 30, limit: int = 240) -> List[Dict[str, Any]]:
    """讀取最近一段時間的循環摘要。"""
    cutoff = datetime.now() - timedelta(minutes=minutes)
    cycles: List[Dict[str, Any]] = []
    for line in _tail_lines(log_dir / "lifecycle.log", limit=4000):
        payload = _parse_cycle_line(line)
        if payload is None:
            continue
        timestamp = _parse_log_timestamp(line)
        if timestamp is not None and timestamp < cutoff:
            continue
        payload["timestamp"] = timestamp.isoformat() if timestamp is not None else None
        cycles.append(payload)
    return cycles[-limit:]


def _latest_reject_summary(log_dir: Path) -> Dict[str, Any]:
    """抓取最近一輪未買入原因摘要。"""
    summary: Dict[str, int] = {}
    samples: List[Dict[str, Any]] = []
    lines = _tail_lines(log_dir / "candidate.log", limit=200)
    for line in reversed(lines):
        if "候選拒絕摘要 | " not in line:
            continue
        raw_payload = line.split("候選拒絕摘要 | ", 1)[1].strip()
        try:
            summary = ast.literal_eval(raw_payload)
        except (SyntaxError, ValueError):
            summary = {"parse_error": 1}
        break

    for line in reversed(lines):
        if "候選拒絕樣本 | " not in line:
            continue
        raw_payload = line.split("候選拒絕樣本 | ", 1)[1].strip()
        try:
            samples.append(ast.literal_eval(raw_payload))
        except (SyntaxError, ValueError):
            continue
        if len(samples) >= 10:
            break
    samples.reverse()
    return {
        "summary": summary,
        "samples": samples,
    }


def _recent_reject_hotlist(log_dir: Path, limit: int = 10) -> List[Dict[str, Any]]:
    """聚合近期最常出現的拒絕樣本。"""
    aggregated: Dict[tuple, Dict[str, Any]] = {}
    extra_fields = (
        "window_state",
        "window_label",
        "tau_seconds",
        "seconds_to_armed",
        "seconds_to_attack",
    )
    for line in _tail_lines(log_dir / "candidate.log", limit=4000):
        if "候選拒絕樣本 | " not in line:
            continue
        raw_payload = line.split("候選拒絕樣本 | ", 1)[1].strip()
        try:
            sample = ast.literal_eval(raw_payload)
        except (SyntaxError, ValueError):
            continue
        key = (
            sample.get("reason"),
            sample.get("question"),
            sample.get("asset"),
            sample.get("style"),
            sample.get("timeframe"),
        )
        timestamp = _parse_log_timestamp(line)
        if key not in aggregated:
            aggregated[key] = {
                "reason": sample.get("reason"),
                "question": sample.get("question"),
                "asset": sample.get("asset"),
                "style": sample.get("style"),
                "timeframe": sample.get("timeframe"),
                "count": 0,
                "last_seen": timestamp.isoformat() if timestamp is not None else None,
            }
            for field in extra_fields:
                aggregated[key][field] = sample.get(field)
        aggregated[key]["count"] += 1
        if timestamp is not None:
            aggregated[key]["last_seen"] = timestamp.isoformat()
        for field in extra_fields:
            value = sample.get(field)
            if value is not None:
                aggregated[key][field] = value
    return sorted(
        aggregated.values(),
        key=lambda item: (-item["count"], item.get("last_seen") or ""),
        reverse=False,
    )[:limit]


def _latest_observations(db_path: Path, limit: int = 10) -> List[Dict[str, Any]]:
    """讀取最近觀測樣本。"""
    if not db_path.exists():
        return []
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                timestamp,
                market_id,
                asset,
                market_style,
                p_yes,
                p_no,
                model_confidence_score,
                yes_ask,
                no_ask,
                spot_price,
                strike_price,
                time_to_expiry_sec,
                net_edge_selected,
                window_state
            FROM observations
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _latest_observation_timestamp(db_path: Path) -> Optional[datetime]:
    """讀取最新 observation 時間。"""
    if not db_path.exists():
        return None
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute("SELECT MAX(timestamp) FROM observations").fetchone()
        if row is None or row[0] is None:
            return None
        return _parse_generic_timestamp(row[0])
    finally:
        conn.close()


def _classify_reject_reason(reason: str) -> str:
    """將拒絕原因歸類到四層阻塞路徑。"""
    if reason in _MARKET_FILTER_REASONS:
        return "market_filter"
    if reason in _RISK_REJECT_REASONS:
        return "risk_reject"
    if reason in _EXECUTION_REJECT_REASONS:
        return "execution_reject"
    return "research_reject"


def _build_buy_block_breakdown(
    latest_cycle: Optional[Dict[str, Any]],
    reject_summary: Dict[str, int],
) -> Dict[str, Any]:
    """建立本輪不買四層占比。"""
    counts = {
        "market_filter": 0,
        "research_reject": 0,
        "risk_reject": 0,
        "execution_reject": 0,
    }
    for reason, value in reject_summary.items():
        layer = _classify_reject_reason(reason)
        counts[layer] += int(value)

    if latest_cycle is not None:
        candidate_count = int(latest_cycle.get("candidates", 0))
        selected_count = int(latest_cycle.get("selected", 0))
        executed_count = int(latest_cycle.get("executed", 0))
        failed_count = int(latest_cycle.get("failed", 0))
        counts["risk_reject"] += max(candidate_count - selected_count, 0)
        counts["execution_reject"] += max(failed_count, max(selected_count - executed_count, 0))

    total = sum(counts.values())
    items = []
    for key in ("market_filter", "research_reject", "risk_reject", "execution_reject"):
        count = counts[key]
        items.append(
            {
                "key": key,
                "label": _BLOCK_LAYER_LABELS[key],
                "count": count,
                "pct": (count / total) if total > 0 else 0.0,
            }
        )

    primary = max(items, key=lambda item: item["count"]) if items else None
    return {
        "total": total,
        "items": items,
        "primary": primary,
    }


def _parse_command_options(command: str) -> Dict[str, Any]:
    """解析命令列參數。"""
    try:
        tokens = shlex.split(command)
    except ValueError:
        return {}

    options: Dict[str, Any] = {}
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if not token.startswith("--"):
            index += 1
            continue
        key = token[2:].replace("-", "_")
        next_index = index + 1
        if next_index < len(tokens) and not tokens[next_index].startswith("--"):
            options[key] = tokens[next_index]
            index += 2
            continue
        options[key] = True
        index += 1
    return options


def _build_parameter_snapshot(process_info: Dict[str, Any]) -> Dict[str, Any]:
    """建立目前 live 參數快照。"""
    processes = process_info.get("processes") or []
    if not processes:
        return {}
    command = processes[0].get("command", "")
    options = _parse_command_options(command)
    return {
        "mode": options.get("mode"),
        "limit_events": options.get("limit_events"),
        "scan_interval": options.get("scan_interval"),
        "assets": options.get("assets"),
        "styles": options.get("styles"),
        "max_candidates": options.get("max_candidates"),
        "db_path": options.get("db_path"),
        "log_dir": options.get("log_dir"),
        "raw_command": command,
    }


def _build_alerts(
    config: MonitorConfig,
    process_info: Dict[str, Any],
    recent_cycles: List[Dict[str, Any]],
) -> List[Dict[str, str]]:
    """建立可疑異常提醒。"""
    alerts: List[Dict[str, str]] = []
    if not process_info.get("running"):
        alerts.append(
            {
                "severity": "high",
                "title": "主程序未執行",
                "detail": "目前沒有偵測到 run_auto_trading.py，監控頁資料將停留在舊結果。",
                "action": "優先檢查常駐程序是否退出、systemd 或啟動命令是否失效。",
            }
        )

    zero_candidate_streak = 0
    for cycle in reversed(recent_cycles):
        if int(cycle.get("candidates", 0)) == 0:
            zero_candidate_streak += 1
            continue
        break
    if zero_candidate_streak >= 3:
        alerts.append(
            {
                "severity": "medium",
                "title": "連續零候選",
                "detail": f"最近已連續 {zero_candidate_streak} 輪 candidates=0，建議先看市場過濾與研究拒絕主因。",
                "action": "優先檢查候選拒絕摘要、live 窗口過濾結果與當前掃描樣本是否都停在 observe。",
            }
        )

    recent_error_count = 0
    cutoff = datetime.now() - timedelta(minutes=10)
    for line in _tail_lines_latest_first(config.log_dir / "error.log", limit=100):
        timestamp = _parse_log_timestamp(line)
        if timestamp is None or timestamp < cutoff:
            continue
        recent_error_count += 1
    if recent_error_count >= 3:
        alerts.append(
            {
                "severity": "high",
                "title": "近期錯誤偏高",
                "detail": f"最近 10 分鐘內共有 {recent_error_count} 筆錯誤日誌，需檢查資料源、preflight 或執行層是否有持續異常。",
                "action": "優先查看 error.log 最新錯誤，確認是否為可重試外部故障或邏輯性失敗。",
            }
        )

    latest_observation_at = _latest_observation_timestamp(config.db_path)
    if latest_observation_at is None:
        alerts.append(
            {
                "severity": "medium",
                "title": "缺少觀測資料",
                "detail": "目前 SQLite 沒有新的 observations，可檢查研究層是否長時間沒有落庫。",
                "action": "優先檢查研究層是否仍有成功寫入 observation，並確認 SQLite 路徑與權限正確。",
            }
        )
    else:
        observation_age_minutes = (datetime.now(timezone.utc) - latest_observation_at).total_seconds() / 60
        if observation_age_minutes >= 30:
            alerts.append(
                {
                    "severity": "medium",
                    "title": "觀測資料停滯",
                    "detail": f"最新 observation 已超過 {int(observation_age_minutes)} 分鐘未更新，建議檢查研究落庫流程。",
                    "action": "優先檢查研究層是否被前置過濾完全攔住，或資料庫寫入是否失敗。",
                }
            )

    if not alerts:
        alerts.append(
            {
                "severity": "ok",
                "title": "目前未發現明顯異常",
                "detail": "主程序存活、近期錯誤量可接受，監控資料仍在持續更新。",
                "action": "目前無需優先處理，可持續觀察候選、拒絕摘要與成交變化。",
            }
        )
    alerts.sort(
        key=lambda alert: (
            -_ALERT_SEVERITY_RANK.get(alert.get("severity", "low"), 0),
            -_ALERT_TITLE_PRIORITY.get(alert.get("title", ""), 0),
            alert.get("title", ""),
        )
    )
    return alerts


def _build_status_payload(config: MonitorConfig) -> Dict[str, Any]:
    """建立狀態 API 回傳。"""
    log_dir = config.log_dir
    process_info = _find_bot_process()
    latest_cycle = _latest_cycle(log_dir)
    latest_reasons = _latest_reject_summary(log_dir)
    recent_cycles = _recent_cycles(log_dir, minutes=30)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "process": process_info,
        "latest_cycle": latest_cycle,
        "latest_reasons": latest_reasons,
        "trend": {
            "window_minutes": 30,
            "points": recent_cycles,
        },
        "buy_block_breakdown": _build_buy_block_breakdown(latest_cycle, latest_reasons["summary"]),
        "hot_reject_samples": _recent_reject_hotlist(log_dir, limit=10),
        "parameter_snapshot": _build_parameter_snapshot(process_info),
        "alerts": _build_alerts(config, process_info, recent_cycles),
        "recent_observations": _latest_observations(config.db_path),
        "logs": {
            "lifecycle": _tail_lines_latest_first(log_dir / "lifecycle.log", limit=30),
            "candidate": _tail_lines_latest_first(log_dir / "candidate.log", limit=30),
            "order": _tail_lines_latest_first(log_dir / "order.log", limit=30),
            "error": _tail_lines_latest_first(log_dir / "error.log", limit=30),
        },
    }


def _build_reasons_payload(config: MonitorConfig) -> Dict[str, Any]:
    """建立未買入原因 API 回傳。"""
    reasons = _latest_reject_summary(config.log_dir)
    latest_cycle = _latest_cycle(config.log_dir)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "latest_cycle": latest_cycle,
        "reject_summary": reasons["summary"],
        "reject_samples": reasons["samples"],
    }


def _render_html(config: MonitorConfig) -> str:
    """渲染監控首頁 HTML。"""
    refresh_ms = max(config.refresh_seconds, 1) * 1000
    return f"""<!doctype html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Polymarket 監控面板</title>
  <style>
    :root {{
      --bg: #f3efe7;
      --ink: #172033;
      --muted: #58657d;
      --panel: rgba(255,255,255,.78);
      --panel-strong: rgba(255,255,255,.92);
      --line: rgba(23,32,51,.10);
      --ok: #0f9d58;
      --warn: #c57b12;
      --bad: #c2412d;
      --accent: #0f766e;
      --accent-soft: rgba(15,118,110,.10);
      --sun: #e0a458;
      --sun-soft: rgba(224,164,88,.14);
      --shadow: 0 20px 60px rgba(23,32,51,.12);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      font-family: "Avenir Next", "SF Pro Display", "PingFang TC", "Noto Sans TC", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(224,164,88,.22), transparent 22%),
        radial-gradient(circle at top right, rgba(15,118,110,.18), transparent 18%),
        linear-gradient(180deg, #fbf8f2 0%, #f3efe7 48%, #ece6db 100%);
    }}
    .wrap {{ max-width: 1400px; margin: 0 auto; padding: 28px 22px 40px; }}
    h1, h2, h3, p {{ margin: 0; }}
    .grid {{ display: grid; grid-template-columns: repeat(12, 1fr); gap: 16px; }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 22px;
      padding: 18px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(16px);
    }}
    .hero {{
      background:
        linear-gradient(135deg, rgba(255,255,255,.96), rgba(250,246,238,.82)),
        linear-gradient(135deg, rgba(15,118,110,.08), rgba(224,164,88,.05));
      overflow: hidden;
      position: relative;
    }}
    .hero::after {{
      content: "";
      position: absolute;
      inset: auto -40px -40px auto;
      width: 180px;
      height: 180px;
      border-radius: 999px;
      background: radial-gradient(circle, rgba(224,164,88,.24), transparent 66%);
    }}
    .hero-top {{
      display: flex;
      gap: 18px;
      justify-content: space-between;
      align-items: flex-start;
      position: relative;
      z-index: 1;
    }}
    .hero-title {{
      font-size: clamp(30px, 4vw, 42px);
      font-weight: 800;
      letter-spacing: -.03em;
      margin-bottom: 10px;
    }}
    .hero-subtitle {{
      max-width: 760px;
      font-size: 15px;
      line-height: 1.75;
      color: var(--muted);
    }}
    .hero-meta {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }}
    .meta-pill, .reason-pill {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,.88);
      font-size: 13px;
      color: var(--muted);
    }}
    .meta-pill strong {{
      color: var(--ink);
      font-weight: 700;
    }}
    .section-kicker {{
      display: inline-block;
      margin-bottom: 10px;
      padding: 5px 10px;
      border-radius: 999px;
      background: var(--sun-soft);
      color: #8b5c1b;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: .08em;
    }}
    .card-title {{
      font-size: 22px;
      font-weight: 750;
      letter-spacing: -.02em;
      margin-bottom: 6px;
    }}
    .card-desc {{
      color: var(--muted);
      font-size: 13px;
      line-height: 1.6;
      margin-bottom: 16px;
    }}
    .span-3 {{ grid-column: span 3; }}
    .span-4 {{ grid-column: span 4; }}
    .span-6 {{ grid-column: span 6; }}
    .span-8 {{ grid-column: span 8; }}
    .span-12 {{ grid-column: span 12; }}
    .status-dot {{
      width: 10px;
      height: 10px;
      border-radius: 999px;
      background: currentColor;
      box-shadow: 0 0 0 6px currentColor;
      opacity: .16;
    }}
    .status-line {{
      display: flex;
      align-items: center;
      gap: 10px;
      font-size: 14px;
      font-weight: 700;
      margin-bottom: 16px;
    }}
    .status-line.ok {{ color: var(--ok); }}
    .status-line.bad {{ color: var(--bad); }}
    .metric-label {{
      font-size: 12px;
      color: var(--muted);
      letter-spacing: .08em;
      text-transform: uppercase;
      margin-bottom: 14px;
    }}
    .metric-value {{
      font-size: clamp(34px, 3vw, 44px);
      line-height: 1;
      font-weight: 800;
      letter-spacing: -.04em;
      margin-bottom: 10px;
    }}
    .metric-foot {{
      color: var(--muted);
      font-size: 13px;
      line-height: 1.6;
    }}
    .decision-card {{
      background:
        linear-gradient(135deg, rgba(255,255,255,.94), rgba(242,248,247,.88)),
        linear-gradient(135deg, rgba(15,118,110,.08), transparent);
    }}
    .decision-title {{
      font-size: 28px;
      line-height: 1.2;
      font-weight: 800;
      letter-spacing: -.03em;
      margin-bottom: 12px;
    }}
    .decision-body {{
      color: var(--muted);
      line-height: 1.85;
      font-size: 15px;
      margin-bottom: 16px;
    }}
    .reason-cloud {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 14px;
    }}
    .reason-pill {{
      background: var(--panel-strong);
      color: var(--ink);
      font-weight: 600;
    }}
    .reason-pill b {{
      font-size: 12px;
      color: var(--accent);
    }}
    .stack-list {{
      display: grid;
      gap: 12px;
    }}
    .stack-row {{
      display: grid;
      grid-template-columns: 110px 1fr 64px;
      gap: 12px;
      align-items: center;
    }}
    .stack-label {{
      font-size: 13px;
      font-weight: 700;
      color: var(--ink);
    }}
    .stack-track {{
      position: relative;
      height: 12px;
      border-radius: 999px;
      background: rgba(23,32,51,.08);
      overflow: hidden;
    }}
    .stack-fill {{
      position: absolute;
      inset: 0 auto 0 0;
      border-radius: 999px;
      background: linear-gradient(90deg, var(--accent), #15b8ac);
    }}
    .stack-value {{
      font-size: 12px;
      font-weight: 700;
      color: var(--muted);
      text-align: right;
    }}
    .trend-grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 14px;
    }}
    .trend-card {{
      border-radius: 18px;
      padding: 14px;
      background: rgba(255,255,255,.76);
      border: 1px solid var(--line);
    }}
    .trend-label {{
      font-size: 12px;
      color: var(--muted);
      letter-spacing: .08em;
      text-transform: uppercase;
      margin-bottom: 6px;
    }}
    .trend-value {{
      font-size: 26px;
      font-weight: 800;
      letter-spacing: -.03em;
      margin-bottom: 8px;
    }}
    .trend-svg {{
      width: 100%;
      height: 92px;
      display: block;
    }}
    .trend-meta {{
      display: flex;
      justify-content: space-between;
      gap: 8px;
      color: var(--muted);
      font-size: 11px;
      margin-top: 8px;
    }}
    .alert-list {{
      display: grid;
      gap: 12px;
    }}
    .alert-card {{
      border-radius: 18px;
      padding: 14px 16px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,.78);
    }}
    .alert-card.high {{
      background: rgba(194,65,45,.10);
      border-color: rgba(194,65,45,.18);
    }}
    .alert-card.medium {{
      background: rgba(197,123,18,.10);
      border-color: rgba(197,123,18,.18);
    }}
    .alert-card.low {{
      background: rgba(15,118,110,.08);
      border-color: rgba(15,118,110,.16);
    }}
    .alert-card.ok {{
      background: rgba(15,157,88,.08);
      border-color: rgba(15,157,88,.14);
    }}
    .alert-title {{
      font-size: 15px;
      font-weight: 800;
      margin-bottom: 6px;
    }}
    .alert-detail {{
      font-size: 13px;
      color: var(--muted);
      line-height: 1.7;
    }}
    .alert-action {{
      margin-top: 8px;
      font-size: 12px;
      font-weight: 700;
      color: var(--ink);
    }}
    .param-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 16px;
    }}
    .param-item {{
      border-radius: 16px;
      padding: 12px 14px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,.70);
    }}
    .param-key {{
      font-size: 12px;
      color: var(--muted);
      letter-spacing: .08em;
      text-transform: uppercase;
      margin-bottom: 6px;
    }}
    .param-value {{
      font-size: 18px;
      font-weight: 750;
      letter-spacing: -.02em;
      word-break: break-word;
    }}
    .command-block {{
      white-space: pre-wrap;
      word-break: break-word;
      background: rgba(23,32,51,.05);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 12px 14px;
      color: var(--muted);
      font-family: "SFMono-Regular", "JetBrains Mono", "Menlo", monospace;
      font-size: 12px;
      line-height: 1.7;
    }}
    .hot-list {{
      display: grid;
      gap: 10px;
    }}
    .hot-item {{
      border-radius: 18px;
      padding: 14px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,.74);
    }}
    .hot-top {{
      display: flex;
      gap: 10px;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 8px;
    }}
    .hot-title {{
      font-size: 15px;
      font-weight: 700;
      line-height: 1.5;
    }}
    .hot-count {{
      flex: none;
      padding: 4px 10px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 12px;
      font-weight: 800;
    }}
    .hot-meta {{
      color: var(--muted);
      font-size: 12px;
      line-height: 1.7;
    }}
    .data-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    .data-table thead th {{
      text-align: left;
      font-size: 12px;
      color: var(--muted);
      letter-spacing: .06em;
      text-transform: uppercase;
      padding: 0 10px 12px;
      border-bottom: 1px solid var(--line);
    }}
    .data-table tbody td {{
      border-bottom: 1px solid var(--line);
      text-align: left;
      padding: 12px 10px;
      vertical-align: top;
      line-height: 1.6;
    }}
    .table-subtext {{
      color: var(--muted);
      font-size: 12px;
      margin-top: 4px;
      line-height: 1.6;
    }}
    .empty-row {{
      color: var(--muted);
      text-align: center;
      padding: 18px 10px;
    }}
    .log-card pre {{
      white-space: pre-wrap;
      word-break: break-word;
      background: #182133;
      color: #edf2f7;
      border: 1px solid rgba(255,255,255,.08);
      padding: 14px;
      border-radius: 16px;
      max-height: 320px;
      overflow: auto;
      font-family: "SFMono-Regular", "JetBrains Mono", "Menlo", monospace;
      font-size: 12px;
      line-height: 1.7;
    }}
    .log-card .card-desc {{
      margin-bottom: 14px;
    }}
    @media (max-width: 900px) {{
      .hero-top {{
        flex-direction: column;
      }}
      .hero-meta {{
        justify-content: flex-start;
      }}
      .trend-grid, .param-grid {{
        grid-template-columns: 1fr;
      }}
      .stack-row {{
        grid-template-columns: 1fr;
      }}
      .span-3, .span-4, .span-6, .span-8, .span-12 {{ grid-column: span 12; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div id="app" class="grid"></div>
  </div>
  <script>
    const app = document.getElementById('app');
    const 原因名稱 = {{
      style_unknown: '市場型別未識別',
      crypto_unsupported: '資產未納入支援',
      non_crypto_unrelated: '非加密無關市場',
      missing_end_date: '缺少到期時間',
      spread_too_wide: '價差過寬',
      edge_too_low: '優勢不足',
      confidence_too_low: '信心不足',
      window_not_open: '未進入窗口',
      volume_too_low: '成交量過低',
      orderbook_unavailable: '無法取得深度',
      ask_quote_missing: '缺少賣價',
      anchor_unavailable: '錨點不可用',
      anchor_source_unsupported: '錨點來源不支援'
    }};
    function esc(v) {{
      return String(v ?? '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;');
    }}
    function formatTime(v) {{
      if (!v) return '-';
      try {{
        return new Date(v).toLocaleString('zh-TW', {{ hour12: false }});
      }} catch (_err) {{
        return String(v);
      }}
    }}
    function 原因標籤(key) {{
      return 原因名稱[key] || key;
    }}
    function 秒數文案(value) {{
      const n = Number(value);
      if (!Number.isFinite(n)) return '-';
      if (n >= 3600) return `${{(n / 3600).toFixed(1)}} 小時`;
      if (n >= 60) return `${{(n / 60).toFixed(1)}} 分鐘`;
      return `${{n.toFixed(0)}} 秒`;
    }}
    function 拒絕文案(row) {{
      if (!row) return '-';
      if (row.reason === 'window_not_open') {{
        if (row.window_label) return row.window_label;
        if (row.window_state === 'observe') return '已開盤未進尾盤';
        if (row.window_state === 'armed') return '已進入尾盤預備';
        if (row.window_state === 'attack') return '已進入尾盤攻擊';
      }}
      return 原因標籤(row.reason);
    }}
    function 拒絕補充(row) {{
      if (!row || row.reason !== 'window_not_open') return '';
      const parts = [];
      if (row.seconds_to_armed !== undefined && row.seconds_to_armed !== null && Number(row.seconds_to_armed) > 0) {{
        parts.push(`距離 armed：約${{秒數文案(row.seconds_to_armed)}}`);
      }}
      if (row.tau_seconds !== undefined && row.tau_seconds !== null) {{
        parts.push(`距離到期：約${{秒數文案(row.tau_seconds)}}`);
      }}
      return parts.join('｜');
    }}
    function 百分比(ratio) {{
      return `${{(Number(ratio || 0) * 100).toFixed(0)}}%`;
    }}
    function 數值(v) {{
      return v === null || v === undefined || v === '' ? '-' : String(v);
    }}
    function 主要原因(reasons) {{
      const entries = Object.entries(reasons || {{}}).sort((a,b) => b[1]-a[1]);
      if (!entries.length) return null;
      return entries[0];
    }}
    function 判斷結論(cycle, reasons) {{
      const totalRejects = Object.values(reasons || {{}}).reduce((sum, v) => sum + Number(v || 0), 0);
      const top = 主要原因(reasons);
      if (!cycle) {{
        return {{
          title: '尚未取得有效循環結果',
          body: '監控頁已啟動，但還沒有新的循環摘要可供判讀。通常等第一輪掃描完成後，這裡就會出現本輪判斷結論。',
        }};
      }}
      if ((cycle.executed || 0) > 0) {{
        return {{
          title: '本輪已進入送單階段',
          body: `本輪已送出 ${{cycle.executed}} 筆訂單，失敗 ${{cycle.failed}} 筆。接下來可優先檢查訂單日誌與成交回寫。`,
        }};
      }}
      if ((cycle.candidates || 0) > 0 && (cycle.selected || 0) === 0) {{
        return {{
          title: '本輪有候選，但尚未通過最終執行篩選',
          body: `本輪找到 ${{cycle.candidates}} 筆候選，但沒有進入送單。建議對照拒絕摘要與最近觀測，檢查 spread、edge、confidence 與窗口狀態。`,
        }};
      }}
      if ((cycle.candidates || 0) === 0 && totalRejects > 0) {{
        return {{
          title: '本輪未進入下單階段',
          body: top
            ? `目前沒有產生可交易候選，多數市場在研究前置階段就被排除。最主要的原因是「${{原因標籤(top[0])}}」，共 ${{top[1]}} 筆。`
            : '目前沒有產生可交易候選，多數市場在研究前置階段就被排除。',
        }};
      }}
      return {{
        title: '本輪沒有新的可執行動作',
        body: '目前監控沒有看到需要下單或錯誤升級的訊號，可持續觀察下一輪摘要與拒絕原因變化。',
      }};
    }}
    function 趨勢圖(points, field, color) {{
      if (!points.length) {{
        return '<div class="empty-row">最近 30 分鐘沒有趨勢資料</div>';
      }}
      const width = 260;
      const height = 92;
      const pad = 10;
      const values = points.map(point => Number(point[field] || 0));
      const maxValue = Math.max(...values, 1);
      const step = points.length === 1 ? 0 : (width - pad * 2) / (points.length - 1);
      const path = values.map((value, index) => {{
        const x = pad + step * index;
        const y = height - pad - ((height - pad * 2) * value / maxValue);
        return `${{index === 0 ? 'M' : 'L'}} ${{x.toFixed(2)}} ${{y.toFixed(2)}}`;
      }}).join(' ');
      const area = `${{path}} L ${{width - pad}} ${{height - pad}} L ${{pad}} ${{height - pad}} Z`;
      const latest = values[values.length - 1] ?? 0;
      const peak = Math.max(...values);
      return `
        <svg viewBox="0 0 ${{width}} ${{height}}" class="trend-svg" preserveAspectRatio="none">
          <line x1="${{pad}}" y1="${{height - pad}}" x2="${{width - pad}}" y2="${{height - pad}}" stroke="rgba(23,32,51,.08)" stroke-width="1" />
          <path d="${{area}}" fill="${{color}}" opacity="0.12"></path>
          <path d="${{path}}" fill="none" stroke="${{color}}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></path>
        </svg>
        <div class="trend-meta">
          <span>最新：${{latest}}</span>
          <span>峰值：${{peak}}</span>
        </div>
      `;
    }}
    function render(data) {{
      const cycle = data.latest_cycle || null;
      const process = data.process || {{running:false, processes:[]}};
      const reasons = data.latest_reasons || {{summary: {{}}, samples: []}};
      const breakdown = data.buy_block_breakdown || {{items: [], total: 0, primary: null}};
      const trend = data.trend || {{points: [], window_minutes: 30}};
      const hotRejects = data.hot_reject_samples || [];
      const params = data.parameter_snapshot || {{}};
      const alerts = data.alerts || [];
      const logs = data.logs || {{}};
      const obs = data.recent_observations || [];
      const decision = 判斷結論(cycle, reasons.summary || {{}});
      const topReason = 主要原因(reasons.summary || {{}});
      const totalRejects = Object.values(reasons.summary || {{}}).reduce((sum, v) => sum + Number(v || 0), 0);
      const reasonBadges = Object.entries(reasons.summary || {{}})
        .sort((a,b) => b[1]-a[1])
        .map(([k,v]) => `<span class="reason-pill"><b>${{esc(原因標籤(k))}}</b> ${{esc(v)}} 筆</span>`)
        .join('');
      const breakdownRows = (breakdown.items || []).map(item => `
        <div class="stack-row">
          <div class="stack-label">${{esc(item.label)}}</div>
          <div class="stack-track"><div class="stack-fill" style="width:${{Math.max(item.pct * 100, item.count > 0 ? 6 : 0)}}%"></div></div>
          <div class="stack-value">${{esc(item.count)}} / ${{esc(百分比(item.pct))}}</div>
        </div>
      `).join('');
      const alertCards = alerts.map(alert => `
        <div class="alert-card ${{esc(alert.severity)}}">
          <div class="alert-title">${{esc(alert.title)}}</div>
          <div class="alert-detail">${{esc(alert.detail)}}</div>
          <div class="alert-action">優先處理：${{esc(alert.action || '請先查看最新相關日誌。')}}</div>
        </div>
      `).join('');
      const hotRejectRows = hotRejects.map(item => `
        <div class="hot-item">
          <div class="hot-top">
            <div class="hot-title">${{esc(item.question || '未提供題目')}}</div>
            <span class="hot-count">${{esc(item.count)}} 次</span>
          </div>
          <div class="hot-meta">原因：${{esc(拒絕文案(item))}}｜資產：${{esc(item.asset || '-')}}｜型別：${{esc(item.style || '-')}}｜週期：${{esc(item.timeframe || '-')}}</div>
          <div class="hot-meta">${{esc(拒絕補充(item) || '未提供窗口補充資訊')}}</div>
          <div class="hot-meta">最近出現：${{esc(formatTime(item.last_seen))}}</div>
        </div>
      `).join('');
      const paramItems = [
        ['模式', params.mode],
        ['掃描上限', params.limit_events],
        ['輪詢秒數', params.scan_interval],
        ['資產', params.assets],
        ['型別', params.styles],
        ['最多候選', params.max_candidates],
      ].map(([label, value]) => `
        <div class="param-item">
          <div class="param-key">${{esc(label)}}</div>
          <div class="param-value">${{esc(數值(value))}}</div>
        </div>
      `).join('');
      const processRows = (process.processes || []).map(
        p => `<tr><td>${{esc(p.pid)}}</td><td>${{esc(p.etime)}}</td><td>${{esc(p.command)}}</td></tr>`
      ).join('');
      const observationRows = obs.map(
        row => `<tr>
          <td>${{esc(row.timestamp)}}</td>
          <td>${{esc(row.asset)}}</td>
          <td>${{esc(row.market_style)}}</td>
          <td>${{esc(row.net_edge_selected)}}</td>
          <td>${{esc(row.model_confidence_score)}}</td>
          <td>${{esc(row.window_state)}}</td>
          <td>${{esc(row.market_id)}}</td>
        </tr>`
      ).join('');
      const sampleRows = (reasons.samples || []).map(
        row => `<tr><td>${{esc(拒絕文案(row))}}</td><td>${{esc(row.asset)}}</td><td>${{esc(row.style)}}</td><td>${{esc(row.timeframe)}}</td><td>${{esc(row.question)}}${{拒絕補充(row) ? `<div class="table-subtext">${{esc(拒絕補充(row))}}</div>` : ''}}</td></tr>`
      ).join('');
      app.innerHTML = `
        <section class="card hero span-12">
          <div class="hero-top">
            <div>
              <div class="section-kicker">即時監控總覽</div>
              <h1 class="hero-title">Polymarket 機器人監控台</h1>
              <p class="hero-subtitle">把本輪掃描結果、未買入主因、關鍵觀測與核心日誌收斂到同一頁，先看結論，再往下追細節，不需要在多個檔案之間來回切換。</p>
            </div>
            <div class="hero-meta">
              <span class="meta-pill"><strong>自動刷新</strong> <span id="refresh-countdown">{config.refresh_seconds}</span> 秒</span>
              <span class="meta-pill"><strong>更新時間</strong> ${{esc(formatTime(data.generated_at))}}</span>
            </div>
          </div>
        </section>
        <section class="card span-3">
          <div class="metric-label">主進程狀態</div>
          <div class="status-line ${{process.running ? 'ok' : 'bad'}}"><span class="status-dot"></span>${{process.running ? '執行中' : '已停止'}}</div>
          <div class="metric-foot">${{process.running ? '已偵測到交易主程序，監控資料正在持續刷新。' : '目前未找到 run_auto_trading.py，需先確認主程序是否啟動。'}}</div>
        </section>
        <section class="card span-3">
          <div class="metric-label">本輪掃描量</div>
          <div class="metric-value">${{cycle ? esc(cycle.scanned) : '-'}}</div>
          <div class="metric-foot">本輪實際掃描事件數，可用來評估目前批次是否適合短週期市場。</div>
        </section>
        <section class="card span-3">
          <div class="metric-label">候選機會</div>
          <div class="metric-value">${{cycle ? esc(cycle.candidates) : '-'}}</div>
          <div class="metric-foot">若長時間維持 0，代表問題多半在市場篩選與研究前置，不在下單層。</div>
        </section>
        <section class="card span-3">
          <div class="metric-label">已送單 / 失敗</div>
          <div class="metric-value">${{cycle ? `${{esc(cycle.executed)}} / ${{esc(cycle.failed)}}` : '-'}}</div>
          <div class="metric-foot">快速辨識本輪是否真的進入執行，以及是否有送單錯誤需要追查。</div>
        </section>
        <section class="card decision-card span-8">
          <div class="section-kicker">判斷結論</div>
          <h2 class="decision-title">${{esc(decision.title)}}</h2>
          <p class="decision-body">${{esc(decision.body)}}</p>
          <div class="reason-cloud">
            <span class="meta-pill"><strong>入選</strong> ${{cycle ? esc(cycle.selected) : '-'}}</span>
            <span class="meta-pill"><strong>已送單</strong> ${{cycle ? esc(cycle.executed) : '-'}}</span>
            <span class="meta-pill"><strong>拒絕總數</strong> ${{esc(totalRejects)}}</span>
            <span class="meta-pill"><strong>主要原因</strong> ${{topReason ? esc(原因標籤(topReason[0])) : '暫無'}}</span>
          </div>
        </section>
        <section class="card span-4">
          <div class="section-kicker">拒絕摘要</div>
          <h2 class="card-title">未買入主因</h2>
          <p class="card-desc">依拒絕數量排序，先看前兩三個主因，通常就能定位目前沒單的真正瓶頸。</p>
          <div class="reason-cloud">${{reasonBadges || '<span class="reason-pill">目前無拒絕摘要</span>'}}</div>
        </section>
        <section class="card span-6">
          <div class="section-kicker">四層阻塞</div>
          <h2 class="card-title">本輪不買結論卡</h2>
          <p class="card-desc">把本輪沒有形成訂單的阻塞路徑拆成市場過濾、研究拒絕、風控拒絕、執行拒絕四層，快速判斷真正卡在哪一層。</p>
          <div class="reason-cloud">
            <span class="meta-pill"><strong>阻塞總量</strong> ${{esc(breakdown.total || 0)}}</span>
            <span class="meta-pill"><strong>主要層級</strong> ${{breakdown.primary ? esc(breakdown.primary.label) : '暫無'}}</span>
          </div>
          <div class="stack-list">${{breakdownRows || '<div class="empty-row">目前無阻塞分層資料</div>'}}</div>
        </section>
        <section class="card span-6">
          <div class="section-kicker">參數快照</div>
          <h2 class="card-title">目前 Live 參數</h2>
          <p class="card-desc">直接從正在執行的命令列解析當前常駐參數，避免監控頁看的是舊值、實際跑的是另一組參數。</p>
          <div class="param-grid">${{paramItems}}</div>
          <div class="command-block">${{esc(params.raw_command || '目前無執行命令可顯示')}}</div>
        </section>
        <section class="card span-12">
          <div class="section-kicker">短趨勢</div>
          <h2 class="card-title">最近 30 分鐘趨勢</h2>
          <p class="card-desc">不要只看單輪，短趨勢能幫你辨識候選是否持續為零、掃描量是否穩定、執行是否突然停滯。</p>
          <div class="trend-grid">
            <div class="trend-card">
              <div class="trend-label">Scanned</div>
              <div class="trend-value">${{cycle ? esc(cycle.scanned) : '-'}}</div>
              ${{趨勢圖(trend.points || [], 'scanned', '#0f766e')}}
            </div>
            <div class="trend-card">
              <div class="trend-label">Candidates</div>
              <div class="trend-value">${{cycle ? esc(cycle.candidates) : '-'}}</div>
              ${{趨勢圖(trend.points || [], 'candidates', '#2563eb')}}
            </div>
            <div class="trend-card">
              <div class="trend-label">Executed</div>
              <div class="trend-value">${{cycle ? esc(cycle.executed) : '-'}}</div>
              ${{趨勢圖(trend.points || [], 'executed', '#0f9d58')}}
            </div>
            <div class="trend-card">
              <div class="trend-label">Failed</div>
              <div class="trend-value">${{cycle ? esc(cycle.failed) : '-'}}</div>
              ${{趨勢圖(trend.points || [], 'failed', '#c2412d')}}
            </div>
          </div>
        </section>
        <section class="card span-12">
          <div class="section-kicker">異常提醒</div>
          <h2 class="card-title">可疑異常提醒</h2>
          <p class="card-desc">把需要優先處理的監控訊號主動挑出來，不用再手動翻日誌判斷是否異常。</p>
          <div class="alert-list">${{alertCards}}</div>
        </section>
        <section class="card span-12">
          <div class="section-kicker">執行背景</div>
          <h2 class="card-title">交易主進程</h2>
          <p class="card-desc">確認目前真正執行中的命令列、存活時間與 PID，避免誤看舊進程或錯參數常駐。</p>
          <table class="data-table">
            <thead><tr><th>PID</th><th>存活時間</th><th>命令</th></tr></thead>
            <tbody>${{processRows || '<tr><td colspan="3" class="empty-row">未找到進程</td></tr>'}}</tbody>
          </table>
        </section>
        <section class="card span-6">
          <div class="section-kicker">熱門樣本</div>
          <h2 class="card-title">熱門拒絕市場樣本</h2>
          <p class="card-desc">把最近最常被擋的題目做聚合排序，方便你快速看出哪些市場反覆卡在同一個原因。</p>
          <div class="hot-list">${{hotRejectRows || '<div class="empty-row">目前沒有可聚合的拒絕樣本</div>'}}</div>
        </section>
        <section class="card span-6">
          <div class="section-kicker">研究觀測</div>
          <h2 class="card-title">最近觀測</h2>
          <p class="card-desc">從落庫觀測快速回看市場型別、淨優勢、模型信心與窗口狀態，判斷研究層是否正常輸出。</p>
          <table class="data-table">
            <thead><tr><th>時間</th><th>資產</th><th>市場型別</th><th>淨優勢</th><th>信心</th><th>窗口</th><th>市場 ID</th></tr></thead>
            <tbody>${{observationRows || '<tr><td colspan="7" class="empty-row">目前無觀測資料</td></tr>'}}</tbody>
          </table>
        </section>
        <section class="card span-12">
          <div class="section-kicker">原始樣本</div>
          <h2 class="card-title">最近未買入樣本</h2>
          <p class="card-desc">保留最近被拒絕的原始樣本，讓你在熱門聚合之外，也能回看最新幾筆具體題目。</p>
          <table class="data-table">
            <thead><tr><th>原因</th><th>資產</th><th>型別</th><th>週期</th><th>問題</th></tr></thead>
            <tbody>${{sampleRows || '<tr><td colspan="5" class="empty-row">目前無樣本</td></tr>'}}</tbody>
          </table>
        </section>
        <section class="card log-card span-6">
          <div class="section-kicker">核心日誌</div>
          <h2 class="card-title">生命週期日誌</h2>
          <p class="card-desc">最新在上，優先看程序啟動、循環完成與 preflight 是否異常。</p>
          <pre>${{esc((logs.lifecycle || []).join('\\n'))}}</pre>
        </section>
        <section class="card log-card span-6">
          <div class="section-kicker">核心日誌</div>
          <h2 class="card-title">候選日誌</h2>
          <p class="card-desc">對照候選拒絕摘要與樣本，觀察市場在研究流程的哪一層被擋掉。</p>
          <pre>${{esc((logs.candidate || []).join('\\n'))}}</pre>
        </section>
        <section class="card log-card span-6">
          <div class="section-kicker">執行追蹤</div>
          <h2 class="card-title">訂單日誌</h2>
          <p class="card-desc">當真的有送單時，這裡是確認下單狀態、成交與拒單訊息的第一現場。</p>
          <pre>${{esc((logs.order || []).join('\\n'))}}</pre>
        </section>
        <section class="card log-card span-6">
          <div class="section-kicker">異常追蹤</div>
          <h2 class="card-title">錯誤日誌</h2>
          <p class="card-desc">集中查看 preflight、資料源與執行例外，方便快速分辨是啟動問題還是市場問題。</p>
          <pre>${{esc((logs.error || []).join('\\n'))}}</pre>
        </section>
      `;
    }}
    const refreshMs = {refresh_ms};
    let nextRefreshAt = Date.now() + refreshMs;
    let refreshTimeoutId = null;

    function 更新刷新倒數() {{
      const countdownNode = document.getElementById('refresh-countdown');
      if (!countdownNode) {{
        return;
      }}
      const remainingMs = Math.max(nextRefreshAt - Date.now(), 0);
      countdownNode.textContent = String(Math.ceil(remainingMs / 1000));
    }}

    function 排程下一次刷新() {{
      if (refreshTimeoutId !== null) {{
        clearTimeout(refreshTimeoutId);
      }}
      nextRefreshAt = Date.now() + refreshMs;
      更新刷新倒數();
      refreshTimeoutId = setTimeout(refresh, refreshMs);
    }}

    async function refresh() {{
      try {{
        const res = await fetch('/api/status');
        const data = await res.json();
        render(data);
      }} finally {{
        排程下一次刷新();
      }}
    }}

    setInterval(更新刷新倒數, 250);
    更新刷新倒數();
    refresh();
  </script>
</body>
</html>"""


def create_handler(config: MonitorConfig):
    """建立 HTTP handler 類別。"""

    class MonitorHandler(BaseHTTPRequestHandler):
        """監控服務處理器。"""

        def _write_json(self, payload: Dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
            body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status.value)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _write_html(self, html: str) -> None:
            body = html.encode("utf-8")
            self.send_response(HTTPStatus.OK.value)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            """處理 GET 請求。"""
            path = urlparse(self.path).path
            if path == "/":
                self._write_html(_render_html(config))
                return
            if path == "/api/status":
                self._write_json(_build_status_payload(config))
                return
            if path == "/api/reasons":
                self._write_json(_build_reasons_payload(config))
                return
            self._write_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)

        def log_message(self, format: str, *args: Any) -> None:
            """抑制 HTTP 服務預設存取日誌。"""
            return

    return MonitorHandler


def serve_monitor(host: str, port: int, config: MonitorConfig) -> None:
    """啟動監控服務。"""
    server = ThreadingHTTPServer((host, port), create_handler(config))
    print(f"監控頁啟動中：http://{host}:{port}")
    server.serve_forever()
