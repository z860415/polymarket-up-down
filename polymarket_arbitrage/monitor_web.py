"""
Polymarket Trading Bot Web Dashboard - Professional Edition

Features:
- Professional dark theme UI
- Real-time bot control (start/stop/restart)
- Comprehensive monitoring metrics
- WebSocket-like auto-refresh
"""

from __future__ import annotations

import ast
import json
import re
import shlex
import sqlite3
import subprocess
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs
import socketserver


@dataclass(frozen=True)
class MonitorConfig:
    """監控服務設定。"""

    db_path: Path
    log_dir: Path
    refresh_seconds: int = 5


# Regex patterns
_CYCLE_PATTERN = re.compile(
    r"循環完成 \| mode=(?P<mode>\w+) \| scanned=(?P<scanned>\d+) \| candidates=(?P<candidates>\d+) "
    r"\| selected=(?P<selected>\d+) \| executed=(?P<executed>\d+) \| failed=(?P<failed>\d+) "
    r"\| claim_submitted=(?P<claim_submitted>\d+) \| claim_failed=(?P<claim_failed>\d+) "
    r"\| claim_dry_run=(?P<claim_dry_run>\d+)"
)
_LOG_TIMESTAMP_PATTERN = re.compile(r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})")

# Rejection reason labels
原因名稱 = {
    "style_unknown": "型別未識別",
    "crypto_unsupported": "資產不支援",
    "non_crypto_unrelated": "非加密市場",
    "missing_end_date": "缺少到期日",
    "timeframe_missing": "缺少週期",
    "style_not_supported": "型別不支援",
    "missing_token_ids": "缺少代幣ID",
    "orderbook_unavailable": "無法取得深度",
    "ask_quote_missing": "缺少報價",
    "spread_too_wide": "價差過寬",
    "volume_too_low": "成交量低",
    "spot_price_unavailable": "現價不可用",
    "fair_probability_failed": "模型計算失敗",
    "edge_too_low": "優勢不足",
    "confidence_too_low": "信心不足",
    "anchor_source_unsupported": "錨點不支援",
    "anchor_unavailable": "錨點不可用",
    "sigma_window_unsupported": "窗口不支援",
    "window_not_open": "窗口未開",
    "lead_z_too_low": "偏離度不足",
    "clob_reject": "CLOB拒絕",
    "orderbook_reject": "深度拒絕",
    "status_reject": "狀態拒絕",
    "insufficient_balance": "餘額不足",
    "position_limit": "倉位限制",
    "exposure_limit": "曝險限制",
    "marketable_buy_below_min_notional": "低於最小名義值",
    "time_to_expiry_too_short": "剩餘時間過短",
    "duplicate_directional_exposure": "重複方向曝險",
    "min_notional_not_met": "未達最小名義值",
    "risk_reject": "風控拒絕",
    "order_rejected": "訂單被拒",
    "order_submit_failed": "提交失敗",
    "execution_failed": "執行失敗",
    "api_error": "API錯誤",
    "order_status_failed": "狀態查詢失敗",
}


class BotController:
    """Bot process controller singleton."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._init()
        return cls._instance

    def _init(self):
        self.last_action_time = 0
        self.last_action_result = None
        self.lock = threading.Lock()

    def _get_workspace_path(self) -> Path:
        """Get the workspace path."""
        return Path("/root/.openclaw/workspace/polymarket-arbitrage")

    def get_status(self) -> Dict[str, Any]:
        """Get current bot status."""
        processes = self._find_bot_processes()
        return {
            "running": len(processes) > 0,
            "processes": processes,
            "count": len(processes),
        }

    def _find_bot_processes(self) -> List[Dict[str, str]]:
        """Find running bot processes."""
        try:
            result = subprocess.run(
                ["ps", "-ax", "-o", "pid=,etime=,command="],
                capture_output=True,
                text=True,
                check=False,
            )
            processes = []
            for line in result.stdout.splitlines():
                if "run_auto_trading.py" not in line or "run_monitor_web.py" in line:
                    continue
                stripped = line.strip()
                if not stripped:
                    continue
                parts = stripped.split(None, 2)
                if len(parts) < 3:
                    continue
                processes.append({
                    "pid": parts[0],
                    "etime": parts[1],
                    "command": parts[2],
                })
            return processes
        except Exception:
            return []

    def start(self) -> Dict[str, Any]:
        """Start the bot."""
        with self.lock:
            status = self.get_status()
            if status["running"]:
                return {"success": False, "message": "機器人已經在運行中", "status": status}

            try:
                workspace = self._get_workspace_path()
                cmd = (
                    f"cd {workspace} && source venv/bin/activate && "
                    "nohup env HTTP_PROXY=http://127.0.0.1:10809 HTTPS_PROXY=http://127.0.0.1:10809 "
                    "python3 run_auto_trading.py --mode live --continuous --scan-interval 10 "
                    "--styles up_down --timeframes 5m,15m,1h,4h,1d --tail-mode adaptive "
                    "--max-candidates 3 --log-dir ./logs > /tmp/trading_bot.log 2>&1 &"
                )
                subprocess.Popen(cmd, shell=True, executable="/bin/bash")
                time.sleep(2)
                status = self.get_status()
                self.last_action_time = time.time()
                self.last_action_result = {"action": "start", "status": status}
                return {"success": status["running"], "message": "機器人啟動" + ("成功" if status["running"] else "中..."), "status": status}
            except Exception as e:
                return {"success": False, "message": f"啟動失敗: {str(e)}", "status": self.get_status()}

    def stop(self) -> Dict[str, Any]:
        """Stop the bot."""
        with self.lock:
            status = self.get_status()
            if not status["running"]:
                return {"success": False, "message": "機器人未運行", "status": status}

            try:
                # Kill all trading bot processes
                subprocess.run(["pkill", "-f", "run_auto_trading.py"], check=False)
                time.sleep(1)
                status = self.get_status()
                self.last_action_time = time.time()
                self.last_action_result = {"action": "stop", "status": status}
                return {"success": not status["running"], "message": "機器人已停止" if not status["running"] else "停止中...", "status": status}
            except Exception as e:
                return {"success": False, "message": f"停止失敗: {str(e)}", "status": self.get_status()}

    def restart(self) -> Dict[str, Any]:
        """Restart the bot."""
        self.stop()
        time.sleep(2)
        return self.start()


bot_controller = BotController()


def _parse_log_timestamp(line: str) -> Optional[datetime]:
    match = _LOG_TIMESTAMP_PATTERN.match(line)
    if match is None:
        return None
    try:
        return datetime.strptime(match.group("timestamp"), "%Y-%m-%d %H:%M:%S,%f")
    except ValueError:
        return None


def _tail_lines(path: Path, limit: int = 40) -> List[str]:
    if not path.exists():
        return []
    try:
        return path.read_text(encoding="utf-8", errors="ignore").splitlines()[-limit:]
    except Exception:
        return []


def _parse_cycle_line(line: str) -> Optional[Dict[str, Any]]:
    match = _CYCLE_PATTERN.search(line)
    if match is None:
        return None
    payload = match.groupdict()
    for key, value in list(payload.items()):
        if key != "mode":
            payload[key] = int(value)
    return payload


def _latest_cycle(log_dir: Path) -> Optional[Dict[str, Any]]:
    for line in reversed(_tail_lines(log_dir / "lifecycle.log", limit=200)):
        payload = _parse_cycle_line(line)
        if payload is not None:
            timestamp = _parse_log_timestamp(line)
            payload["timestamp"] = timestamp.isoformat() if timestamp else None
            return payload
    return None


def _recent_cycles(log_dir: Path, minutes: int = 30) -> List[Dict[str, Any]]:
    cutoff = datetime.now() - timedelta(minutes=minutes)
    cycles = []
    for line in _tail_lines(log_dir / "lifecycle.log", limit=4000):
        payload = _parse_cycle_line(line)
        if payload is None:
            continue
        timestamp = _parse_log_timestamp(line)
        if timestamp and timestamp < cutoff:
            continue
        payload["timestamp"] = timestamp.isoformat() if timestamp else None
        cycles.append(payload)
    return cycles[-240:]


def _latest_reject_summary(log_dir: Path) -> Dict[str, Any]:
    summary = {}
    samples = []
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
    return {"summary": summary, "samples": samples}


def _build_status_payload(config: MonitorConfig) -> Dict[str, Any]:
    log_dir = config.log_dir
    bot_status = bot_controller.get_status()
    latest_cycle = _latest_cycle(log_dir)
    latest_reasons = _latest_reject_summary(log_dir)
    recent_cycles = _recent_cycles(log_dir, minutes=30)

    # Calculate uptime and stats
    total_rejects = sum(latest_reasons["summary"].values()) if latest_reasons["summary"] else 0
    top_reason = max(latest_reasons["summary"].items(), key=lambda x: x[1]) if latest_reasons["summary"] else None

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "bot": bot_status,
        "latest_cycle": latest_cycle,
        "latest_reasons": latest_reasons,
        "trend": {
            "window_minutes": 30,
            "points": recent_cycles,
            "count": len(recent_cycles),
        },
        "summary": {
            "total_rejects": total_rejects,
            "top_reason": top_reason[0] if top_reason else None,
            "top_reason_count": top_reason[1] if top_reason else 0,
        },
        "logs": {
            "lifecycle": list(reversed(_tail_lines(log_dir / "lifecycle.log", limit=30))),
            "candidate": list(reversed(_tail_lines(log_dir / "candidate.log", limit=30))),
            "error": list(reversed(_tail_lines(log_dir / "error.log", limit=30))),
        },
    }


def _render_html(config: MonitorConfig) -> str:
    refresh_ms = max(config.refresh_seconds, 1) * 1000
    return f"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Polymarket Trading Bot | 專業監控台</title>
    <style>
        :root {{
            --bg-primary: #0f172a;
            --bg-secondary: #1e293b;
            --bg-tertiary: #334155;
            --bg-card: #1e293b;
            --text-primary: #f1f5f9;
            --text-secondary: #94a3b8;
            --text-muted: #64748b;
            --accent-primary: #06b6d4;
            --accent-secondary: #8b5cf6;
            --accent-success: #10b981;
            --accent-warning: #f59e0b;
            --accent-danger: #ef4444;
            --border: #334155;
            --shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.3), 0 2px 4px -1px rgba(0, 0, 0, 0.2);
            --shadow-lg: 0 20px 25px -5px rgba(0, 0, 0, 0.4), 0 10px 10px -5px rgba(0, 0, 0, 0.2);
        }}

        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang TC', 'Microsoft JhengHei', sans-serif;
            background: linear-gradient(135deg, var(--bg-primary) 0%, #0a0f1d 100%);
            color: var(--text-primary);
            min-height: 100vh;
            line-height: 1.6;
        }}

        .container {{
            max-width: 1600px;
            margin: 0 auto;
            padding: 24px;
        }}

        /* Header */
        .header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 32px;
            padding: 24px 32px;
            background: var(--bg-card);
            border-radius: 16px;
            border: 1px solid var(--border);
            box-shadow: var(--shadow-lg);
        }}

        .header-left {{
            display: flex;
            align-items: center;
            gap: 20px;
        }}

        .logo {{
            width: 48px;
            height: 48px;
            background: linear-gradient(135deg, var(--accent-primary), var(--accent-secondary));
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 24px;
            font-weight: bold;
        }}

        .header-title {{
            font-size: 28px;
            font-weight: 700;
            background: linear-gradient(135deg, var(--text-primary), var(--accent-primary));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }}

        .header-subtitle {{
            color: var(--text-secondary);
            font-size: 14px;
            margin-top: 4px;
        }}

        .header-meta {{
            display: flex;
            align-items: center;
            gap: 16px;
        }}

        .refresh-indicator {{
            display: flex;
            align-items: center;
            gap: 8px;
            color: var(--text-muted);
            font-size: 13px;
        }}

        .refresh-dot {{
            width: 8px;
            height: 8px;
            background: var(--accent-success);
            border-radius: 50%;
            animation: pulse 2s infinite;
        }}

        @keyframes pulse {{
            0%, 100% {{ opacity: 1; }}
            50% {{ opacity: 0.5; }}
        }}

        /* Control Panel */
        .control-panel {{
            display: flex;
            gap: 12px;
        }}

        .btn {{
            padding: 12px 24px;
            border: none;
            border-radius: 10px;
            font-size: 14px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s ease;
            display: flex;
            align-items: center;
            gap: 8px;
        }}

        .btn:disabled {{
            opacity: 0.5;
            cursor: not-allowed;
        }}

        .btn-primary {{
            background: linear-gradient(135deg, var(--accent-primary), #0891b2);
            color: white;
        }}

        .btn-primary:hover:not(:disabled) {{
            transform: translateY(-2px);
            box-shadow: 0 8px 20px rgba(6, 182, 212, 0.3);
        }}

        .btn-danger {{
            background: linear-gradient(135deg, var(--accent-danger), #dc2626);
            color: white;
        }}

        .btn-danger:hover:not(:disabled) {{
            transform: translateY(-2px);
            box-shadow: 0 8px 20px rgba(239, 68, 68, 0.3);
        }}

        .btn-secondary {{
            background: var(--bg-tertiary);
            color: var(--text-primary);
            border: 1px solid var(--border);
        }}

        .btn-secondary:hover:not(:disabled) {{
            background: var(--bg-card);
        }}

        /* Status Banner */
        .status-banner {{
            display: flex;
            align-items: center;
            gap: 16px;
            padding: 16px 24px;
            margin-bottom: 24px;
            border-radius: 12px;
            border: 1px solid var(--border);
            background: var(--bg-card);
        }}

        .status-banner.running {{
            border-color: var(--accent-success);
            background: linear-gradient(135deg, rgba(16, 185, 129, 0.1), var(--bg-card));
        }}

        .status-banner.stopped {{
            border-color: var(--accent-danger);
            background: linear-gradient(135deg, rgba(239, 68, 68, 0.1), var(--bg-card));
        }}

        .status-icon {{
            width: 40px;
            height: 40px;
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 20px;
        }}

        .status-banner.running .status-icon {{
            background: rgba(16, 185, 129, 0.2);
            color: var(--accent-success);
        }}

        .status-banner.stopped .status-icon {{
            background: rgba(239, 68, 68, 0.2);
            color: var(--accent-danger);
        }}

        .status-info {{
            flex: 1;
        }}

        .status-title {{
            font-size: 18px;
            font-weight: 700;
        }}

        .status-detail {{
            color: var(--text-secondary);
            font-size: 14px;
        }}

        /* Grid Layout */
        .grid {{
            display: grid;
            grid-template-columns: repeat(12, 1fr);
            gap: 20px;
        }}

        .card {{
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 24px;
            box-shadow: var(--shadow);
        }}

        .card-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
        }}

        .card-title {{
            font-size: 16px;
            font-weight: 600;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}

        .card-value {{
            font-size: 42px;
            font-weight: 700;
            margin: 8px 0;
        }}

        .card-subtitle {{
            font-size: 13px;
            color: var(--text-muted);
        }}

        .span-3 {{ grid-column: span 3; }}
        .span-4 {{ grid-column: span 4; }}
        .span-6 {{ grid-column: span 6; }}
        .span-8 {{ grid-column: span 8; }}
        .span-12 {{ grid-column: span 12; }}

        /* Metrics */
        .metric-positive {{ color: var(--accent-success); }}
        .metric-negative {{ color: var(--accent-danger); }}
        .metric-neutral {{ color: var(--accent-primary); }}
        .metric-warning {{ color: var(--accent-warning); }}

        /* Chart Area */
        .chart-container {{
            height: 200px;
            background: var(--bg-primary);
            border-radius: 12px;
            padding: 16px;
            margin-top: 16px;
            position: relative;
            overflow: hidden;
        }}

        .chart-svg {{
            width: 100%;
            height: 100%;
        }}

        /* Reason Pills */
        .reason-list {{
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            margin-top: 16px;
        }}

        .reason-pill {{
            padding: 8px 14px;
            background: var(--bg-primary);
            border: 1px solid var(--border);
            border-radius: 20px;
            font-size: 13px;
            display: flex;
            align-items: center;
            gap: 8px;
        }}

        .reason-pill .count {{
            background: var(--accent-primary);
            color: white;
            padding: 2px 8px;
            border-radius: 10px;
            font-size: 11px;
            font-weight: 700;
        }}

        /* Log Section */
        .log-container {{
            background: var(--bg-primary);
            border-radius: 12px;
            padding: 16px;
            font-family: 'JetBrains Mono', 'SF Mono', Monaco, monospace;
            font-size: 12px;
            line-height: 1.8;
            max-height: 300px;
            overflow-y: auto;
        }}

        .log-line {{
            color: var(--text-secondary);
            padding: 2px 0;
            border-bottom: 1px solid rgba(255,255,255,0.05);
        }}

        .log-line:last-child {{
            border-bottom: none;
        }}

        /* Alert Toast */
        .toast {{
            position: fixed;
            top: 24px;
            right: 24px;
            padding: 16px 24px;
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            box-shadow: var(--shadow-lg);
            display: none;
            align-items: center;
            gap: 12px;
            z-index: 1000;
            animation: slideIn 0.3s ease;
        }}

        .toast.show {{
            display: flex;
        }}

        .toast.success {{
            border-color: var(--accent-success);
        }}

        .toast.error {{
            border-color: var(--accent-danger);
        }}

        @keyframes slideIn {{
            from {{
                transform: translateX(100%);
                opacity: 0;
            }}
            to {{
                transform: translateX(0);
                opacity: 1;
            }}
        }}

        /* Responsive */
        @media (max-width: 1200px) {{
            .span-3, .span-4 {{ grid-column: span 6; }}
        }}

        @media (max-width: 768px) {{
            .header {{
                flex-direction: column;
                gap: 20px;
            }}
            .span-3, .span-4, .span-6, .span-8 {{ grid-column: span 12; }}
            .control-panel {{
                width: 100%;
                justify-content: center;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <div class="header">
            <div class="header-left">
                <div class="logo">P</div>
                <div>
                    <div class="header-title">Polymarket Trading Bot</div>
                    <div class="header-subtitle">專業級量化交易監控系統</div>
                </div>
            </div>
            <div class="header-meta">
                <div class="refresh-indicator">
                    <div class="refresh-dot"></div>
                    <span id="refresh-count">5</span>秒後刷新
                </div>
                <div class="control-panel">
                    <button class="btn btn-primary" id="btn-start" onclick="controlBot('start')">
                        <span>▶</span> 啟動
                    </button>
                    <button class="btn btn-danger" id="btn-stop" onclick="controlBot('stop')">
                        <span>⏹</span> 停止
                    </button>
                    <button class="btn btn-secondary" id="btn-restart" onclick="controlBot('restart')">
                        <span>↻</span> 重啟
                    </button>
                </div>
            </div>
        </div>

        <!-- Status Banner -->
        <div id="status-banner" class="status-banner stopped">
            <div class="status-icon" id="status-icon">⏸</div>
            <div class="status-info">
                <div class="status-title" id="status-title">機器人已停止</div>
                <div class="status-detail" id="status-detail">目前沒有運行中的交易進程</div>
            </div>
        </div>

        <!-- Metrics Grid -->
        <div class="grid">
            <!-- Status Card -->
            <div class="card span-3">
                <div class="card-header">
                    <div class="card-title">系統狀態</div>
                </div>
                <div class="card-value metric-neutral" id="metric-status">--</div>
                <div class="card-subtitle">交易機器人運行狀態</div>
            </div>

            <!-- Scanned Card -->
            <div class="card span-3">
                <div class="card-header">
                    <div class="card-title">掃描市場</div>
                </div>
                <div class="card-value metric-neutral" id="metric-scanned">--</div>
                <div class="card-subtitle">本輪掃描的市場數量</div>
            </div>

            <!-- Candidates Card -->
            <div class="card span-3">
                <div class="card-header">
                    <div class="card-title">候選機會</div>
                </div>
                <div class="card-value metric-positive" id="metric-candidates">--</div>
                <div class="card-subtitle">符合條件的交易機會</div>
            </div>

            <!-- Executed Card -->
            <div class="card span-3">
                <div class="card-header">
                    <div class="card-title">執行訂單</div>
                </div>
                <div class="card-value" id="metric-executed">--</div>
                <div class="card-subtitle">成功/失敗: <span id="metric-failed">--</span></div>
            </div>

            <!-- Trend Chart -->
            <div class="card span-8">
                <div class="card-header">
                    <div class="card-title">30分鐘趨勢</div>
                </div>
                <div class="chart-container">
                    <svg class="chart-svg" id="trend-chart">
                        <!-- Chart will be rendered here -->
                    </svg>
                </div>
            </div>

            <!-- Top Reason -->
            <div class="card span-4">
                <div class="card-header">
                    <div class="card-title">主要拒絕原因</div>
                </div>
                <div class="card-value" style="font-size: 24px;" id="metric-top-reason">--</div>
                <div class="card-subtitle">影響 <span id="metric-top-count">--</span> 個市場</div>
                <div class="reason-list" id="reason-list">
                    <!-- Reasons will be populated here -->
                </div>
            </div>

            <!-- Recent Rejects -->
            <div class="card span-6">
                <div class="card-header">
                    <div class="card-title">最近拒絕樣本</div>
                </div>
                <div class="log-container" id="log-candidate">
                    <!-- Log lines will be populated here -->
                </div>
            </div>

            <!-- System Logs -->
            <div class="card span-6">
                <div class="card-header">
                    <div class="card-title">系統日誌</div>
                </div>
                <div class="log-container" id="log-lifecycle">
                    <!-- Log lines will be populated here -->
                </div>
            </div>
        </div>
    </div>

    <!-- Toast Notification -->
    <div id="toast" class="toast">
        <span id="toast-icon">✓</span>
        <span id="toast-message">操作成功</span>
    </div>

    <script>
        const REFRESH_INTERVAL = {refresh_ms};
        let countdown = 5;
        let refreshTimer = null;
        let countdownTimer = null;

        function showToast(message, type = 'success') {{
            const toast = document.getElementById('toast');
            const icon = document.getElementById('toast-icon');
            const msg = document.getElementById('toast-message');
            
            toast.className = 'toast show ' + type;
            icon.textContent = type === 'success' ? '✓' : '✗';
            msg.textContent = message;
            
            setTimeout(() => {{
                toast.classList.remove('show');
            }}, 3000);
        }}

        async function controlBot(action) {{
            const btnStart = document.getElementById('btn-start');
            const btnStop = document.getElementById('btn-stop');
            const btnRestart = document.getElementById('btn-restart');
            
            // Disable all buttons
            [btnStart, btnStop, btnRestart].forEach(btn => btn.disabled = true);
            
            try {{
                const response = await fetch('/api/control', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{ action: action }})
                }});
                
                const data = await response.json();
                
                if (data.success) {{
                    showToast(data.message, 'success');
                }} else {{
                    showToast(data.message, 'error');
                }}
                
                // Refresh status immediately
                await fetchStatus();
            }} catch (error) {{
                showToast('操作失敗: ' + error.message, 'error');
            }} finally {{
                // Re-enable buttons
                [btnStart, btnStop, btnRestart].forEach(btn => btn.disabled = false);
            }}
        }}

        function renderTrendChart(points) {{
            const svg = document.getElementById('trend-chart');
            if (!points || points.length === 0) {{
                svg.innerHTML = '<text x="50%" y="50%" text-anchor="middle" fill="#64748b">暫無趨勢數據</text>';
                return;
            }}

            const width = svg.clientWidth || 600;
            const height = svg.clientHeight || 200;
            const padding = 20;
            
            const candidates = points.map(p => p.candidates || 0);
            const maxVal = Math.max(...candidates, 1);
            
            // Create path
            let pathD = '';
            candidates.forEach((val, i) => {{
                const x = padding + (i / (candidates.length - 1 || 1)) * (width - padding * 2);
                const y = height - padding - (val / maxVal) * (height - padding * 2);
                pathD += (i === 0 ? 'M' : 'L') + x + ',' + y;
            }});

            // Create gradient area
            let areaD = pathD + ` L ${{width - padding}},${{height - padding}} L ${{padding}},${{height - padding}} Z`;

            svg.innerHTML = `
                <defs>
                    <linearGradient id="chartGradient" x1="0%" y1="0%" x2="0%" y2="100%">
                        <stop offset="0%" style="stop-color:#06b6d4;stop-opacity:0.3" />
                        <stop offset="100%" style="stop-color:#06b6d4;stop-opacity:0" />
                    </linearGradient>
                </defs>
                <path d="${{areaD}}" fill="url(#chartGradient)" />
                <path d="${{pathD}}" fill="none" stroke="#06b6d4" stroke-width="2" />
                ${{candidates.map((val, i) => {{
                    const x = padding + (i / (candidates.length - 1 || 1)) * (width - padding * 2);
                    const y = height - padding - (val / maxVal) * (height - padding * 2);
                    return `<circle cx="${{x}}" cy="${{y}}" r="3" fill="#06b6d4" />`;
                }}).join('')}}
            `;
        }}

        async function fetchStatus() {{
            try {{
                const response = await fetch('/api/status');
                const data = await response.json();
                updateUI(data);
            }} catch (error) {{
                console.error('Fetch error:', error);
            }}
        }}

        function updateUI(data) {{
            const bot = data.bot || {{}};
            const cycle = data.latest_cycle || {{}};
            const reasons = data.latest_reasons || {{}};
            const trend = data.trend || {{}};
            const summary = data.summary || {{}};
            const logs = data.logs || {{}};

            // Update status banner
            const banner = document.getElementById('status-banner');
            const icon = document.getElementById('status-icon');
            const title = document.getElementById('status-title');
            const detail = document.getElementById('status-detail');
            
            if (bot.running) {{
                banner.className = 'status-banner running';
                icon.textContent = '▶';
                title.textContent = '機器人運行中';
                const procs = bot.processes || [];
                detail.textContent = procs.length > 0 ? `PID: ${{procs[0].pid}} | 運行時間: ${{procs[0].etime}}` : '交易機器人正在執行';
            }} else {{
                banner.className = 'status-banner stopped';
                icon.textContent = '⏸';
                title.textContent = '機器人已停止';
                detail.textContent = '目前沒有運行中的交易進程';
            }}

            // Update metrics
            document.getElementById('metric-status').textContent = bot.running ? '運行中' : '已停止';
            document.getElementById('metric-status').className = 'card-value ' + (bot.running ? 'metric-positive' : 'metric-negative');
            document.getElementById('metric-scanned').textContent = cycle.scanned || '--';
            document.getElementById('metric-candidates').textContent = cycle.candidates || '--';
            document.getElementById('metric-executed').textContent = (cycle.executed || 0) + '/' + (cycle.selected || 0);
            document.getElementById('metric-executed').className = 'card-value ' + ((cycle.executed || 0) > 0 ? 'metric-positive' : 'metric-neutral');
            document.getElementById('metric-failed').textContent = cycle.failed || '0';

            // Update top reason
            const reasonLabels = {json.dumps(原因名稱, ensure_ascii=False)};
            const topReason = summary.top_reason;
            document.getElementById('metric-top-reason').textContent = topReason ? (reasonLabels[topReason] || topReason) : '無';
            document.getElementById('metric-top-count').textContent = summary.top_reason_count || 0;

            // Update reason list
            const reasonList = document.getElementById('reason-list');
            const reasonEntries = Object.entries(reasons.summary || {{}}).sort((a, b) => b[1] - a[1]).slice(0, 5);
            reasonList.innerHTML = reasonEntries.map(([key, count]) => `
                <div class="reason-pill">
                    ${{reasonLabels[key] || key}}
                    <span class="count">${{count}}</span>
                </div>
            `).join('') || '<div class="reason-pill">暫無拒絕數據</div>';

            // Update trend chart
            renderTrendChart(trend.points || []);

            // Update logs
            const reasonNames = {json.dumps(原因名稱, ensure_ascii=False)};
            
            const candidateLog = document.getElementById('log-candidate');
            candidateLog.innerHTML = (logs.candidate || []).slice(0, 20).map(line => {{
                // Extract reason from log line
                let reasonText = line;
                if (line.includes('reason\': \'')) {{
                    const match = line.match(/reason\':\s*'([^']+)'/);
                    if (match) {{
                        const reason = match[1];
                        const label = reasonNames[reason] || reason;
                        reasonText = line.replace(reason, label);
                    }}
                }}
                return `<div class="log-line">${{esc(reasonText)}}</div>`;
            }}).join('') || '<div class="log-line">暫無數據</div>';

            const lifecycleLog = document.getElementById('log-lifecycle');
            lifecycleLog.innerHTML = (logs.lifecycle || []).slice(0, 20).map(line => 
                `<div class="log-line">${{esc(line)}}</div>`
            ).join('') || '<div class="log-line">暫無數據</div>';
        }}

        function esc(str) {{
            return String(str || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        }}

        function startCountdown() {{
            countdown = 5;
            document.getElementById('refresh-count').textContent = countdown;
            
            if (countdownTimer) clearInterval(countdownTimer);
            countdownTimer = setInterval(() => {{
                countdown--;
                if (countdown <= 0) countdown = 5;
                document.getElementById('refresh-count').textContent = countdown;
            }}, 1000);
        }}

        function scheduleRefresh() {{
            if (refreshTimer) clearTimeout(refreshTimer);
            refreshTimer = setTimeout(() => {{
                fetchStatus();
                startCountdown();
                scheduleRefresh();
            }}, REFRESH_INTERVAL);
        }}

        // Initialize
        document.addEventListener('DOMContentLoaded', () => {{
            fetchStatus();
            startCountdown();
            scheduleRefresh();
        }});
    </script>
</body>
</html>"""


class ThreadingHTTPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """Threading HTTP server."""
    allow_reuse_address = True
    daemon_threads = True


def create_handler(config: MonitorConfig):
    """Create HTTP handler class."""

    from http.server import BaseHTTPRequestHandler

    class MonitorHandler(BaseHTTPRequestHandler):
        def _send_json(self, data: Dict[str, Any], status: int = 200):
            body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, html: str):
            body = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_OPTIONS(self):
            self.send_response(200)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()

        def do_GET(self):
            path = urlparse(self.path).path
            if path == "/":
                self._send_html(_render_html(config))
            elif path == "/api/status":
                self._send_json(_build_status_payload(config))
            else:
                self._send_json({"error": "not_found"}, 404)

        def do_POST(self):
            path = urlparse(self.path).path
            if path == "/api/control":
                content_length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_length).decode("utf-8")
                try:
                    data = json.loads(body)
                    action = data.get("action")
                    if action == "start":
                        result = bot_controller.start()
                    elif action == "stop":
                        result = bot_controller.stop()
                    elif action == "restart":
                        result = bot_controller.restart()
                    else:
                        self._send_json({"success": False, "message": "未知操作"}, 400)
                        return
                    self._send_json(result)
                except Exception as e:
                    self._send_json({"success": False, "message": str(e)}, 500)
            else:
                self._send_json({"error": "not_found"}, 404)

        def log_message(self, format, *args):
            return

    return MonitorHandler


def serve_monitor(host: str, port: int, config: MonitorConfig) -> None:
    """Start monitoring service."""
    server = ThreadingHTTPServer((host, port), create_handler(config))
    print(f"監控台啟動: http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n監控台已停止")
        server.shutdown()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--log-dir", default="./logs")
    parser.add_argument("--db-path", default="./research_signals.db")
    args = parser.parse_args()
    
    config = MonitorConfig(
        db_path=Path(args.db_path),
        log_dir=Path(args.log_dir),
        refresh_seconds=5,
    )
    serve_monitor(args.host, args.port, config)
