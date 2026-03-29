"""
UP / DOWN 開盤錨點儲存與抓取。
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import requests

from .market_definition import MarketDefinition, OracleFamily


@dataclass(frozen=True)
class SettlementSourceDescriptor:
    """結算來源描述。"""

    family: OracleFamily
    symbol: str
    raw_source: str
    supported: bool


@dataclass(frozen=True)
class OpeningAnchorRecord:
    """單一市場的開盤錨點記錄。"""

    market_id: str
    asset: str
    timeframe: str
    anchor_timestamp: datetime
    anchor_price: float
    source: str
    source_trade_id: Optional[str]
    quality_score: float
    captured_at: datetime


class SettlementSourceResolver:
    """把市場定義轉成可執行的結算來源設定。"""

    def resolve(
        self, market_def: MarketDefinition
    ) -> Optional[SettlementSourceDescriptor]:
        """解析市場的結算來源。"""
        if not market_def.settlement_source_descriptor:
            return None
        return SettlementSourceDescriptor(
            family=market_def.oracle_family,
            symbol=market_def.oracle_symbol,
            raw_source=market_def.settlement_source_descriptor,
            supported=market_def.oracle_family == OracleFamily.BINANCE,
        )


class OpeningAnchorStore:
    """抓取並持久化 UP / DOWN 市場的開盤錨點。"""

    _BINANCE_INTERVALS = {
        "1m": "1m",
        "5m": "5m",
        "15m": "15m",
        "1h": "1h",
        "4h": "4h",
        "12h": "12h",
        "1d": "1d",
    }

    def __init__(self, db_path: Optional[str] = None) -> None:
        self.db_path = db_path
        self.session = requests.Session()
        self.resolver = SettlementSourceResolver()
        if db_path:
            self._init_database()

    def _init_database(self) -> None:
        """初始化錨點資料表。"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS opening_anchors (
                market_id TEXT PRIMARY KEY,
                asset TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                anchor_timestamp TEXT NOT NULL,
                anchor_price REAL NOT NULL,
                source TEXT NOT NULL,
                source_trade_id TEXT,
                quality_score REAL NOT NULL,
                captured_at TEXT NOT NULL
            )
            """
        )
        conn.commit()
        conn.close()

    def get_anchor(self, market_id: str) -> Optional[OpeningAnchorRecord]:
        """從 SQLite 取得既有錨點。"""
        if not self.db_path:
            return None
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT market_id, asset, timeframe, anchor_timestamp, anchor_price,
                   source, source_trade_id, quality_score, captured_at
            FROM opening_anchors
            WHERE market_id = ?
            """,
            (market_id,),
        )
        row = cursor.fetchone()
        conn.close()
        if row is None:
            return None
        return OpeningAnchorRecord(
            market_id=row[0],
            asset=row[1],
            timeframe=row[2],
            anchor_timestamp=datetime.fromisoformat(row[3]),
            anchor_price=float(row[4]),
            source=row[5],
            source_trade_id=row[6],
            quality_score=float(row[7]),
            captured_at=datetime.fromisoformat(row[8]),
        )

    def capture_anchor(
        self, market_def: MarketDefinition
    ) -> Optional[OpeningAnchorRecord]:
        """依市場設定抓取開盤錨點。"""
        try:
            cached = self.get_anchor(market_def.market_id)
            if cached is not None:
                return cached

            if not market_def.anchor_required:
                return None
            if market_def.market_start_timestamp is None or not market_def.timeframe:
                return None

            source_descriptor = self.resolver.resolve(market_def)
            if source_descriptor is None or not source_descriptor.supported:
                return None

            interval = self._BINANCE_INTERVALS.get(market_def.timeframe)
            if interval is None:
                return None

            open_time_ms = int(market_def.market_start_timestamp.timestamp() * 1000)
            response = self.session.get(
                "https://api.binance.com/api/v3/klines",
                params={
                    "symbol": source_descriptor.symbol,
                    "interval": interval,
                    "startTime": open_time_ms,
                    "limit": 1,
                },
                timeout=5,
            )
            response.raise_for_status()
            payload = response.json()
            if not payload:
                return None

            first_kline = payload[0]
            anchor_timestamp = datetime.fromtimestamp(
                int(first_kline[0]) / 1000, tz=timezone.utc
            )
            record = OpeningAnchorRecord(
                market_id=market_def.market_id,
                asset=market_def.asset,
                timeframe=market_def.timeframe,
                anchor_timestamp=anchor_timestamp,
                anchor_price=float(first_kline[1]),
                source=source_descriptor.raw_source,
                source_trade_id=str(first_kline[0]),
                quality_score=0.95
                if anchor_timestamp == market_def.market_start_timestamp
                else 0.80,
                captured_at=datetime.now(timezone.utc),
            )
            self._persist(record)
            return record
        except Exception:
            return None

    def _persist(self, record: OpeningAnchorRecord) -> None:
        """將錨點寫入 SQLite。"""
        if not self.db_path:
            return
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO opening_anchors (
                market_id,
                asset,
                timeframe,
                anchor_timestamp,
                anchor_price,
                source,
                source_trade_id,
                quality_score,
                captured_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.market_id,
                record.asset,
                record.timeframe,
                record.anchor_timestamp.isoformat(),
                record.anchor_price,
                record.source,
                record.source_trade_id,
                record.quality_score,
                record.captured_at.isoformat(),
            ),
        )
        conn.commit()
        conn.close()
