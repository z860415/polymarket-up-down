"""
Package D: Signal Logger
Observation fact store for calibration and analysis.

Design principle: D = append-only observation fact store
E = calibration / reporting (separate package)
"""

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from decimal import Decimal
import json
import sqlite3
import uuid
from typing import Any, Dict, List, Optional, Tuple

from polymarket_arbitrage.market_definition import (
    MarketDefinition,
    PayoffType,
    ResolutionOperator,
    SettlementRule,
    StrikeType,
    OracleFamily,
)
from polymarket_arbitrage.reference_builder import (
    ReferencePrice,
    ReferenceStatus,
    ReferenceMethod,
)
from polymarket_arbitrage.fair_prob_model import FairProbEstimate, WarningCode


# ============================================================================
# Exceptions
# ============================================================================

class InvalidObservationError(ValueError):
    """Observation data is invalid or incomplete."""
    pass


class SettlementUpdateError(ValueError):
    """Failed to update settlement outcome."""
    pass


# ============================================================================
# Signal Observation Dataclass
# ============================================================================

@dataclass(frozen=True)
class SignalObservation:
    """
    Single signal observation record (fact table row).
    
    Stores A/B/C summary + market book bid/ask + outcome backfill.
    No derived analysis results (Brier, decile, etc. - those go to Package E).
    """
    
    # Identity
    observation_id: str
    market_id: str
    timestamp: datetime  # as_of time, timezone-aware UTC
    
    # Market semantics (from Package A - key fields only)
    asset: str
    market_style: Optional[str]
    payoff_type: str
    resolution_operator: str
    strike_type: str
    settlement_rule: str
    anchor_price: Optional[float]
    anchor_timestamp: Optional[datetime]
    lead_z: Optional[float]
    sigma_tail: Optional[float]
    window_state: Optional[str]
    net_edge_selected: Optional[float]
    
    # Reference Price (from Package B)
    reference_price_value: Optional[float]
    reference_quality_score: float
    reference_status: str
    reference_source: str
    reference_method: str
    reference_symbol: Optional[str]  # Added v0.2: specific symbol e.g., BTCUSDT
    
    # Fair Probability (from Package C)
    p_yes: float
    p_no: float
    model_confidence_score: float
    spot_price: float
    strike_price: float
    volatility: float
    time_to_expiry_sec: float
    model_version: str
    
    # Market Book (split bid/ask)
    yes_bid: Optional[float]
    yes_ask: Optional[float]
    no_bid: Optional[float]
    no_ask: Optional[float]
    mid_yes: Optional[float]  # (bid+ask)/2, only if both present
    mid_no: Optional[float]
    
    # Edge (multi-dimensional)
    edge_vs_yes_ask: Optional[float]   # p_yes - yes_ask
    edge_vs_yes_bid: Optional[float]   # p_yes - yes_bid
    edge_vs_mid_yes: Optional[float]   # p_yes - mid_yes
    
    # Settlement Outcome (backfilled)
    settlement_outcome: Optional[bool]  # True=Yes, False=No
    settlement_timestamp: Optional[datetime]
    
    # Metadata
    warning_flags: List[str]  # WarningCode values as strings
    model_assumptions_json: Optional[str] = None
    
    # Debug (optional raw data)
    market_definition_json: Optional[str] = None


# ============================================================================
# Signal Logger
# ============================================================================

class SignalLogger:
    """
    Signal observation logger with memory buffer and SQLite persistence.
    
    Design: D = observation fact store only
    Analysis (Brier, decile, edge groups) belongs to Package E.
    """
    
    def __init__(
        self,
        db_path: Optional[str] = None,    # SQLite path, None = memory only
        max_memory_size: int = 10000,     # Memory buffer limit
    ):
        self.db_path = db_path
        self._memory_buffer: List[SignalObservation] = []
        self._max_memory = max_memory_size
        
        if db_path:
            self._init_database()
    
    def _init_database(self):
        """Initialize SQLite database with observation schema."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Main observations table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS observations (
                -- Identity
                observation_id TEXT PRIMARY KEY,
                market_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,  -- ISO8601 UTC
                
                -- Market semantics
                asset TEXT NOT NULL,
                market_style TEXT,
                payoff_type TEXT NOT NULL,
                resolution_operator TEXT,
                strike_type TEXT NOT NULL,
                settlement_rule TEXT NOT NULL,
                anchor_price REAL,
                anchor_timestamp TEXT,
                lead_z REAL,
                sigma_tail REAL,
                window_state TEXT,
                net_edge_selected REAL,
                
                -- Reference
                reference_price_value REAL,
                reference_quality_score REAL NOT NULL,
                reference_status TEXT NOT NULL,
                reference_source TEXT NOT NULL,
                reference_method TEXT NOT NULL,
                reference_symbol TEXT,
                
                -- Model
                p_yes REAL NOT NULL,
                p_no REAL NOT NULL,
                model_confidence_score REAL NOT NULL,
                spot_price REAL NOT NULL,
                strike_price REAL NOT NULL,
                volatility REAL NOT NULL,
                time_to_expiry_sec REAL NOT NULL,
                model_version TEXT NOT NULL,
                
                -- Market Book
                yes_bid REAL,
                yes_ask REAL,
                no_bid REAL,
                no_ask REAL,
                mid_yes REAL,
                mid_no REAL,
                
                -- Edge
                edge_vs_yes_ask REAL,
                edge_vs_yes_bid REAL,
                edge_vs_mid_yes REAL,
                
                -- Settlement (backfilled)
                settlement_outcome INTEGER,  -- NULL/0/1
                settlement_timestamp TEXT,   -- ISO8601 UTC
                
                -- Metadata
                warning_flags TEXT,  -- JSON array
                model_assumptions_json TEXT,  -- FairProbEstimate.assumptions
                market_definition_json TEXT  -- Optional raw data
            )
        ''')
        
        # Indexes
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_obs_market_id ON observations(market_id)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_obs_timestamp ON observations(timestamp)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_obs_settlement 
            ON observations(settlement_outcome) WHERE settlement_outcome IS NOT NULL
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_obs_model_version ON observations(model_version)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_obs_model_time ON observations(model_version, timestamp)
        ''')
        
        self._ensure_column_exists(cursor, "observations", "market_style", "TEXT")
        self._ensure_column_exists(cursor, "observations", "anchor_price", "REAL")
        self._ensure_column_exists(cursor, "observations", "anchor_timestamp", "TEXT")
        self._ensure_column_exists(cursor, "observations", "lead_z", "REAL")
        self._ensure_column_exists(cursor, "observations", "sigma_tail", "REAL")
        self._ensure_column_exists(cursor, "observations", "window_state", "TEXT")
        self._ensure_column_exists(cursor, "observations", "net_edge_selected", "REAL")
        
        conn.commit()
        conn.close()

    @staticmethod
    def _ensure_column_exists(cursor: sqlite3.Cursor, table_name: str, column_name: str, column_type: str) -> None:
        """確保既有資料表也能補上新欄位。"""
        cursor.execute(f"PRAGMA table_info({table_name})")
        existing_columns = {row[1] for row in cursor.fetchall()}
        if column_name not in existing_columns:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
    
    def log_signal(
        self,
        market_def: MarketDefinition,
        ref_price: ReferencePrice,
        fair_prob: FairProbEstimate,
        yes_bid: Optional[float],
        yes_ask: Optional[float],
        no_bid: Optional[float],
        no_ask: Optional[float],
        as_of: datetime,
        market_style: Optional[str] = None,
        anchor_price: Optional[float] = None,
        anchor_timestamp: Optional[datetime] = None,
        lead_z: Optional[float] = None,
        sigma_tail: Optional[float] = None,
        window_state: Optional[str] = None,
        net_edge_selected: Optional[float] = None,
    ) -> SignalObservation:
        """
        Log a complete signal observation.
        
        Automatically calculates:
        - mid_yes = (yes_bid + yes_ask) / 2 (only if both present)
        - mid_no = (no_bid + no_ask) / 2 (only if both present)
        - edge_vs_yes_ask = fair_prob.p_yes - yes_ask (if yes_ask not None)
        - edge_vs_yes_bid = fair_prob.p_yes - yes_bid (if yes_bid not None)
        - edge_vs_mid_yes = fair_prob.p_yes - mid_yes (if mid_yes not None)
        
        Args:
            as_of: Unified time baseline (must be timezone-aware UTC)
        
        Raises:
            InvalidObservationError: If required fields are missing.
        """
        # Ensure UTC timezone
        as_of = self._ensure_utc_datetime(as_of)
        
        # Generate observation ID
        observation_id = str(uuid.uuid4())
        
        # Calculate mid prices (only if both bid and ask present)
        mid_yes = (yes_bid + yes_ask) / 2.0 if yes_bid is not None and yes_ask is not None else None
        mid_no = (no_bid + no_ask) / 2.0 if no_bid is not None and no_ask is not None else None
        
        # Calculate edge (only if corresponding price available)
        edge_vs_yes_ask = fair_prob.p_yes - yes_ask if yes_ask is not None else None
        edge_vs_yes_bid = fair_prob.p_yes - yes_bid if yes_bid is not None else None
        edge_vs_mid_yes = fair_prob.p_yes - mid_yes if mid_yes is not None else None
        
        # Convert warning flags to strings
        warning_flags = [w.value if isinstance(w, WarningCode) else str(w) for w in fair_prob.warning_flags]
        
        # Serialize market definition for debug (optional)
        market_def_json = self._serialize_market_definition(market_def)
        
        # Serialize model assumptions for calibration analysis
        assumptions_json = json.dumps(fair_prob.assumptions) if fair_prob.assumptions else None
        
        # Create observation
        observation = SignalObservation(
            observation_id=observation_id,
            market_id=market_def.market_id,
            timestamp=as_of,
            
            # Market semantics
            asset=market_def.asset,
            market_style=market_style or market_def.market_style,
            payoff_type=market_def.payoff_type.value,
            resolution_operator=market_def.resolution_operator.value,
            strike_type=market_def.strike_type.value,
            settlement_rule=market_def.settlement_rule.value,
            anchor_price=anchor_price,
            anchor_timestamp=self._ensure_utc_datetime(anchor_timestamp) if anchor_timestamp else None,
            lead_z=lead_z,
            sigma_tail=sigma_tail,
            window_state=window_state,
            net_edge_selected=net_edge_selected,
            
            # Reference
            reference_price_value=ref_price.value,
            reference_quality_score=ref_price.quality_score,
            reference_status=ref_price.status.value,
            reference_source=ref_price.source.value,
            reference_method=ref_price.method.value,
            reference_symbol=ref_price.symbol,
            
            # Model
            p_yes=fair_prob.p_yes,
            p_no=fair_prob.p_no,
            model_confidence_score=fair_prob.model_confidence_score,
            spot_price=fair_prob.spot_price,
            strike_price=fair_prob.strike_price,
            volatility=fair_prob.volatility,
            time_to_expiry_sec=fair_prob.time_to_expiry_sec,
            model_version=fair_prob.model_version,
            
            # Market Book
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            no_bid=no_bid,
            no_ask=no_ask,
            mid_yes=mid_yes,
            mid_no=mid_no,
            
            # Edge
            edge_vs_yes_ask=edge_vs_yes_ask,
            edge_vs_yes_bid=edge_vs_yes_bid,
            edge_vs_mid_yes=edge_vs_mid_yes,
            
            # Settlement (empty on creation)
            settlement_outcome=None,
            settlement_timestamp=None,
            
            # Metadata
            warning_flags=warning_flags,
            model_assumptions_json=assumptions_json,
            market_definition_json=market_def_json,
        )
        
        # Add to memory buffer
        self._memory_buffer.append(observation)
        
        # Flush to SQLite if buffer exceeds limit
        if len(self._memory_buffer) > self._max_memory:
            self._flush_to_database()
        
        return observation
    
    def record_settlement(
        self,
        observation_id: str,
        outcome: bool,                    # True=Yes, False=No
        settlement_ts: datetime,
        allow_overwrite: bool = False,    # Protect against accidental rewrites
    ):
        """
        Backfill settlement outcome for an observation.
        
        Args:
            outcome: True if Yes token settled, False if No token settled
            settlement_ts: Timestamp of settlement
            allow_overwrite: If False, raises error when observation already has settlement
        
        Raises:
            SettlementUpdateError: If observation not found, already settled, or update fails.
        """
        settlement_ts = self._ensure_utc_datetime(settlement_ts)
        
        # Check if already settled (in memory buffer)
        for obs in self._memory_buffer:
            if obs.observation_id == observation_id:
                if obs.settlement_outcome is not None and not allow_overwrite:
                    raise SettlementUpdateError(
                        f"Observation {observation_id} already has settlement outcome. "
                        f"Use allow_overwrite=True to force update."
                    )
                break
        
        if self.db_path:
            # Check if already settled (in database)
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            if not allow_overwrite:
                cursor.execute('''
                    SELECT settlement_outcome FROM observations WHERE observation_id = ?
                ''', (observation_id,))
                row = cursor.fetchone()
                if row and row[0] is not None:
                    conn.close()
                    raise SettlementUpdateError(
                        f"Observation {observation_id} already has settlement outcome. "
                        f"Use allow_overwrite=True to force update."
                    )
            
            # Update in database
            cursor.execute('''
                UPDATE observations
                SET settlement_outcome = ?,
                    settlement_timestamp = ?
                WHERE observation_id = ?
            ''', (1 if outcome else 0, settlement_ts.isoformat(), observation_id))
            
            if cursor.rowcount == 0:
                conn.close()
                raise SettlementUpdateError(f"Observation {observation_id} not found")
            
            conn.commit()
            conn.close()
        
        # Also update in memory buffer if present
        for i, obs in enumerate(self._memory_buffer):
            if obs.observation_id == observation_id:
                # Create updated observation (immutable, so replace)
                updated_obs = SignalObservation(
                    **{**obs.__dict__, 'settlement_outcome': outcome, 'settlement_timestamp': settlement_ts}
                )
                self._memory_buffer[i] = updated_obs
                break
    
    def get_observations(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        market_id: Optional[str] = None,
        min_confidence: float = 0.0,
        include_unsettled: bool = True,
    ) -> List[SignalObservation]:
        """
        Query historical observations with filters.
        
        Returns observations from both memory buffer and database.
        """
        results = []
        
        # Ensure timezone-aware datetimes
        if start_time:
            start_time = self._ensure_utc_datetime(start_time)
        if end_time:
            end_time = self._ensure_utc_datetime(end_time)
        
        # Query from memory buffer
        for obs in self._memory_buffer:
            if self._matches_filters(obs, start_time, end_time, market_id, min_confidence, include_unsettled):
                results.append(obs)
        
        # Query from database if exists
        if self.db_path:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Build query
            conditions = ["model_confidence_score >= ?"]
            params = [min_confidence]
            
            if start_time:
                conditions.append("timestamp >= ?")
                params.append(start_time.isoformat())
            if end_time:
                conditions.append("timestamp <= ?")
                params.append(end_time.isoformat())
            if market_id:
                conditions.append("market_id = ?")
                params.append(market_id)
            if not include_unsettled:
                conditions.append("settlement_outcome IS NOT NULL")
            
            query = f"SELECT * FROM observations WHERE {' AND '.join(conditions)}"
            cursor.execute(query, params)
            
            # Parse rows
            columns = [desc[0] for desc in cursor.description]
            for row in cursor.fetchall():
                row_dict = dict(zip(columns, row))
                obs = self._row_to_observation(row_dict)
                # Avoid duplicates from memory buffer
                if obs.observation_id not in [r.observation_id for r in results]:
                    results.append(obs)
            
            conn.close()
        
        # Sort by timestamp
        results.sort(key=lambda x: x.timestamp)
        return results
    
    def get_settled_observations(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        market_id: Optional[str] = None,
        min_confidence: float = 0.0,
    ) -> List[SignalObservation]:
        """Get only observations with known settlement outcomes."""
        return self.get_observations(
            start_time=start_time,
            end_time=end_time,
            market_id=market_id,
            min_confidence=min_confidence,
            include_unsettled=False,
        )
    
    def flush(self):
        """Manually flush memory buffer to database."""
        if self.db_path:
            self._flush_to_database()
    
    def _flush_to_database(self):
        """Flush memory buffer to SQLite."""
        if not self.db_path or not self._memory_buffer:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for obs in self._memory_buffer:
            row = self._observation_to_row(obs)
            cursor.execute(
                '''
                INSERT OR REPLACE INTO observations (
                    observation_id,
                    market_id,
                    timestamp,
                    asset,
                    market_style,
                    payoff_type,
                    resolution_operator,
                    strike_type,
                    settlement_rule,
                    anchor_price,
                    anchor_timestamp,
                    lead_z,
                    sigma_tail,
                    window_state,
                    net_edge_selected,
                    reference_price_value,
                    reference_quality_score,
                    reference_status,
                    reference_source,
                    reference_method,
                    reference_symbol,
                    p_yes,
                    p_no,
                    model_confidence_score,
                    spot_price,
                    strike_price,
                    volatility,
                    time_to_expiry_sec,
                    model_version,
                    yes_bid,
                    yes_ask,
                    no_bid,
                    no_ask,
                    mid_yes,
                    mid_no,
                    edge_vs_yes_ask,
                    edge_vs_yes_bid,
                    edge_vs_mid_yes,
                    settlement_outcome,
                    settlement_timestamp,
                    warning_flags,
                    model_assumptions_json,
                    market_definition_json
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?
                )
                ''',
                row,
            )
        
        conn.commit()
        conn.close()
        
        # Clear memory buffer
        self._memory_buffer = []
    
    def _matches_filters(
        self,
        obs: SignalObservation,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        market_id: Optional[str],
        min_confidence: float,
        include_unsettled: bool,
    ) -> bool:
        """Check if observation matches query filters."""
        if start_time and obs.timestamp < start_time:
            return False
        if end_time and obs.timestamp > end_time:
            return False
        if market_id and obs.market_id != market_id:
            return False
        if obs.model_confidence_score < min_confidence:
            return False
        if not include_unsettled and obs.settlement_outcome is None:
            return False
        return True
    
    def _observation_to_row(self, obs: SignalObservation) -> Tuple:
        """Convert observation to database row tuple."""
        return (
            obs.observation_id,
            obs.market_id,
            obs.timestamp.isoformat(),
            obs.asset,
            obs.market_style,
            obs.payoff_type,
            obs.resolution_operator,
            obs.strike_type,
            obs.settlement_rule,
            obs.anchor_price,
            obs.anchor_timestamp.isoformat() if obs.anchor_timestamp else None,
            obs.lead_z,
            obs.sigma_tail,
            obs.window_state,
            obs.net_edge_selected,
            obs.reference_price_value,
            obs.reference_quality_score,
            obs.reference_status,
            obs.reference_source,
            obs.reference_method,
            obs.reference_symbol,
            obs.p_yes,
            obs.p_no,
            obs.model_confidence_score,
            obs.spot_price,
            obs.strike_price,
            obs.volatility,
            obs.time_to_expiry_sec,
            obs.model_version,
            obs.yes_bid,
            obs.yes_ask,
            obs.no_bid,
            obs.no_ask,
            obs.mid_yes,
            obs.mid_no,
            obs.edge_vs_yes_ask,
            obs.edge_vs_yes_bid,
            obs.edge_vs_mid_yes,
            1 if obs.settlement_outcome is True else (0 if obs.settlement_outcome is False else None),
            obs.settlement_timestamp.isoformat() if obs.settlement_timestamp else None,
            json.dumps(obs.warning_flags),
            obs.model_assumptions_json,
            obs.market_definition_json,
        )
    
    def _row_to_observation(self, row: Dict[str, Any]) -> SignalObservation:
        """Convert database row to observation."""
        # Parse timestamps
        timestamp = datetime.fromisoformat(row['timestamp'])
        settlement_ts = datetime.fromisoformat(row['settlement_timestamp']) if row['settlement_timestamp'] else None
        
        # Parse settlement outcome
        settlement_outcome = bool(row['settlement_outcome']) if row['settlement_outcome'] is not None else None
        
        # Parse warning flags
        warning_flags = json.loads(row['warning_flags']) if row['warning_flags'] else []
        
        return SignalObservation(
            observation_id=row['observation_id'],
            market_id=row['market_id'],
            timestamp=timestamp,
            asset=row['asset'],
            market_style=row.get('market_style'),
            payoff_type=row['payoff_type'],
            resolution_operator=row['resolution_operator'],
            strike_type=row['strike_type'],
            settlement_rule=row['settlement_rule'],
            anchor_price=row.get('anchor_price'),
            anchor_timestamp=datetime.fromisoformat(row['anchor_timestamp']) if row.get('anchor_timestamp') else None,
            lead_z=row.get('lead_z'),
            sigma_tail=row.get('sigma_tail'),
            window_state=row.get('window_state'),
            net_edge_selected=row.get('net_edge_selected'),
            reference_price_value=row['reference_price_value'],
            reference_quality_score=row['reference_quality_score'],
            reference_status=row['reference_status'],
            reference_source=row['reference_source'],
            reference_method=row['reference_method'],
            reference_symbol=row['reference_symbol'],
            p_yes=row['p_yes'],
            p_no=row['p_no'],
            model_confidence_score=row['model_confidence_score'],
            spot_price=row['spot_price'],
            strike_price=row['strike_price'],
            volatility=row['volatility'],
            time_to_expiry_sec=row['time_to_expiry_sec'],
            model_version=row['model_version'],
            yes_bid=row['yes_bid'],
            yes_ask=row['yes_ask'],
            no_bid=row['no_bid'],
            no_ask=row['no_ask'],
            mid_yes=row['mid_yes'],
            mid_no=row['mid_no'],
            edge_vs_yes_ask=row['edge_vs_yes_ask'],
            edge_vs_yes_bid=row['edge_vs_yes_bid'],
            edge_vs_mid_yes=row['edge_vs_mid_yes'],
            settlement_outcome=settlement_outcome,
            settlement_timestamp=settlement_ts,
            warning_flags=warning_flags,
            model_assumptions_json=row['model_assumptions_json'],
            market_definition_json=row['market_definition_json'],
        )
    
    @staticmethod
    def _ensure_utc_datetime(dt: datetime) -> datetime:
        """Ensure datetime is timezone-aware UTC."""
        if dt.tzinfo is None:
            raise InvalidObservationError(f"datetime must be timezone-aware, got naive: {dt}")
        return dt.astimezone(timezone.utc)
    
    @staticmethod
    def _serialize_market_definition(market_def: MarketDefinition) -> str:
        """Serialize market definition to JSON for debug storage."""
        from polymarket_arbitrage.market_definition import market_definition_to_dict
        try:
            d = market_definition_to_dict(market_def)
            return json.dumps(d)
        except Exception:
            return None
