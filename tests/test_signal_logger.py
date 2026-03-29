"""
Package D Unit Tests: Signal Logger
14 tests covering observation logging, settlement backfill, and SQLite persistence.
"""

from datetime import datetime, timedelta, timezone
import os
import tempfile

from polymarket_arbitrage.signal_logger import (
    SignalLogger,
    SignalObservation,
    InvalidObservationError,
    SettlementUpdateError,
)
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
    ReferenceMethod,
    ReferenceStatus,
)
from polymarket_arbitrage.fair_prob_model import FairProbEstimate, WarningCode


# ============================================================================
# Test Fixtures
# ============================================================================

def create_utc_datetime(year, month, day, hour, minute, second):
    """Helper to create timezone-aware UTC datetime."""
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


def create_market_definition():
    """Helper to create a market definition for testing."""
    return MarketDefinition(
        market_id="test-market-001",
        asset="BTC",
        payoff_type=PayoffType.DIGITAL_ABOVE,
        resolution_operator=ResolutionOperator.GT,
        strike_type=StrikeType.OPEN_PRICE,
        strike_value=None,
        upper_strike_value=None,
        strike_timestamp=create_utc_datetime(2026, 3, 28, 12, 0, 0),
        strike_window_seconds=None,
        expiry_timestamp=create_utc_datetime(2026, 3, 28, 12, 5, 0),
        settlement_rule=SettlementRule.TERMINAL_PRICE,
        oracle_family=OracleFamily.BINANCE,
        oracle_symbol="BTCUSDT",
        oracle_decimals=2,
        fallback_oracle_family=None,
        fallback_oracle_symbol=None,
        fallback_oracle_decimals=None,
        fee_enabled=True,
        yes_token_id="yes-token-001",
        no_token_id="no-token-001",
        raw_question="Will BTC go above $65000?",
        raw_description=None,
    )


def create_reference_price():
    """Helper to create a reference price for testing."""
    target = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    return ReferencePrice(
        value=65000.0,
        source=OracleFamily.BINANCE,
        symbol="BTCUSDT",
        method=ReferenceMethod.WINDOW_FIRST_TICK,
        status=ReferenceStatus.FINALIZED,
        target_timestamp=target,
        source_timestamp=target,
        left_timestamp=None,
        right_timestamp=None,
        window_start=target - timedelta(seconds=10),
        window_end=target + timedelta(seconds=10),
        num_ticks_in_window=5,
        num_ticks_total=10,
        quality_score=0.95,
        quality_components={},
        warnings=[],
        prefer_method=ReferenceMethod.WINDOW_FIRST_TICK,
        allow_interpolation=True,
    )


def create_fair_prob_estimate():
    """Helper to create a fair probability estimate for testing."""
    return FairProbEstimate(
        p_yes=0.75,
        p_no=0.25,
        fair_yes_price=0.75,
        fair_no_price=0.25,
        model_version="lognormal_terminal_exceedance_v0.2",
        assumptions={},
        model_confidence_score=0.85,
        input_quality_score=0.95,
        input_freshness_ms=100.0,
        strike_price=65000.0,
        spot_price=66000.0,
        time_to_expiry_sec=300.0,
        volatility=0.5,
        drift=0.0,
        warning_flags=[],
    )


# ============================================================================
# Test Cases 1-14
# ============================================================================

def test_log_signal_generates_observation_id():
    """Test 1: log_signal generates unique observation_id"""
    logger = SignalLogger(db_path=None)  # Memory only
    market = create_market_definition()
    ref_price = create_reference_price()
    fair_prob = create_fair_prob_estimate()
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    obs = logger.log_signal(
        market, ref_price, fair_prob,
        yes_bid=0.70, yes_ask=0.72,
        no_bid=0.28, no_ask=0.30,
        as_of=as_of
    )
    
    assert obs.observation_id is not None
    assert len(obs.observation_id) > 0
    assert obs.market_id == "test-market-001"
    assert obs.market_style == "ABOVE_BELOW"
    print(f"✅ Test 1: observation_id generated: {obs.observation_id[:8]}...")


def test_log_signal_persists_tail_fields():
    """Test 1b: tail fields persist to observation"""
    logger = SignalLogger(db_path=None)
    market = create_market_definition()
    ref_price = create_reference_price()
    fair_prob = create_fair_prob_estimate()
    as_of = create_utc_datetime(2026, 3, 28, 12, 4, 30)

    obs = logger.log_signal(
        market,
        ref_price,
        fair_prob,
        yes_bid=0.70,
        yes_ask=0.72,
        no_bid=0.28,
        no_ask=0.30,
        as_of=as_of,
        market_style="UP_DOWN",
        anchor_price=64950.0,
        anchor_timestamp=create_utc_datetime(2026, 3, 28, 12, 0, 0),
        lead_z=2.4,
        sigma_tail=0.42,
        window_state="attack",
        net_edge_selected=0.061,
    )

    assert obs.market_style == "UP_DOWN"
    assert obs.anchor_price == 64950.0
    assert obs.lead_z == 2.4
    assert obs.sigma_tail == 0.42
    assert obs.window_state == "attack"
    assert obs.net_edge_selected == 0.061
    print("✅ Test 1b: tail fields persisted")


def test_mid_yes_calculation():
    """Test 2: mid_yes = (bid + ask) / 2 when both present"""
    logger = SignalLogger(db_path=None)
    market = create_market_definition()
    ref_price = create_reference_price()
    fair_prob = create_fair_prob_estimate()
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    obs = logger.log_signal(
        market, ref_price, fair_prob,
        yes_bid=0.70, yes_ask=0.72,
        no_bid=0.28, no_ask=0.30,
        as_of=as_of
    )
    
    expected_mid = (0.70 + 0.72) / 2.0
    assert obs.mid_yes == expected_mid
    assert obs.mid_no == (0.28 + 0.30) / 2.0
    print(f"✅ Test 2: mid_yes={obs.mid_yes} (expected {expected_mid})")


def test_mid_yes_none_when_single_sided():
    """Test 3: mid_yes is None when single-sided (only ask)"""
    logger = SignalLogger(db_path=None)
    market = create_market_definition()
    ref_price = create_reference_price()
    fair_prob = create_fair_prob_estimate()
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    obs = logger.log_signal(
        market, ref_price, fair_prob,
        yes_bid=None, yes_ask=0.72,  # Missing bid
        no_bid=0.28, no_ask=0.30,
        as_of=as_of
    )
    
    assert obs.mid_yes is None  # Cannot calculate mid without bid
    assert obs.mid_no is not None  # Both present for no
    print(f"✅ Test 3: mid_yes=None when single-sided")


def test_edge_vs_yes_ask_calculation():
    """Test 4: edge_vs_yes_ask = p_yes - yes_ask"""
    logger = SignalLogger(db_path=None)
    market = create_market_definition()
    ref_price = create_reference_price()
    fair_prob = create_fair_prob_estimate()  # p_yes = 0.75
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    obs = logger.log_signal(
        market, ref_price, fair_prob,
        yes_bid=0.70, yes_ask=0.72,
        no_bid=0.28, no_ask=0.30,
        as_of=as_of
    )
    
    expected_edge = 0.75 - 0.72  # 0.03
    assert obs.edge_vs_yes_ask == expected_edge
    print(f"✅ Test 4: edge_vs_yes_ask={obs.edge_vs_yes_ask} (expected {expected_edge})")


def test_edge_vs_mid_yes_none_when_mid_none():
    """Test 5: edge_vs_mid_yes is None when mid_yes is None"""
    logger = SignalLogger(db_path=None)
    market = create_market_definition()
    ref_price = create_reference_price()
    fair_prob = create_fair_prob_estimate()
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    obs = logger.log_signal(
        market, ref_price, fair_prob,
        yes_bid=None, yes_ask=0.72,  # No mid
        no_bid=0.28, no_ask=0.30,
        as_of=as_of
    )
    
    assert obs.mid_yes is None
    assert obs.edge_vs_mid_yes is None  # Should follow mid availability
    print(f"✅ Test 5: edge_vs_mid_yes=None when mid_yes=None")


def test_memory_buffer_flush():
    """Test 6: Memory buffer flushes when reaching max size"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    try:
        logger = SignalLogger(db_path=db_path, max_memory_size=3)
        market = create_market_definition()
        ref_price = create_reference_price()
        fair_prob = create_fair_prob_estimate()
        as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
        
        # Add 3 observations (at limit, should not flush yet)
        for i in range(3):
            logger.log_signal(
                market, ref_price, fair_prob,
                yes_bid=0.70, yes_ask=0.72,
                no_bid=0.28, no_ask=0.30,
                as_of=as_of + timedelta(seconds=i)
            )
        
        assert len(logger._memory_buffer) == 3
        
        # Add 4th observation (triggers flush)
        logger.log_signal(
            market, ref_price, fair_prob,
            yes_bid=0.70, yes_ask=0.72,
            no_bid=0.28, no_ask=0.30,
            as_of=as_of + timedelta(seconds=3)
        )
        
        assert len(logger._memory_buffer) == 0  # Buffer flushed
        print(f"✅ Test 6: Memory buffer flushed at max size")
    finally:
        os.unlink(db_path)


def test_record_settlement_backfill():
    """Test 7: record_settlement correctly backfills outcome"""
    logger = SignalLogger(db_path=None)
    market = create_market_definition()
    ref_price = create_reference_price()
    fair_prob = create_fair_prob_estimate()
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    obs = logger.log_signal(
        market, ref_price, fair_prob,
        yes_bid=0.70, yes_ask=0.72,
        no_bid=0.28, no_ask=0.30,
        as_of=as_of
    )
    
    assert obs.settlement_outcome is None
    
    # Backfill settlement
    settlement_ts = create_utc_datetime(2026, 3, 28, 12, 10, 0)
    logger.record_settlement(obs.observation_id, True, settlement_ts)
    
    # Check updated in memory
    updated = logger._memory_buffer[0]
    assert updated.settlement_outcome is True
    assert updated.settlement_timestamp == settlement_ts
    print(f"✅ Test 7: Settlement backfilled: outcome={updated.settlement_outcome}")


def test_get_observations_time_filter():
    """Test 8: get_observations filters by time range"""
    logger = SignalLogger(db_path=None)
    market = create_market_definition()
    ref_price = create_reference_price()
    fair_prob = create_fair_prob_estimate()
    
    # Create observations at different times
    for i in range(5):
        as_of = create_utc_datetime(2026, 3, 28, 12, 0, i * 10)
        logger.log_signal(
            market, ref_price, fair_prob,
            yes_bid=0.70, yes_ask=0.72,
            no_bid=0.28, no_ask=0.30,
            as_of=as_of
        )
    
    # Query with time filter
    start = create_utc_datetime(2026, 3, 28, 12, 0, 15)
    end = create_utc_datetime(2026, 3, 28, 12, 0, 35)
    results = logger.get_observations(start_time=start, end_time=end)
    
    # Should get observations at 20s and 30s
    assert len(results) == 2
    print(f"✅ Test 8: Time filter returned {len(results)} observations")


def test_get_observations_market_id_filter():
    """Test 9: get_observations filters by market_id"""
    logger = SignalLogger(db_path=None)
    ref_price = create_reference_price()
    fair_prob = create_fair_prob_estimate()
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    # Create observations for different markets
    for market_id in ["market-001", "market-002", "market-001"]:
        market = create_market_definition()
        # Hack to change market_id
        market = MarketDefinition(**{**market.__dict__, "market_id": market_id})
        logger.log_signal(
            market, ref_price, fair_prob,
            yes_bid=0.70, yes_ask=0.72,
            no_bid=0.28, no_ask=0.30,
            as_of=as_of
        )
    
    results = logger.get_observations(market_id="market-001")
    assert len(results) == 2
    print(f"✅ Test 9: Market filter returned {len(results)} observations")


def test_get_observations_confidence_filter():
    """Test 10: get_observations filters by min_confidence"""
    logger = SignalLogger(db_path=None)
    market = create_market_definition()
    ref_price = create_reference_price()
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    # Create observations with different confidence
    for conf in [0.90, 0.70, 0.95]:
        fair_prob = FairProbEstimate(
            **{**create_fair_prob_estimate().__dict__, "model_confidence_score": conf}
        )
        logger.log_signal(
            market, ref_price, fair_prob,
            yes_bid=0.70, yes_ask=0.72,
            no_bid=0.28, no_ask=0.30,
            as_of=as_of
        )
    
    results = logger.get_observations(min_confidence=0.80)
    assert len(results) == 2  # 0.90 and 0.95
    print(f"✅ Test 10: Confidence filter returned {len(results)} observations")


def test_warning_flags_roundtrip():
    """Test 11: warning_flags correctly persist and deserialize"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    try:
        logger = SignalLogger(db_path=db_path)
        market = create_market_definition()
        ref_price = create_reference_price()
        
        # Create with warning flags
        fair_prob = FairProbEstimate(
            **{**create_fair_prob_estimate().__dict__, 
               "warning_flags": [WarningCode.VOL_ESTIMATE_MISSING, WarningCode.SPOT_STALE]}
        )
        as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
        
        obs = logger.log_signal(
            market, ref_price, fair_prob,
            yes_bid=0.70, yes_ask=0.72,
            no_bid=0.28, no_ask=0.30,
            as_of=as_of
        )
        
        # Flush to DB
        logger.flush()
        
        # Read back
        results = logger.get_observations()
        assert len(results) == 1
        
        read_obs = results[0]
        assert WarningCode.VOL_ESTIMATE_MISSING.value in read_obs.warning_flags
        assert WarningCode.SPOT_STALE.value in read_obs.warning_flags
        print(f"✅ Test 11: warning_flags roundtrip: {read_obs.warning_flags}")
    finally:
        os.unlink(db_path)


def test_market_definition_json_roundtrip():
    """Test 12: market_definition_json optional roundtrip"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    try:
        logger = SignalLogger(db_path=db_path)
        market = create_market_definition()
        ref_price = create_reference_price()
        fair_prob = create_fair_prob_estimate()
        as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
        
        obs = logger.log_signal(
            market, ref_price, fair_prob,
            yes_bid=0.70, yes_ask=0.72,
            no_bid=0.28, no_ask=0.30,
            as_of=as_of
        )
        
        assert obs.market_definition_json is not None
        assert "test-market-001" in obs.market_definition_json
        
        # Flush and read back
        logger.flush()
        results = logger.get_observations()
        
        read_obs = results[0]
        assert read_obs.market_definition_json is not None
        print(f"✅ Test 12: market_definition_json roundtrip OK")
    finally:
        os.unlink(db_path)


def test_sqlite_persistence():
    """Test 13: SQLite persistence works correctly"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    try:
        # Create logger and add observation
        logger1 = SignalLogger(db_path=db_path)
        market = create_market_definition()
        ref_price = create_reference_price()
        fair_prob = create_fair_prob_estimate()
        as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
        
        obs = logger1.log_signal(
            market, ref_price, fair_prob,
            yes_bid=0.70, yes_ask=0.72,
            no_bid=0.28, no_ask=0.30,
            as_of=as_of
        )
        
        logger1.flush()
        
        # Create new logger instance (simulating restart)
        logger2 = SignalLogger(db_path=db_path)
        results = logger2.get_observations()
        
        assert len(results) == 1
        assert results[0].observation_id == obs.observation_id
        assert results[0].p_yes == 0.75
        print(f"✅ Test 13: SQLite persistence works")
    finally:
        os.unlink(db_path)


def test_settlement_outcome_none_query():
    """Test 14: Query with include_unsettled=False filters correctly"""
    logger = SignalLogger(db_path=None)
    market = create_market_definition()
    ref_price = create_reference_price()
    fair_prob = create_fair_prob_estimate()
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    # Create 2 observations
    obs1 = logger.log_signal(
        market, ref_price, fair_prob,
        yes_bid=0.70, yes_ask=0.72,
        no_bid=0.28, no_ask=0.30,
        as_of=as_of
    )
    obs2 = logger.log_signal(
        market, ref_price, fair_prob,
        yes_bid=0.71, yes_ask=0.73,
        no_bid=0.27, no_ask=0.29,
        as_of=as_of + timedelta(seconds=1)
    )
    
    # Settle only one
    logger.record_settlement(obs1.observation_id, True, as_of + timedelta(minutes=10))
    
    # Query all (include unsettled)
    all_results = logger.get_observations(include_unsettled=True)
    assert len(all_results) == 2
    
    # Query only settled
    settled_results = logger.get_observations(include_unsettled=False)
    assert len(settled_results) == 1
    assert settled_results[0].observation_id == obs1.observation_id
    print(f"✅ Test 14: Settlement filtering works")


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("Package D Unit Tests - Signal Logger")
    print("=" * 70)
    
    # Run all tests
    test_log_signal_generates_observation_id()
    test_mid_yes_calculation()
    test_mid_yes_none_when_single_sided()
    test_edge_vs_yes_ask_calculation()
    test_edge_vs_mid_yes_none_when_mid_none()
    test_memory_buffer_flush()
    test_record_settlement_backfill()
    test_get_observations_time_filter()
    test_get_observations_market_id_filter()
    test_get_observations_confidence_filter()
    test_warning_flags_roundtrip()
    test_market_definition_json_roundtrip()
    test_sqlite_persistence()
    test_settlement_outcome_none_query()
    
    print("=" * 70)
    print("All 14 tests passed! ✅")
    print("=" * 70)
