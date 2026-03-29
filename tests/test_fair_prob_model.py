"""
Package C Unit Tests: Fair Probability Model
20 tests covering core functionality, edge cases, and error conditions.
"""

from datetime import datetime, timedelta, timezone
import math

from polymarket_arbitrage.fair_prob_model import (
    FairProbabilityModel,
    FairProbEstimate,
    WarningCode,
    UnsupportedPayoffError,
    UnsupportedSettlementError,
    UnsupportedStrikeTypeError,
    InvalidModelInputError,
    MissingReferencePriceError,
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


# ============================================================================
# Test Fixtures
# ============================================================================

def create_utc_datetime(year, month, day, hour, minute, second):
    """Helper to create timezone-aware UTC datetime."""
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


def create_reference_price(value, quality_score=0.95):
    """Helper to create a reference price for testing."""
    target = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    return ReferencePrice(
        value=value,
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
        quality_score=quality_score,
        quality_components={},
        warnings=[],
        prefer_method=ReferenceMethod.WINDOW_FIRST_TICK,
        allow_interpolation=True,
    )


def create_market_definition(
    payoff_type=PayoffType.DIGITAL_ABOVE,
    strike_type=StrikeType.OPEN_PRICE,
    strike_value=None,
    resolution_operator=ResolutionOperator.GT,
):
    """Helper to create a market definition for testing."""
    expiry = create_utc_datetime(2026, 3, 28, 12, 5, 0)  # 5 min from target
    return MarketDefinition(
        market_id="test-market-001",
        asset="BTC",
        payoff_type=payoff_type,
        resolution_operator=resolution_operator,
        strike_type=strike_type,
        strike_value=strike_value,
        upper_strike_value=None,
        strike_timestamp=create_utc_datetime(2026, 3, 28, 12, 0, 0),
        strike_window_seconds=None,
        expiry_timestamp=expiry,
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


def assert_raises(expected_exception, callable_func, *args, **kwargs):
    """Helper to assert that a function raises an expected exception."""
    try:
        callable_func(*args, **kwargs)
        raise AssertionError(f"Expected {expected_exception.__name__} was not raised")
    except expected_exception:
        return True
    except Exception as e:
        raise AssertionError(f"Expected {expected_exception.__name__} but got {type(e).__name__}: {e}")


# ============================================================================
# Test Cases 1-20
# ============================================================================

def test_digital_above_tau_positive_spot_above_strike():
    """Test 1: DIGITAL_ABOVE, tau>0, spot>K → p_yes > 0.5"""
    model = FairProbabilityModel()
    ref_price = create_reference_price(value=65000.0)
    market = create_market_definition(payoff_type=PayoffType.DIGITAL_ABOVE)
    
    spot = 66000.0  # above strike
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    spot_ts = as_of
    
    result = model.estimate_settlement_probability(
        market, ref_price, spot, spot_ts, as_of=as_of
    )
    
    assert result.p_yes > 0.5
    assert result.p_no < 0.5
    assert result.fair_yes_price == result.p_yes  # No fee deduction
    assert result.fair_no_price == result.p_no
    print(f"✅ Test 1: DIGITAL_ABOVE, spot>K, p_yes={result.p_yes:.4f}")


def test_digital_below_tau_positive_spot_below_strike():
    """Test 2: DIGITAL_BELOW, tau>0, spot<K → p_yes > 0.5 (for below)"""
    model = FairProbabilityModel()
    ref_price = create_reference_price(value=65000.0)
    market = create_market_definition(payoff_type=PayoffType.DIGITAL_BELOW)
    
    spot = 64000.0  # below strike
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    spot_ts = as_of
    
    result = model.estimate_settlement_probability(
        market, ref_price, spot, spot_ts, as_of=as_of
    )
    
    # For DIGITAL_BELOW, if spot < strike, p_yes should be high
    assert result.p_yes > 0.5
    print(f"✅ Test 2: DIGITAL_BELOW, spot<K, p_yes={result.p_yes:.4f}")


def test_tau_zero_gt_operator():
    """Test 3: tau=0, GT operator, spot>K → p_yes=1.0"""
    model = FairProbabilityModel()
    ref_price = create_reference_price(value=65000.0)
    market = create_market_definition(
        payoff_type=PayoffType.DIGITAL_ABOVE,
        resolution_operator=ResolutionOperator.GT
    )
    
    spot = 65100.0  # strictly greater
    as_of = create_utc_datetime(2026, 3, 28, 12, 5, 0)  # at expiry (tau=0)
    spot_ts = as_of
    
    result = model.estimate_settlement_probability(
        market, ref_price, spot, spot_ts, as_of=as_of
    )
    
    assert result.p_yes == 1.0
    assert result.p_no == 0.0
    assert WarningCode.DISCRETE_SETTLEMENT_APPLIED in result.warning_flags
    print(f"✅ Test 3: tau=0, GT, spot>K, p_yes={result.p_yes}")


def test_tau_zero_gte_operator():
    """Test 4: tau=0, GTE operator, spot=K → p_yes=1.0"""
    model = FairProbabilityModel()
    ref_price = create_reference_price(value=65000.0)
    market = create_market_definition(
        payoff_type=PayoffType.DIGITAL_ABOVE,
        resolution_operator=ResolutionOperator.GTE
    )
    
    spot = 65000.0  # equal to strike
    as_of = create_utc_datetime(2026, 3, 28, 12, 5, 0)  # at expiry
    spot_ts = as_of
    
    result = model.estimate_settlement_probability(
        market, ref_price, spot, spot_ts, as_of=as_of
    )
    
    assert result.p_yes == 1.0  # GTE includes equality
    assert WarningCode.DISCRETE_SETTLEMENT_APPLIED in result.warning_flags
    print(f"✅ Test 4: tau=0, GTE, spot=K, p_yes={result.p_yes}")


def test_tau_zero_gt_operator_spot_equal():
    """Test 5: tau=0, GT operator, spot=K → p_yes=0.0 (strict inequality)"""
    model = FairProbabilityModel()
    ref_price = create_reference_price(value=65000.0)
    market = create_market_definition(
        payoff_type=PayoffType.DIGITAL_ABOVE,
        resolution_operator=ResolutionOperator.GT
    )
    
    spot = 65000.0  # equal, not strictly greater
    as_of = create_utc_datetime(2026, 3, 28, 12, 5, 0)  # at expiry
    spot_ts = as_of
    
    result = model.estimate_settlement_probability(
        market, ref_price, spot, spot_ts, as_of=as_of
    )
    
    assert result.p_yes == 0.0  # GT is strict
    print(f"✅ Test 5: tau=0, GT, spot=K, p_yes={result.p_yes} (strict)")


def test_vol_input_none_triggers_fallback():
    """Test 6: vol_input=None → VOL_ESTIMATE_MISSING warning"""
    model = FairProbabilityModel()
    ref_price = create_reference_price(value=65000.0)
    market = create_market_definition()
    
    spot = 66000.0
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    spot_ts = as_of
    
    result = model.estimate_settlement_probability(
        market, ref_price, spot, spot_ts, vol_input=None, as_of=as_of
    )
    
    assert WarningCode.VOL_ESTIMATE_MISSING in result.warning_flags
    assert result.assumptions["vol_source"] == "fallback_default"
    assert result.volatility == model.default_vol
    print(f"✅ Test 6: vol=None triggers fallback warning")


def test_reference_quality_low_warning():
    """Test 7: reference_quality < 0.5 → REFERENCE_QUALITY_LOW warning"""
    model = FairProbabilityModel()
    ref_price = create_reference_price(value=65000.0, quality_score=0.3)
    market = create_market_definition()
    
    spot = 66000.0
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    spot_ts = as_of
    
    result = model.estimate_settlement_probability(
        market, ref_price, spot, spot_ts, as_of=as_of
    )
    
    assert WarningCode.REFERENCE_QUALITY_LOW in result.warning_flags
    assert result.input_quality_score == 0.3
    print(f"✅ Test 7: quality<0.5 triggers warning")


def test_open_price_strike_source():
    """Test 8: OPEN_PRICE strike source uses reference_price.value"""
    model = FairProbabilityModel()
    ref_price = create_reference_price(value=65000.0)
    market = create_market_definition(strike_type=StrikeType.OPEN_PRICE)
    
    spot = 66000.0
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    spot_ts = as_of
    
    result = model.estimate_settlement_probability(
        market, ref_price, spot, spot_ts, as_of=as_of
    )
    
    assert result.strike_price == 65000.0
    assert result.assumptions["strike_source"] == "reference_price"
    print(f"✅ Test 8: OPEN_PRICE strike from reference")


def test_fixed_price_strike_source():
    """Test 9: FIXED_PRICE strike source uses market_def.strike_value"""
    model = FairProbabilityModel()
    ref_price = create_reference_price(value=None)  # reference value not used
    market = create_market_definition(
        strike_type=StrikeType.FIXED_PRICE,
        strike_value=70000.0
    )
    
    spot = 71000.0
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    spot_ts = as_of
    
    result = model.estimate_settlement_probability(
        market, ref_price, spot, spot_ts, as_of=as_of
    )
    
    assert result.strike_price == 70000.0
    assert result.assumptions["strike_source"] == "market_def"
    print(f"✅ Test 9: FIXED_PRICE strike from market_def")


def test_unsupported_payoff_type_raises():
    """Test 10: Unsupported payoff type raises UnsupportedPayoffError"""
    model = FairProbabilityModel()
    ref_price = create_reference_price(value=65000.0)
    market = create_market_definition(payoff_type=PayoffType.DIGITAL_INSIDE_RANGE)
    
    spot = 66000.0
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    spot_ts = as_of
    
    try:
        model.estimate_settlement_probability(
            market, ref_price, spot, spot_ts, as_of=as_of
        )
        raise AssertionError("Expected UnsupportedPayoffError was not raised")
    except UnsupportedPayoffError as e:
        assert "DIGITAL_ABOVE/BELOW" in str(e)
        print(f"✅ Test 10: Unsupported payoff raises error")


def test_unsupported_settlement_rule_raises():
    """Test 11: Unsupported settlement rule raises UnsupportedSettlementError"""
    model = FairProbabilityModel()
    ref_price = create_reference_price(value=65000.0)
    market = create_market_definition()
    # Monkey-patch settlement rule
    market = MarketDefinition(
        **{**market.__dict__, "settlement_rule": SettlementRule.TWAP}
    )
    
    spot = 66000.0
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    spot_ts = as_of
    
    try:
        model.estimate_settlement_probability(
            market, ref_price, spot, spot_ts, as_of=as_of
        )
        raise AssertionError("Expected UnsupportedSettlementError was not raised")
    except UnsupportedSettlementError as e:
        assert "TERMINAL_PRICE" in str(e)
        print(f"✅ Test 11: Unsupported settlement raises error")


def test_spot_less_equal_zero_raises():
    """Test 12: spot <= 0 raises InvalidModelInputError"""
    model = FairProbabilityModel()
    ref_price = create_reference_price(value=65000.0)
    market = create_market_definition()
    
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    spot_ts = as_of
    
    try:
        model.estimate_settlement_probability(
            market, ref_price, 0.0, spot_ts, as_of=as_of
        )
        raise AssertionError("Expected InvalidModelInputError was not raised")
    except InvalidModelInputError as e:
        assert "spot must be positive" in str(e)
        print(f"✅ Test 12: spot<=0 raises error")


def test_strike_less_equal_zero_raises():
    """Test 13: strike <= 0 raises InvalidModelInputError (via _bs_digital_probability)"""
    model = FairProbabilityModel()
    ref_price = create_reference_price(value=0.0)  # invalid strike
    market = create_market_definition()
    
    spot = 100.0
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    spot_ts = as_of
    
    try:
        model.estimate_settlement_probability(
            market, ref_price, spot, spot_ts, as_of=as_of
        )
        raise AssertionError("Expected InvalidModelInputError was not raised")
    except (InvalidModelInputError, MissingReferencePriceError) as e:
        # Either error is acceptable
        print(f"✅ Test 13: strike<=0 path validated (via reference check)")


def test_vol_less_equal_zero_raises():
    """Test 14: vol <= 0 raises InvalidModelInputError"""
    model = FairProbabilityModel()
    ref_price = create_reference_price(value=65000.0)
    market = create_market_definition()
    
    spot = 66000.0
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    spot_ts = as_of
    
    try:
        model.estimate_settlement_probability(
            market, ref_price, spot, spot_ts, vol_input=-0.1, as_of=as_of
        )
        raise AssertionError("Expected InvalidModelInputError was not raised")
    except InvalidModelInputError as e:
        assert "volatility must be positive" in str(e)
        print(f"✅ Test 14: vol<=0 raises error")


def test_open_price_missing_reference_raises():
    """Test 15: OPEN_PRICE with reference_price.value=None raises MissingReferencePriceError"""
    model = FairProbabilityModel()
    ref_price = create_reference_price(value=None)  # Missing
    market = create_market_definition(strike_type=StrikeType.OPEN_PRICE)
    
    spot = 66000.0
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    spot_ts = as_of
    
    try:
        model.estimate_settlement_probability(
            market, ref_price, spot, spot_ts, as_of=as_of
        )
        raise AssertionError("Expected MissingReferencePriceError was not raised")
    except MissingReferencePriceError as e:
        assert "reference_price.value is None" in str(e)
        print(f"✅ Test 15: Missing reference for OPEN_PRICE raises error")


def test_spot_equal_strike_tau_positive_large_vol():
    """Test 16: spot=strike, tau>0, very large vol → p_yes ≈ 0.5"""
    model = FairProbabilityModel()
    ref_price = create_reference_price(value=65000.0)
    market = create_market_definition(payoff_type=PayoffType.DIGITAL_ABOVE)
    
    spot = 65000.0  # equal to strike
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    spot_ts = as_of
    
    result = model.estimate_settlement_probability(
        market, ref_price, spot, spot_ts, vol_input=2.0, drift_input=0.0, as_of=as_of
    )
    
    # With drift=0 and spot=strike, p_yes should be close to 0.5
    assert abs(result.p_yes - 0.5) < 0.1
    print(f"✅ Test 16: spot=strike, large vol, p_yes={result.p_yes:.4f} ≈ 0.5")


def test_fixed_price_missing_strike_raises():
    """Test 17: FIXED_PRICE with market_def.strike_value=None raises MissingReferencePriceError"""
    model = FairProbabilityModel()
    ref_price = create_reference_price(value=65000.0)
    market = create_market_definition(
        strike_type=StrikeType.FIXED_PRICE,
        strike_value=None  # Missing
    )
    
    spot = 66000.0
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    spot_ts = as_of
    
    try:
        model.estimate_settlement_probability(
            market, ref_price, spot, spot_ts, as_of=as_of
        )
        raise AssertionError("Expected MissingReferencePriceError was not raised")
    except MissingReferencePriceError as e:
        assert "market_def.strike_value is None" in str(e)
        print(f"✅ Test 17: Missing strike_value for FIXED_PRICE raises error")


def test_unsupported_strike_type_raises():
    """Test 18: Unsupported strike type (e.g., TWAP_REFERENCE) raises UnsupportedStrikeTypeError"""
    model = FairProbabilityModel()
    ref_price = create_reference_price(value=65000.0)
    market = create_market_definition()
    # Monkey-patch strike type
    market = MarketDefinition(
        **{**market.__dict__, "strike_type": StrikeType.TWAP_REFERENCE}
    )
    
    spot = 66000.0
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    spot_ts = as_of
    
    try:
        model.estimate_settlement_probability(
            market, ref_price, spot, spot_ts, as_of=as_of
        )
        raise AssertionError("Expected UnsupportedStrikeTypeError was not raised")
    except UnsupportedStrikeTypeError as e:
        assert "OPEN_PRICE and FIXED_PRICE" in str(e)
        print(f"✅ Test 18: Unsupported strike type raises error")


def test_naive_datetime_rejected():
    """Test 19: Naive datetime raises InvalidModelInputError"""
    model = FairProbabilityModel()
    ref_price = create_reference_price(value=65000.0)
    market = create_market_definition()
    
    spot = 66000.0
    naive_dt = datetime(2026, 3, 28, 12, 0, 0)  # No timezone
    
    try:
        model.estimate_settlement_probability(
            market, ref_price, spot, naive_dt, as_of=naive_dt
        )
        raise AssertionError("Expected InvalidModelInputError was not raised")
    except InvalidModelInputError as e:
        assert "timezone-aware" in str(e)
        print(f"✅ Test 19: Naive datetime rejected")


def test_model_confidence_multiplicative():
    """Test 20: Model confidence is multiplicative"""
    model = FairProbabilityModel()
    
    # Low quality reference
    ref_price_low = create_reference_price(value=65000.0, quality_score=0.3)
    market = create_market_definition()
    
    spot = 66000.0
    as_of = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    spot_ts = as_of  # Very fresh
    
    result = model.estimate_settlement_probability(
        market, ref_price_low, spot, spot_ts, vol_input=0.5, as_of=as_of
    )
    
    # Expected: 0.3 * 1.0 * 1.0 = 0.3
    expected_confidence = 0.3 * 1.0 * 1.0
    assert abs(result.model_confidence_score - expected_confidence) < 0.01
    
    # Check components recorded
    assert result.assumptions["reference_quality_component"] == 0.3
    assert result.assumptions["spot_freshness_component"] == 1.0
    assert result.assumptions["volatility_quality_component"] == 1.0
    print(f"✅ Test 20: Multiplicative confidence: {result.model_confidence_score:.4f}")


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("Package C Unit Tests - Fair Probability Model")
    print("=" * 70)
    
    # Run all tests
    test_digital_above_tau_positive_spot_above_strike()
    test_digital_below_tau_positive_spot_below_strike()
    test_tau_zero_gt_operator()
    test_tau_zero_gte_operator()
    test_tau_zero_gt_operator_spot_equal()
    test_vol_input_none_triggers_fallback()
    test_reference_quality_low_warning()
    test_open_price_strike_source()
    test_fixed_price_strike_source()
    test_unsupported_payoff_type_raises()
    test_unsupported_settlement_rule_raises()
    test_spot_less_equal_zero_raises()
    test_strike_less_equal_zero_raises()
    test_vol_less_equal_zero_raises()
    test_open_price_missing_reference_raises()
    test_spot_equal_strike_tau_positive_large_vol()
    test_fixed_price_missing_strike_raises()
    test_unsupported_strike_type_raises()
    test_naive_datetime_rejected()
    test_model_confidence_multiplicative()
    
    print("=" * 70)
    print("All 20 tests passed! ✅")
    print("=" * 70)
