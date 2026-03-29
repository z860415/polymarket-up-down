"""
Package C: Fair Probability Model
Lognormal Terminal Exceedance Probability Estimator

This is NOT risk-neutral option pricing.
It estimates P(S_T > K) based on lognormal terminal distribution assumptions.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from scipy.stats import norm
import math

from polymarket_arbitrage.market_definition import (
    MarketDefinition,
    PayoffType,
    ResolutionOperator,
    SettlementRule,
    StrikeType,
)
from polymarket_arbitrage.reference_builder import ReferencePrice, ReferenceStatus


# ============================================================================
# Exceptions
# ============================================================================

class UnsupportedPayoffError(ValueError):
    """Payoff type not supported in this model version."""
    pass


class UnsupportedSettlementError(ValueError):
    """Settlement rule not supported in this model version."""
    pass


class UnsupportedStrikeTypeError(ValueError):
    """Strike type not supported in this model version."""
    pass


class InvalidModelInputError(ValueError):
    """Model input violates mathematical constraints (e.g., spot <= 0)."""
    pass


class MissingReferencePriceError(ValueError):
    """Reference price required but not available."""
    pass


# ============================================================================
# Warning Codes
# ============================================================================

class WarningCode(str, Enum):
    """Warning codes for probability estimation."""
    VOL_ESTIMATE_MISSING = "VOL_ESTIMATE_MISSING"
    VOL_FALLBACK_USED = "VOL_FALLBACK_USED"
    SPOT_STALE = "SPOT_STALE"
    REFERENCE_QUALITY_LOW = "REFERENCE_QUALITY_LOW"
    TAU_NEAR_ZERO_APPROXIMATION = "TAU_NEAR_ZERO_APPROXIMATION"
    UNSUPPORTED_PAYOFF_TYPE = "UNSUPPORTED_PAYOFF_TYPE"
    UNSUPPORTED_SETTLEMENT_RULE = "UNSUPPORTED_SETTLEMENT_RULE"
    DISCRETE_SETTLEMENT_APPLIED = "DISCRETE_SETTLEMENT_APPLIED"


# ============================================================================
# Result Dataclass
# ============================================================================

@dataclass(frozen=True)
class FairProbEstimate:
    """
    Fair probability estimate result.
    
    Note: fair_yes_price / fair_no_price are raw probabilities (no fee adjustment).
    Fee handling is deferred to downstream execution layers.
    """
    
    # Core probabilities
    p_yes: float                    # P(event happens)
    p_no: float                     # P(event not happens) = 1 - p_yes
    
    # Fair prices (v0.2: equal to probabilities, no fee deduction)
    fair_yes_price: float           # = p_yes
    fair_no_price: float            # = p_no
    
    # Model metadata
    model_version: str              # "lognormal_terminal_exceedance_v0.2"
    assumptions: Dict[str, Any]     # Key assumptions recorded
    
    # Input quality (separate from probability)
    model_confidence_score: float   # 0.0-1.0, based on input quality
    input_quality_score: float      # From reference_price.quality_score
    input_freshness_ms: float       # Delay from spot to as_of
    
    # Pricing parameters (transparent)
    strike_price: float
    spot_price: float
    time_to_expiry_sec: float       # tau >= 0
    volatility: float               # Annualized volatility
    drift: Optional[float]          # Default 0.0, explicitly recorded
    
    # Warnings
    warning_flags: List[WarningCode]


# ============================================================================
# Main Model Class
# ============================================================================

class FairProbabilityModel:
    """
    Lognormal Terminal Exceedance Probability Estimator
    
    This model estimates P(S_T > K) based on lognormal terminal distribution
    assumptions. It is NOT risk-neutral Black-Scholes pricing.
    
    Supported in v0.2:
    - Payoff: DIGITAL_ABOVE, DIGITAL_BELOW
    - Settlement: TERMINAL_PRICE
    - Strike: OPEN_PRICE, FIXED_PRICE
    """
    
    def __init__(
        self,
        default_volatility: float = 0.50,      # Default 50% annual vol
        vol_fallback_threshold: float = 0.10,   # vol < 10% considered abnormal
    ):
        self.default_vol = default_volatility
        self.vol_threshold = vol_fallback_threshold
        self.model_version = "lognormal_terminal_exceedance_v0.2"
    
    def estimate_settlement_probability(
        self,
        market_def: MarketDefinition,
        reference_price: ReferencePrice,
        spot_price: float,
        spot_timestamp: datetime,
        vol_input: Optional[float] = None,
        drift_input: Optional[float] = None,
        as_of: Optional[datetime] = None,
    ) -> FairProbEstimate:
        """
        Estimate settlement probability.
        
        Strike price source:
        - OPEN_PRICE: reference_price.value
        - FIXED_PRICE: market_def.strike_value
        
        tau=0 boundary: Direct discrete comparison, no continuous formula.
        
        Args:
            as_of: Unified time baseline. If None, uses datetime.now(timezone.utc).
        
        Raises:
            UnsupportedPayoffError: If payoff type not DIGITAL_ABOVE/BELOW.
            UnsupportedSettlementError: If settlement not TERMINAL_PRICE.
            UnsupportedStrikeTypeError: If strike type not OPEN/FIXED_PRICE.
            InvalidModelInputError: If spot/strike/vol <= 0.
            MissingReferencePriceError: If strike_type=OPEN_PRICE but reference missing.
        """
        warnings: List[WarningCode] = []
        
        # Unified time baseline
        if as_of is None:
            as_of = datetime.now(timezone.utc)
        as_of = self._ensure_utc_datetime(as_of)
        
        # 1. Check supported range (raise directly, no warning append first)
        if market_def.payoff_type not in (PayoffType.DIGITAL_ABOVE, PayoffType.DIGITAL_BELOW):
            raise UnsupportedPayoffError(
                f"Only DIGITAL_ABOVE/BELOW supported in v0.2, got {market_def.payoff_type}"
            )
        
        if market_def.settlement_rule != SettlementRule.TERMINAL_PRICE:
            raise UnsupportedSettlementError(
                f"Only TERMINAL_PRICE supported in v0.2, got {market_def.settlement_rule}"
            )
        
        # 2. Determine strike price (explicit source, strict checks)
        strike = self._calculate_strike(market_def, reference_price)
        
        # 3. Calculate tau (non-negative, based on as_of)
        tau = self._calculate_tau(market_def, as_of)
        
        # 4. Input quality check
        if reference_price.quality_score < 0.5:
            warnings.append(WarningCode.REFERENCE_QUALITY_LOW)
        
        # 5. Spot freshness (based on as_of)
        input_freshness_ms = abs((as_of - spot_timestamp).total_seconds() * 1000)
        if input_freshness_ms > 5000:  # > 5s considered stale
            warnings.append(WarningCode.SPOT_STALE)
        
        # 6. Determine volatility
        if vol_input is not None and vol_input <= 0:
            raise InvalidModelInputError(f"volatility must be positive, got {vol_input}")
        
        vol = vol_input if vol_input is not None else self.default_vol
        vol_quality = "input"
        if vol_input is None:
            warnings.append(WarningCode.VOL_ESTIMATE_MISSING)
            vol_quality = "fallback_default"
        elif vol < self.vol_threshold:
            warnings.append(WarningCode.VOL_FALLBACK_USED)
            vol = self.default_vol
            vol_quality = "fallback_threshold"
        
        # 7. Determine drift (default 0.0)
        drift = drift_input if drift_input is not None else 0.0
        
        # 8. Calculate probability
        if tau <= 0:
            # tau=0: Discrete settlement
            p_yes = self._discrete_settlement(spot_price, strike, market_def.resolution_operator)
            warnings.append(WarningCode.DISCRETE_SETTLEMENT_APPLIED)
        else:
            # tau>0: Lognormal terminal exceedance probability
            p_yes = self._bs_digital_probability(
                spot_price, strike, tau, vol, drift, market_def.payoff_type
            )
        
        p_no = 1.0 - p_yes
        
        # 9. Calculate model_confidence_score (multiplicative model)
        ref_component = reference_price.quality_score  # 0.0-1.0
        
        # Spot freshness component: bucketed
        if input_freshness_ms < 500:
            spot_component = 1.0  # Very fresh
            freshness_bucket = "fresh"
        elif input_freshness_ms < 2000:
            spot_component = 0.7  # Acceptable
            freshness_bucket = "acceptable"
        else:
            spot_component = 0.4  # Stale
            freshness_bucket = "stale"
        
        # Volatility quality component
        vol_component = 1.0 if vol_quality == "input" else 0.6
        vol_bucket = "input" if vol_component == 1.0 else "fallback"
        
        # Multiplicative model
        model_confidence = ref_component * spot_component * vol_component
        
        # 10. Assemble result
        return FairProbEstimate(
            p_yes=p_yes,
            p_no=p_no,
            fair_yes_price=p_yes,  # v0.2: No fee deduction
            fair_no_price=p_no,
            model_version=self.model_version,
            assumptions={
                "model_type": "lognormal_terminal_exceedance",
                "drift": drift,
                "vol_source": vol_quality,
                "strike_source": "reference_price" if market_def.strike_type == StrikeType.OPEN_PRICE else "market_def",
                "confidence_rule_version": "multiplicative_v0.2",
                "reference_quality_component": round(ref_component, 4),
                "spot_freshness_component": spot_component,
                "volatility_quality_component": vol_component,
                "spot_freshness_ms": input_freshness_ms,
                "spot_freshness_bucket": freshness_bucket,
                "vol_quality_bucket": vol_bucket,
            },
            model_confidence_score=round(model_confidence, 4),
            input_quality_score=reference_price.quality_score,
            input_freshness_ms=input_freshness_ms,
            strike_price=strike,
            spot_price=spot_price,
            time_to_expiry_sec=tau,
            volatility=vol,
            drift=drift,
            warning_flags=warnings,
        )
    
    def _calculate_strike(
        self,
        market_def: MarketDefinition,
        reference_price: ReferencePrice,
    ) -> float:
        """
        Determine strike price with explicit source checking.
        
        - OPEN_PRICE: reference_price.value (must not be None)
        - FIXED_PRICE: market_def.strike_value (must not be None)
        
        Raises:
            MissingReferencePriceError: If OPEN_PRICE but reference missing.
            UnsupportedStrikeTypeError: If strike type not supported.
        """
        if market_def.strike_type == StrikeType.OPEN_PRICE:
            if reference_price.value is None:
                raise MissingReferencePriceError(
                    "strike_type=OPEN_PRICE but reference_price.value is None"
                )
            return float(reference_price.value)
        
        elif market_def.strike_type == StrikeType.FIXED_PRICE:
            if market_def.strike_value is None:
                raise MissingReferencePriceError(
                    "strike_type=FIXED_PRICE but market_def.strike_value is None"
                )
            return float(market_def.strike_value)
        
        else:
            # v0.2: Only OPEN_PRICE and FIXED_PRICE supported
            raise UnsupportedStrikeTypeError(
                f"Strike type {market_def.strike_type} not supported in v0.2. "
                f"Only OPEN_PRICE and FIXED_PRICE."
            )
    
    def _calculate_tau(
        self,
        market_def: MarketDefinition,
        as_of: datetime,
    ) -> float:
        """
        Calculate remaining time to expiry in seconds.
        
        Returns non-negative value (tau >= 0).
        If already expired, returns 0.0.
        """
        expiry = market_def.expiry_timestamp
        tau_seconds = (expiry - as_of).total_seconds()
        return max(0.0, tau_seconds)
    
    def _bs_digital_probability(
        self,
        spot: float,
        strike: float,
        tau: float,
        vol: float,
        drift: float,
        payoff_type: PayoffType,
    ) -> float:
        """
        Lognormal Terminal Exceedance Probability
        
        Estimates P(S_T > K) based on lognormal terminal distribution.
        
        Note: This is NOT risk-neutral option pricing. It uses drift μ,
        not risk-free rate r.
        
        For DIGITAL_ABOVE (P(S_T > K)):
            p = Φ(d2)
            where d2 = [ln(S/K) + (μ - σ²/2)T] / (σ√T)
            T is time in YEARS (tau / 31536000)
        
        For DIGITAL_BELOW (P(S_T < K)):
            p = Φ(-d2) = 1 - Φ(d2)
        
        Raises:
            InvalidModelInputError: If spot/strike/vol <= 0.
        """
        # Parameter guards
        if spot <= 0:
            raise InvalidModelInputError(f"spot must be positive, got {spot}")
        if strike <= 0:
            raise InvalidModelInputError(f"strike must be positive, got {strike}")
        if vol <= 0:
            raise InvalidModelInputError(f"volatility must be positive, got {vol}")
        
        # Convert tau (seconds) to years
        T = tau / 31536000.0  # seconds per year
        
        if T <= 0:
            raise InvalidModelInputError(f"time must be positive, got tau={tau}s, T={T}y")
        
        # Calculate d2 (using drift μ, not risk-free rate r)
        ln_sk = math.log(spot / strike)
        drift_adjustment = (drift - 0.5 * vol * vol) * T
        denominator = vol * math.sqrt(T)
        
        if denominator == 0:
            raise InvalidModelInputError(
                f"denominator zero, tau={tau}, vol={vol}"
            )
        
        d2 = (ln_sk + drift_adjustment) / denominator
        
        # Calculate probability
        if payoff_type == PayoffType.DIGITAL_ABOVE:
            p = norm.cdf(d2)
        elif payoff_type == PayoffType.DIGITAL_BELOW:
            p = norm.cdf(-d2)  # = 1 - Φ(d2)
        else:
            raise UnsupportedPayoffError(
                f"Unsupported payoff type: {payoff_type}"
            )
        
        # Boundary protection
        return max(0.0, min(1.0, p))
    
    def _discrete_settlement(
        self,
        spot: float,
        strike: float,
        operator: ResolutionOperator,
    ) -> float:
        """
        Discrete settlement at tau=0.
        
        Returns 1.0 if condition met, 0.0 otherwise.
        
        GT:  spot > strike  → 1.0, else 0.0
        GTE: spot >= strike → 1.0, else 0.0
        LT:  spot < strike  → 1.0, else 0.0
        LTE: spot <= strike → 1.0, else 0.0
        """
        if operator == ResolutionOperator.GT:
            return 1.0 if spot > strike else 0.0
        elif operator == ResolutionOperator.GTE:
            return 1.0 if spot >= strike else 0.0
        elif operator == ResolutionOperator.LT:
            return 1.0 if spot < strike else 0.0
        elif operator == ResolutionOperator.LTE:
            return 1.0 if spot <= strike else 0.0
        else:
            # For operators not directly applicable, use GT as default
            return 1.0 if spot > strike else 0.0
    
    @staticmethod
    def _ensure_utc_datetime(dt: datetime) -> datetime:
        """Ensure datetime is timezone-aware UTC."""
        if dt.tzinfo is None:
            raise InvalidModelInputError(
                f"datetime must be timezone-aware, got naive: {dt}"
            )
        return dt.astimezone(timezone.utc)
