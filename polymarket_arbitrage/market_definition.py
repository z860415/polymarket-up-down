"""
Package A: Market Definition

市場語義層：將 Polymarket market 解析成 payoff 對齊的標準化定義。

時間欄位約束：
- 所有 datetime 必須是 timezone-aware UTC
- 禁止 naive datetime
- 序列化時轉 ISO8601 UTC
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple, Pattern
from enum import Enum
import re


# ============================================================================
# Enums
# ============================================================================


class PayoffType(str, Enum):
    """Payoff 類型：決定定價公式"""

    DIGITAL_ABOVE = "digital_above"  # S_T > K
    DIGITAL_BELOW = "digital_below"  # S_T < K
    DIGITAL_INSIDE_RANGE = "digital_inside_range"  # K1 <= S_T <= K2
    DIGITAL_OUTSIDE_RANGE = "digital_outside_range"  # S_T < K1 or S_T > K2
    TOUCH_UPPER = "touch_upper"  # max(S_t) >= K
    TOUCH_LOWER = "touch_lower"  # min(S_t) <= K


class ResolutionOperator(str, Enum):
    """結算運算子：處理邊界條件"""

    GT = "gt"  # >
    GTE = "gte"  # >=
    LT = "lt"  # <
    LTE = "lte"  # <=
    INSIDE_CLOSED = "inside_closed"  # [K1, K2]
    INSIDE_OPEN = "inside_open"  # (K1, K2)
    INSIDE_LEFT_CLOSED = "inside_left_closed"  # [K1, K2)
    INSIDE_RIGHT_CLOSED = "inside_right_closed"  # (K1, K2]


class StrikeType(str, Enum):
    """Strike 定義類型"""

    OPEN_PRICE = "open_price"
    FIXED_PRICE = "fixed_price"
    PREVIOUS_CLOSE = "previous_close"
    TWAP_REFERENCE = "twap_reference"


class SettlementRule(str, Enum):
    """結算規則"""

    TERMINAL_PRICE = "terminal_price"
    HIGH_LOW = "high_low"
    TWAP = "twap"


class OracleFamily(str, Enum):
    """Oracle 來源家族"""

    CHAINLINK = "chainlink"
    BINANCE = "binance"
    POLYMARKET_INTERNAL = "polymarket_internal"


class WarningCode(str, Enum):
    """解析警告代碼"""

    PAYOFF_PARSE_FAILED = "PAYOFF_PARSE_FAILED"
    ASSET_NOT_RECOGNIZED = "ASSET_NOT_RECOGNIZED"
    UNSUPPORTED_SETTLEMENT = "UNSUPPORTED_SETTLEMENT"
    AMBIGUOUS_BOUNDARY = "AMBIGUOUS_BOUNDARY"
    DESCRIPTION_OVERRIDES_QUESTION = "DESCRIPTION_OVERRIDES_QUESTION"
    FALLBACK_ORACLE_ASSIGNED = "FALLBACK_ORACLE_ASSIGNED"
    NAIVE_DATETIME_REJECTED = "NAIVE_DATETIME_REJECTED"


# ============================================================================
# Dataclasses
# ============================================================================


@dataclass(frozen=True)
class PayoffSemantics:
    """Payoff 語義解析結果"""

    payoff_type: PayoffType
    resolution_operator: ResolutionOperator
    strike_type: StrikeType
    strike_value: Optional[float]
    upper_strike_value: Optional[float]  # for range markets


@dataclass(frozen=True)
class OracleConfig:
    """Oracle 配置"""

    family: OracleFamily
    symbol: str
    decimals: int


@dataclass(frozen=True)
class MarketDefinitionResult:
    """
    build_market_definition 的完整回傳

    parse_confidence 和 parse_notes 放在這裡，不在 MarketDefinition 本體
    """

    definition: Optional["MarketDefinition"]
    success: bool
    warnings: List[str]  # WarningCode 字串列表
    parse_confidence: float  # 0.0-1.0
    parse_notes: Optional[str] = None


@dataclass(frozen=True)
class MarketDefinition:
    """
    市場定義 - 語義化、payoff 對齊的市場描述

    Asset 標準化：
    - 只輸出 "BTC", "ETH" 等大寫標準代碼
    - 禁止 "bitcoin", "btc", "BTCUSD", "XBT" 等變體

    時間欄位約束：
    - 所有 datetime 必須是 timezone-aware UTC
    - 禁止 naive datetime
    """

    # 基礎識別
    market_id: str

    # 資產（標準化大寫）
    asset: str

    # Payoff 語義（核心）
    payoff_type: PayoffType
    resolution_operator: ResolutionOperator

    # Strike 定義
    strike_type: StrikeType
    strike_value: Optional[float]
    upper_strike_value: Optional[float]  # for range markets
    strike_timestamp: datetime  # timezone-aware UTC
    strike_window_seconds: Optional[int]  # for twap_reference

    # 結算參數
    expiry_timestamp: datetime  # timezone-aware UTC
    settlement_rule: SettlementRule

    # Oracle 來源（主）
    oracle_family: OracleFamily
    oracle_symbol: str
    oracle_decimals: int

    # 交易參數
    fee_enabled: bool
    yes_token_id: str
    no_token_id: str

    # 原始資料（供 debug）
    raw_question: str
    raw_description: Optional[str]

    # Oracle 來源（Fallback）- 必須放在所有 non-default 欄位之後
    fallback_oracle_family: Optional[OracleFamily] = None
    fallback_oracle_symbol: Optional[str] = None
    fallback_oracle_decimals: Optional[int] = None

    # v1 尾盤主線擴充欄位
    market_style: str = "ABOVE_BELOW"
    timeframe: Optional[str] = None
    timeframe_seconds: Optional[int] = None
    market_start_timestamp: Optional[datetime] = None
    settlement_source_descriptor: Optional[str] = None
    anchor_required: bool = False


# ============================================================================
# Asset 標準化映射
# ============================================================================

ASSET_SYNONYMS = {
    # 輸入: 標準輸出
    "bitcoin": "BTC",
    "btc": "BTC",
    "xbt": "BTC",
    "ethereum": "ETH",
    "ether": "ETH",
    "eth": "ETH",
    "solana": "SOL",
    "sol": "SOL",
    "cardano": "ADA",
    "ada": "ADA",
    "ripple": "XRP",
    "xrp": "XRP",
    "polkadot": "DOT",
    "dot": "DOT",
    "polygon": "MATIC",
    "matic": "MATIC",
    "pol": "MATIC",
    "chainlink": "LINK",
    "link": "LINK",
    "avalanche": "AVAX",
    "avax": "AVAX",
    "litecoin": "LTC",
    "ltc": "LTC",
    "dogecoin": "DOGE",
    "doge": "DOGE",
    "shiba": "SHIB",
    "shib": "SHIB",
    "pepe": "PEPE",
    "bonk": "BONK",
    "wif": "WIF",
    "hyperliquid": "HYPE",
    "hype": "HYPE",
    "sui": "SUI",
    "near": "NEAR",
    "apt": "APT",
    "aptos": "APT",
    "arb": "ARB",
    "arbitrum": "ARB",
    "op": "OP",
    "optimism": "OP",
}

TIMEFRAME_SECONDS = {
    "1m": 1 * 60,
    "5m": 5 * 60,
    "15m": 15 * 60,
    "30m": 30 * 60,
    "1h": 60 * 60,
    "4h": 4 * 60 * 60,
    "12h": 12 * 60 * 60,
    "1d": 24 * 60 * 60,
}

# 所有支援 Binance oracle 的資產（Binance USDT 交易對）
_BINANCE_SUPPORTED_ASSETS = {
    "BTC": ("BTCUSDT", 2),
    "ETH": ("ETHUSDT", 2),
    "SOL": ("SOLUSDT", 2),
    "ADA": ("ADAUSDT", 4),
    "XRP": ("XRPUSDT", 4),
    "DOT": ("DOTUSDT", 2),
    "MATIC": ("MATICUSDT", 4),
    "LINK": ("LINKUSDT", 2),
    "AVAX": ("AVAXUSDT", 2),
    "LTC": ("LTCUSDT", 2),
    "DOGE": ("DOGEUSDT", 5),
    "SHIB": ("SHIBUSDT", 8),
    "PEPE": ("PEPEUSDT", 8),
    "BONK": ("BONKUSDT", 8),
    "WIF": ("WIFUSDT", 4),
    "HYPE": ("HYPEUSDT", 4),
    "SUI": ("SUIUSDT", 4),
    "NEAR": ("NEARUSDT", 3),
    "APT": ("APTUSDT", 2),
    "ARB": ("ARBUSDT", 4),
    "OP": ("OPUSDT", 4),
}

# Chainlink 支援的資產子集（有 ChainLink price feed 的）
_CHAINLINK_SUPPORTED_ASSETS = {
    "BTC": ("BTCUSD", 8),
    "ETH": ("ETHUSD", 8),
    "SOL": ("SOLUSD", 8),
    "LINK": ("LINKUSD", 8),
    "AVAX": ("AVAXUSD", 8),
    "DOGE": ("DOGEUSD", 8),
    "MATIC": ("MATICUSD", 8),
}


def _build_oracle_mapping() -> dict:
    """動態建立 ORACLE_MAPPING。"""
    mapping = {}
    for asset, (symbol, decimals) in _BINANCE_SUPPORTED_ASSETS.items():
        primary = OracleConfig(OracleFamily.BINANCE, symbol, decimals)
        fallback = None
        if asset in _CHAINLINK_SUPPORTED_ASSETS:
            cl_symbol, cl_decimals = _CHAINLINK_SUPPORTED_ASSETS[asset]
            fallback = OracleConfig(OracleFamily.CHAINLINK, cl_symbol, cl_decimals)
        mapping[(asset, OracleFamily.BINANCE)] = (primary, fallback)

        if asset in _CHAINLINK_SUPPORTED_ASSETS:
            cl_symbol, cl_decimals = _CHAINLINK_SUPPORTED_ASSETS[asset]
            cl_primary = OracleConfig(OracleFamily.CHAINLINK, cl_symbol, cl_decimals)
            cl_fallback = OracleConfig(OracleFamily.BINANCE, symbol, decimals)
            mapping[(asset, OracleFamily.CHAINLINK)] = (cl_primary, cl_fallback)
    return mapping


ORACLE_MAPPING = _build_oracle_mapping()


# ============================================================================
# Regex Patterns for Parsing
# ============================================================================

# 價格匹配（排除時間格式如 12:00, 12:00AM）
PRICE_PATTERN = re.compile(
    r"\$([\d,]+(?:\.\d+)?)\s*(k|thousand|m|million)?(?![\s]*\d*:\d+)",  # $123 或 $1,234
    re.IGNORECASE,
)

# 單純數字價格（前面有貨幣符號或特定關鍵詞）
STANDALONE_PRICE_PATTERN = re.compile(
    r"(?:at|above|below|between)\s+\$?([\d,]+(?:\.\d+)?)\s*(k|thousand|m|million)?(?=\s|$|,)",
    re.IGNORECASE,
)

# 資產識別
ASSET_PATTERNS = {
    "BTC": re.compile(r"\b(?:bitcoin|btc|xbt)\b", re.IGNORECASE),
    "ETH": re.compile(r"\b(?:ethereum|ether|eth)\b", re.IGNORECASE),
    "SOL": re.compile(r"\b(?:solana|sol)\b", re.IGNORECASE),
    "ADA": re.compile(r"\b(?:cardano|ada)\b", re.IGNORECASE),
    "XRP": re.compile(r"\b(?:ripple|xrp)\b", re.IGNORECASE),
    "DOT": re.compile(r"\b(?:polkadot|dot)\b", re.IGNORECASE),
    "MATIC": re.compile(r"\b(?:polygon|matic|pol)\b", re.IGNORECASE),
    "LINK": re.compile(r"\b(?:chainlink|link)\b", re.IGNORECASE),
    "AVAX": re.compile(r"\b(?:avalanche|avax)\b", re.IGNORECASE),
    "LTC": re.compile(r"\b(?:litecoin|ltc)\b", re.IGNORECASE),
    "DOGE": re.compile(r"\b(?:dogecoin|doge)\b", re.IGNORECASE),
    "SHIB": re.compile(r"\b(?:shiba|shib)\b", re.IGNORECASE),
    "PEPE": re.compile(r"\b(?:pepe)\b", re.IGNORECASE),
    "BONK": re.compile(r"\b(?:bonk)\b", re.IGNORECASE),
    "WIF": re.compile(r"\b(?:wif)\b", re.IGNORECASE),
    "HYPE": re.compile(r"\b(?:hyperliquid|hype)\b", re.IGNORECASE),
    "SUI": re.compile(r"\b(?:sui)\b", re.IGNORECASE),
    "NEAR": re.compile(r"\b(?:near)\b", re.IGNORECASE),
    "APT": re.compile(r"\b(?:aptos|apt)\b", re.IGNORECASE),
    "ARB": re.compile(r"\b(?:arbitrum|arb)\b", re.IGNORECASE),
    "OP": re.compile(r"\b(?:optimism|op)\b", re.IGNORECASE),
}

# Payoff 類型識別
PAYOFF_PATTERNS = {
    PayoffType.DIGITAL_ABOVE: [
        re.compile(r"above|higher|up|over", re.IGNORECASE),
    ],
    PayoffType.DIGITAL_BELOW: [
        re.compile(r"below|lower|down|under", re.IGNORECASE),
    ],
    PayoffType.DIGITAL_INSIDE_RANGE: [
        re.compile(r"between\s+\$?\d+.*and\s+\$?\d+", re.IGNORECASE),
        re.compile(r"range|inside|within", re.IGNORECASE),
    ],
    PayoffType.TOUCH_UPPER: [
        re.compile(r"touch.*(above|upper)|hit.*\$?\d+", re.IGNORECASE),
    ],
    PayoffType.TOUCH_LOWER: [
        re.compile(r"touch.*(below|lower)", re.IGNORECASE),
    ],
}

# 邊界語義識別
BOUNDARY_PATTERNS = {
    ResolutionOperator.GTE: [
        re.compile(r"at\s+or\s+above|greater\s+than\s+or\s+equal", re.IGNORECASE),
    ],
    ResolutionOperator.LTE: [
        re.compile(r"at\s+or\s+below|less\s+than\s+or\s+equal", re.IGNORECASE),
    ],
    ResolutionOperator.GT: [
        re.compile(r"above|higher\s+than|over", re.IGNORECASE),
    ],
    ResolutionOperator.LT: [
        re.compile(r"below|lower\s+than|under", re.IGNORECASE),
    ],
    ResolutionOperator.INSIDE_LEFT_CLOSED: [
        re.compile(r"inclusive.*exclusive|\[.*\)", re.IGNORECASE),
    ],
    ResolutionOperator.INSIDE_RIGHT_CLOSED: [
        re.compile(r"exclusive.*inclusive|\(.*\]", re.IGNORECASE),
    ],
}


# ============================================================================
# Parser Functions
# ============================================================================


def _normalize_asset(asset_str: str) -> Optional[str]:
    """
    標準化 asset 名稱

    輸入: "bitcoin", "btc", "BTC", "BTCUSD", "SOL"
    輸出: "BTC" 或 None
    """
    if not asset_str:
        return None

    # 清理輸入
    cleaned = asset_str.strip().lower().replace("usdt", "").replace("usd", "")

    # 查映射表
    if cleaned in ASSET_SYNONYMS:
        return ASSET_SYNONYMS[cleaned]

    # 直接匹配已知資產標準名稱
    upper = cleaned.upper()
    if upper in _BINANCE_SUPPORTED_ASSETS:
        return upper

    return None


def _extract_asset_from_text(text: str) -> Optional[str]:
    """從文字中提取資產"""
    for asset, pattern in ASSET_PATTERNS.items():
        if pattern.search(text):
            return asset
    return None


def _extract_price(text: str) -> Optional[float]:
    """提取價格數值（排除時間格式）"""
    # 優先使用有貨幣符號的模式
    match = PRICE_PATTERN.search(text)
    if not match:
        # 其次使用 standalone 模式
        match = STANDALONE_PRICE_PATTERN.search(text)
        if not match:
            return None

    number_str = match.group(1).replace(",", "")
    try:
        number = float(number_str)
    except ValueError:
        return None

    # 過濾掉可能是時間的值（如 12:00 中的 12）
    if number < 100 and ":" in text[match.start() : match.start() + 10]:
        # 可能是時間，嘗試找下一個匹配
        next_match = PRICE_PATTERN.search(text, match.end())
        if next_match:
            return _extract_price(text[match.end() :])
        return None

    # 處理 k/thousand/m/million
    multiplier_str = match.group(2)
    if multiplier_str:
        multiplier_str = multiplier_str.lower()
        if multiplier_str in ("k", "thousand"):
            number *= 1_000
        elif multiplier_str in ("m", "million"):
            number *= 1_000_000

    return number


def _extract_two_prices(text: str) -> Tuple[Optional[float], Optional[float]]:
    """提取兩個價格（用於 range）"""
    # 使用更嚴格的模式避免時間匹配
    matches = list(PRICE_PATTERN.finditer(text))
    if len(matches) >= 2:
        values = []
        for match in matches[:2]:
            try:
                val = float(match.group(1).replace(",", ""))
                # 過濾可能是時間的值
                if (
                    val < 100
                    and ":" in text[match.start() : min(match.start() + 10, len(text))]
                ):
                    continue

                # 處理 multiplier
                mult = match.group(2)
                if mult:
                    mult = mult.lower()
                    if mult in ("k", "thousand"):
                        val *= 1_000
                    elif mult in ("m", "million"):
                        val *= 1_000_000
                values.append(val)
            except (ValueError, IndexError):
                continue

        if len(values) >= 2:
            return (min(values[0], values[1]), max(values[0], values[1]))

    return (None, None)


def parse_payoff_type(question: str, description: Optional[str]) -> PayoffSemantics:
    """
    解析 payoff 語義

    優先順序：description（較詳細）> question
    """
    texts_to_check = []
    if description:
        texts_to_check.append(("description", description))
    texts_to_check.append(("question", question))

    # 識別 payoff type
    payoff_type = None
    source_used = None

    for source, text in texts_to_check:
        for ptype, patterns in PAYOFF_PATTERNS.items():
            for pattern in patterns:
                if pattern.search(text):
                    payoff_type = ptype
                    source_used = source
                    break
            if payoff_type:
                break
        if payoff_type:
            break

    if not payoff_type:
        # 預設假設
        payoff_type = PayoffType.DIGITAL_ABOVE
        source_used = "default"

    # 識別 resolution operator
    resolution_operator = ResolutionOperator.GT  # 預設

    for source, text in texts_to_check:
        for op, patterns in BOUNDARY_PATTERNS.items():
            for pattern in patterns:
                if pattern.search(text):
                    resolution_operator = op
                    break
            if resolution_operator != ResolutionOperator.GT:
                break
        if resolution_operator != ResolutionOperator.GT:
            break

    # 識別 strike
    strike_value = None
    upper_strike_value = None

    if payoff_type in (PayoffType.DIGITAL_ABOVE, PayoffType.DIGITAL_BELOW):
        # 單一價格
        strike_value = _extract_price(texts_to_check[0][1])
        strike_type = StrikeType.FIXED_PRICE if strike_value else StrikeType.OPEN_PRICE
    elif payoff_type in (
        PayoffType.DIGITAL_INSIDE_RANGE,
        PayoffType.DIGITAL_OUTSIDE_RANGE,
    ):
        # 雙價格
        strike_value, upper_strike_value = _extract_two_prices(texts_to_check[0][1])
        strike_type = StrikeType.FIXED_PRICE if strike_value else StrikeType.OPEN_PRICE
    else:
        # TOUCH 類型
        strike_value = _extract_price(texts_to_check[0][1])
        strike_type = StrikeType.FIXED_PRICE if strike_value else StrikeType.OPEN_PRICE

    # 如果沒有提取到價格，預設為 OPEN_PRICE
    if strike_value is None:
        strike_type = StrikeType.OPEN_PRICE

    return PayoffSemantics(
        payoff_type=payoff_type,
        resolution_operator=resolution_operator,
        strike_type=strike_type,
        strike_value=strike_value,
        upper_strike_value=upper_strike_value,
    )


def extract_oracle_config(
    asset: str,
    market: Optional[dict] = None,
    preferred_family: OracleFamily = OracleFamily.BINANCE,
) -> Tuple[OracleConfig, Optional[OracleConfig]]:
    """
    提取 Oracle 配置

    回傳: (primary, fallback)
    對未知資產使用動態 {ASSET}USDT 兜底，不再拋異常。
    """
    # 標準化 asset
    normalized_asset = _normalize_asset(asset)
    if not normalized_asset:
        # 動態兜底：嘗試用大寫原始名稱 + USDT
        fallback_symbol = f"{asset.upper()}USDT"
        return (
            OracleConfig(OracleFamily.BINANCE, fallback_symbol, 4),
            None,
        )

    # 檢查是否有市場特定的來源提示
    if market and "oracleSource" in market:
        hint = market["oracleSource"].lower()
        if "chainlink" in hint:
            preferred_family = OracleFamily.CHAINLINK

    # 查映射表
    key = (normalized_asset, preferred_family)
    if key in ORACLE_MAPPING:
        primary, fallback = ORACLE_MAPPING[key]
        return primary, fallback

    # 動態兜底：已正規化但沒有靜態映射的資產
    fallback_symbol = f"{normalized_asset}USDT"
    return (
        OracleConfig(OracleFamily.BINANCE, fallback_symbol, 4),
        None,
    )


def _ensure_utc_datetime(dt: Any) -> Optional[datetime]:
    """
    確保 datetime 是 timezone-aware UTC

    拒絕 naive datetime
    """
    if dt is None:
        return None

    if isinstance(dt, str):
        # 解析 ISO8601
        dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))

    if not isinstance(dt, datetime):
        return None

    # 拒絕 naive datetime
    if dt.tzinfo is None:
        return None

    # 轉 UTC
    return dt.astimezone(timezone.utc)


def _detect_market_style(question: str) -> str:
    """從題目判斷市場風格。"""
    question_lower = question.lower()
    above_below_keywords = [
        "above",
        "below",
        "higher than",
        "lower than",
        "greater than",
        "less than",
        ">",
        "<",
    ]
    if any(keyword in question_lower for keyword in above_below_keywords):
        return "ABOVE_BELOW"
    up_down_keywords = ["up", "down", "rise", "fall", "higher", "lower", "green", "red"]
    if any(keyword in question_lower for keyword in up_down_keywords):
        return "UP_DOWN"
    return "UNKNOWN"


def _detect_timeframe(question: str) -> Optional[str]:
    """從題目判斷市場週期。"""
    question_lower = question.lower()
    patterns = {
        "1m": ["1 minute", "1 min", "1m "],
        "5m": ["5 minute", "5 min", "5m "],
        "15m": ["15 minute", "15 min", "15m "],
        "30m": ["30 minute", "30 min", "30m "],
        "1h": ["1 hour", "1h", "hourly", "next hour"],
        "4h": ["4 hour", "4h"],
        "12h": ["12 hour", "12h"],
        "1d": ["1 day", "1d", "daily", "24 hour", "24h"],
    }
    for timeframe, candidates in patterns.items():
        if any(candidate in question_lower for candidate in candidates):
            return timeframe
    range_match = re.search(
        r"(\d{1,2}):(\d{2})\s*(am|pm)\s*-\s*(\d{1,2}):(\d{2})\s*(am|pm)",
        question,
        re.IGNORECASE,
    )
    if range_match:
        start_hour = int(range_match.group(1)) % 12
        start_minute = int(range_match.group(2))
        start_period = range_match.group(3).lower()
        end_hour = int(range_match.group(4)) % 12
        end_minute = int(range_match.group(5))
        end_period = range_match.group(6).lower()
        if start_period == "pm":
            start_hour += 12
        if end_period == "pm":
            end_hour += 12
        start_total_minutes = start_hour * 60 + start_minute
        end_total_minutes = end_hour * 60 + end_minute
        if end_total_minutes < start_total_minutes:
            end_total_minutes += 24 * 60
        duration_minutes = end_total_minutes - start_total_minutes
        duration_to_timeframe = {
            5: "5m",
            15: "15m",
            30: "30m",
            60: "1h",
            240: "4h",
            1440: "1d",
        }
        return duration_to_timeframe.get(duration_minutes)
    return None


def _build_settlement_source_descriptor(
    market: dict,
    primary_oracle: OracleConfig,
) -> str:
    """建立結算來源描述字串。"""
    raw_source = market.get("oracleSource") or primary_oracle.family.value
    return f"{raw_source}:{primary_oracle.symbol}"


def build_market_definition(market: dict) -> MarketDefinitionResult:
    """
    主入口：從 Polymarket market object 建立完整定義

    回傳 MarketDefinitionResult，包含 definition, success, warnings, parse_confidence
    """
    warnings: List[str] = []
    parse_notes_parts: List[str] = []
    parse_confidence = 1.0

    try:
        # 提取基本資訊
        market_id = market.get("conditionId") or market.get("id")
        if not market_id:
            return MarketDefinitionResult(
                definition=None,
                success=False,
                warnings=[WarningCode.PAYOFF_PARSE_FAILED.value],
                parse_confidence=0.0,
                parse_notes="Missing market_id",
            )

        question = market.get("question", "")
        description = market.get("description")

        # 提取資產
        asset = _extract_asset_from_text(question)
        if not asset and description:
            asset = _extract_asset_from_text(description)

        if not asset:
            return MarketDefinitionResult(
                definition=None,
                success=False,
                warnings=[WarningCode.ASSET_NOT_RECOGNIZED.value],
                parse_confidence=0.0,
                parse_notes="Could not extract asset from question or description",
            )

        # 解析 payoff 語義
        semantics = parse_payoff_type(question, description)
        parse_notes_parts.append(
            f"payoff_type from {'description' if description else 'question'}"
        )

        # 檢查是否用了 description 覆蓋
        if description and semantics.payoff_type in (
            PayoffType.DIGITAL_ABOVE,
            PayoffType.DIGITAL_BELOW,
        ):
            q_match = (
                any(
                    p.search(question)
                    for p in PAYOFF_PATTERNS.get(semantics.payoff_type, [])
                )
                if "PAYOFF_PATTERNS" in dir()
                else False
            )
            d_match = any(
                p.search(description)
                for p in PAYOFF_PATTERNS.get(semantics.payoff_type, [])
            )
            if not q_match and d_match:
                warnings.append(WarningCode.DESCRIPTION_OVERRIDES_QUESTION.value)
                parse_notes_parts.append(
                    "description provided clearer semantics than question"
                )

        # 提取時間
        end_date_str = market.get("endDate") or market.get("expiration")
        if not end_date_str:
            return MarketDefinitionResult(
                definition=None,
                success=False,
                warnings=[WarningCode.UNSUPPORTED_SETTLEMENT.value],
                parse_confidence=0.0,
                parse_notes="Missing endDate/expiration",
            )

        expiry = _ensure_utc_datetime(end_date_str)
        if expiry is None:
            return MarketDefinitionResult(
                definition=None,
                success=False,
                warnings=[WarningCode.NAIVE_DATETIME_REJECTED.value],
                parse_confidence=0.0,
                parse_notes="Invalid or naive datetime for expiry",
            )

        market_style = _detect_market_style(question)
        timeframe = _detect_timeframe(f"{question} {description or ''}")
        timeframe_seconds = TIMEFRAME_SECONDS.get(timeframe) if timeframe else None
        market_start_timestamp = None
        strike_ts = expiry
        if semantics.strike_type == StrikeType.OPEN_PRICE:
            if timeframe_seconds is None:
                return MarketDefinitionResult(
                    definition=None,
                    success=False,
                    warnings=[WarningCode.PAYOFF_PARSE_FAILED.value],
                    parse_confidence=0.0,
                    parse_notes="UP/DOWN open strike market requires timeframe",
                )
            from datetime import timedelta

            market_start_timestamp = expiry - timedelta(seconds=timeframe_seconds)
            strike_ts = market_start_timestamp
        else:
            from datetime import timedelta

            strike_ts = expiry - timedelta(seconds=60)

        # 提取 token IDs
        token_ids = market.get("clobTokenIds", [])
        if len(token_ids) >= 2:
            yes_token_id = token_ids[0]
            no_token_id = token_ids[1]
        else:
            yes_token_id = market.get("yesTokenId", "")
            no_token_id = market.get("noTokenId", "")

        # 提取 Oracle 配置
        try:
            primary_oracle, fallback_oracle = extract_oracle_config(asset, market)
        except ValueError as e:
            return MarketDefinitionResult(
                definition=None,
                success=False,
                warnings=[WarningCode.ASSET_NOT_RECOGNIZED.value],
                parse_confidence=0.0,
                parse_notes=str(e),
            )

        if fallback_oracle:
            warnings.append(WarningCode.FALLBACK_ORACLE_ASSIGNED.value)

        settlement_source_descriptor = _build_settlement_source_descriptor(
            market, primary_oracle
        )

        # 判斷 settlement rule
        if semantics.payoff_type in (PayoffType.TOUCH_UPPER, PayoffType.TOUCH_LOWER):
            settlement_rule = SettlementRule.HIGH_LOW
        elif "twap" in (description or "").lower():
            settlement_rule = SettlementRule.TWAP
        else:
            settlement_rule = SettlementRule.TERMINAL_PRICE

        # 判斷 strike window（TWAP 時需要）
        strike_window = None
        if semantics.strike_type == StrikeType.TWAP_REFERENCE:
            strike_window = 300  # 預設 5 分鐘
            # 嘗試從描述解析
            twap_match = re.search(
                r"(\d+)\s*minute", (description or ""), re.IGNORECASE
            )
            if twap_match:
                strike_window = int(twap_match.group(1)) * 60

        # 組裝 MarketDefinition
        definition = MarketDefinition(
            market_id=market_id,
            asset=asset,
            payoff_type=semantics.payoff_type,
            resolution_operator=semantics.resolution_operator,
            strike_type=semantics.strike_type,
            strike_value=semantics.strike_value,
            upper_strike_value=semantics.upper_strike_value,
            strike_timestamp=strike_ts,
            strike_window_seconds=strike_window,
            expiry_timestamp=expiry,
            settlement_rule=settlement_rule,
            oracle_family=primary_oracle.family,
            oracle_symbol=primary_oracle.symbol,
            oracle_decimals=primary_oracle.decimals,
            fallback_oracle_family=fallback_oracle.family if fallback_oracle else None,
            fallback_oracle_symbol=fallback_oracle.symbol if fallback_oracle else None,
            fallback_oracle_decimals=fallback_oracle.decimals
            if fallback_oracle
            else None,
            fee_enabled=market.get("feesEnabled", True),
            yes_token_id=yes_token_id,
            no_token_id=no_token_id,
            raw_question=question,
            raw_description=description,
            market_style=market_style if market_style != "UNKNOWN" else "ABOVE_BELOW",
            timeframe=timeframe,
            timeframe_seconds=timeframe_seconds,
            market_start_timestamp=market_start_timestamp,
            settlement_source_descriptor=settlement_source_descriptor,
            anchor_required=semantics.strike_type == StrikeType.OPEN_PRICE,
        )

        # 驗證
        validation_errors = validate_market_definition(definition)
        if validation_errors:
            return MarketDefinitionResult(
                definition=None,
                success=False,
                warnings=validation_errors,
                parse_confidence=0.5,
                parse_notes="; ".join(parse_notes_parts),
            )

        return MarketDefinitionResult(
            definition=definition,
            success=True,
            warnings=warnings,
            parse_confidence=parse_confidence,
            parse_notes="; ".join(parse_notes_parts),
        )

    except Exception as e:
        return MarketDefinitionResult(
            definition=None,
            success=False,
            warnings=[WarningCode.PAYOFF_PARSE_FAILED.value],
            parse_confidence=0.0,
            parse_notes=f"Exception: {str(e)}",
        )


def validate_market_definition(definition: MarketDefinition) -> List[str]:
    """
    驗證 MarketDefinition 的內部一致性

    檢查項目：
    1. payoff_type 與 settlement_rule 相容性
    2. range payoff 必須有 upper_strike_value
    3. TWAP 必須有 strike_window_seconds
    4. strike_timestamp < expiry_timestamp（不能相等）
    5. datetime 都有 timezone
    6. FIXED_PRICE 時 strike_value 必填
    """
    errors: List[str] = []

    # 1. payoff 與 settlement 相容矩陣
    VALID_COMBINATIONS = {
        # payoff_type: [valid settlement_rules]
        PayoffType.DIGITAL_ABOVE: [SettlementRule.TERMINAL_PRICE, SettlementRule.TWAP],
        PayoffType.DIGITAL_BELOW: [SettlementRule.TERMINAL_PRICE, SettlementRule.TWAP],
        PayoffType.DIGITAL_INSIDE_RANGE: [
            SettlementRule.TERMINAL_PRICE,
            SettlementRule.TWAP,
        ],
        PayoffType.DIGITAL_OUTSIDE_RANGE: [
            SettlementRule.TERMINAL_PRICE,
            SettlementRule.TWAP,
        ],
        PayoffType.TOUCH_UPPER: [SettlementRule.HIGH_LOW],
        PayoffType.TOUCH_LOWER: [SettlementRule.HIGH_LOW],
    }

    valid_settlements = VALID_COMBINATIONS.get(definition.payoff_type, [])
    if definition.settlement_rule not in valid_settlements:
        errors.append(
            f"INCOMPATIBLE_SETTLEMENT: {definition.payoff_type.value} "
            f"cannot use {definition.settlement_rule.value}, "
            f"valid: {[s.value for s in valid_settlements]}"
        )

    # 2. range 必須有上界
    if definition.payoff_type in (
        PayoffType.DIGITAL_INSIDE_RANGE,
        PayoffType.DIGITAL_OUTSIDE_RANGE,
    ):
        if definition.upper_strike_value is None:
            errors.append(
                "MISSING_UPPER_STRIKE: range payoff requires upper_strike_value"
            )
        if definition.strike_value is None:
            errors.append("MISSING_LOWER_STRIKE: range payoff requires strike_value")

    # 3. TWAP 必須有 window
    if definition.strike_type == StrikeType.TWAP_REFERENCE:
        if definition.strike_window_seconds is None:
            errors.append(
                "MISSING_TWAP_WINDOW: TWAP reference requires strike_window_seconds"
            )

    # 4. FIXED_PRICE 必須有 strike_value
    if definition.strike_type == StrikeType.FIXED_PRICE:
        if definition.strike_value is None:
            errors.append("MISSING_STRIKE_VALUE: FIXED_PRICE requires strike_value")

    # 5. 時間順序：strike 必須嚴格小於 expiry
    if definition.strike_timestamp >= definition.expiry_timestamp:
        errors.append(
            f"INVALID_TIME_ORDER: strike_timestamp ({definition.strike_timestamp}) "
            f"must be < expiry_timestamp ({definition.expiry_timestamp})"
        )

    # 6. datetime 必須有 timezone
    if definition.strike_timestamp.tzinfo is None:
        errors.append("NAIVE_DATETIME: strike_timestamp must be timezone-aware")
    if definition.expiry_timestamp.tzinfo is None:
        errors.append("NAIVE_DATETIME: expiry_timestamp must be timezone-aware")

    return errors


# ============================================================================
# Serialization
# ============================================================================


def market_definition_to_dict(definition: MarketDefinition) -> dict:
    """
    序列化 MarketDefinition，時間轉 ISO8601 UTC
    """
    return {
        "market_id": definition.market_id,
        "asset": definition.asset,
        "payoff_type": definition.payoff_type.value,
        "resolution_operator": definition.resolution_operator.value,
        "strike_type": definition.strike_type.value,
        "strike_value": definition.strike_value,
        "upper_strike_value": definition.upper_strike_value,
        "strike_timestamp": definition.strike_timestamp.isoformat(),
        "strike_window_seconds": definition.strike_window_seconds,
        "expiry_timestamp": definition.expiry_timestamp.isoformat(),
        "settlement_rule": definition.settlement_rule.value,
        "oracle_family": definition.oracle_family.value,
        "oracle_symbol": definition.oracle_symbol,
        "oracle_decimals": definition.oracle_decimals,
        "fallback_oracle_family": definition.fallback_oracle_family.value
        if definition.fallback_oracle_family
        else None,
        "fallback_oracle_symbol": definition.fallback_oracle_symbol,
        "fallback_oracle_decimals": definition.fallback_oracle_decimals,
        "fee_enabled": definition.fee_enabled,
        "yes_token_id": definition.yes_token_id,
        "no_token_id": definition.no_token_id,
        "raw_question": definition.raw_question,
        "raw_description": definition.raw_description,
        "market_style": definition.market_style,
        "timeframe": definition.timeframe,
        "timeframe_seconds": definition.timeframe_seconds,
        "market_start_timestamp": definition.market_start_timestamp.isoformat()
        if definition.market_start_timestamp
        else None,
        "settlement_source_descriptor": definition.settlement_source_descriptor,
        "anchor_required": definition.anchor_required,
    }


def market_definition_from_dict(data: dict) -> MarketDefinition:
    """
    反序列化 MarketDefinition，時間解析為 timezone-aware UTC

    拒絕 naive datetime
    """
    # 解析時間
    strike_ts = datetime.fromisoformat(data["strike_timestamp"])
    expiry_ts = datetime.fromisoformat(data["expiry_timestamp"])

    # 確保 timezone-aware
    if strike_ts.tzinfo is None:
        raise ValueError("strike_timestamp must be timezone-aware")
    if expiry_ts.tzinfo is None:
        raise ValueError("expiry_timestamp must be timezone-aware")

    # 解析 fallback oracle
    fallback_family = None
    if data.get("fallback_oracle_family"):
        fallback_family = OracleFamily(data["fallback_oracle_family"])

    return MarketDefinition(
        market_id=data["market_id"],
        asset=data["asset"],
        payoff_type=PayoffType(data["payoff_type"]),
        resolution_operator=ResolutionOperator(data["resolution_operator"]),
        strike_type=StrikeType(data["strike_type"]),
        strike_value=data.get("strike_value"),
        upper_strike_value=data.get("upper_strike_value"),
        strike_timestamp=strike_ts,
        strike_window_seconds=data.get("strike_window_seconds"),
        expiry_timestamp=expiry_ts,
        settlement_rule=SettlementRule(data["settlement_rule"]),
        oracle_family=OracleFamily(data["oracle_family"]),
        oracle_symbol=data["oracle_symbol"],
        oracle_decimals=data["oracle_decimals"],
        fallback_oracle_family=fallback_family,
        fallback_oracle_symbol=data.get("fallback_oracle_symbol"),
        fallback_oracle_decimals=data.get("fallback_oracle_decimals"),
        fee_enabled=data["fee_enabled"],
        yes_token_id=data["yes_token_id"],
        no_token_id=data["no_token_id"],
        raw_question=data["raw_question"],
        raw_description=data.get("raw_description"),
        market_style=data.get("market_style", "ABOVE_BELOW"),
        timeframe=data.get("timeframe"),
        timeframe_seconds=data.get("timeframe_seconds"),
        market_start_timestamp=_ensure_utc_datetime(data.get("market_start_timestamp")),
        settlement_source_descriptor=data.get("settlement_source_descriptor"),
        anchor_required=bool(data.get("anchor_required", False)),
    )
