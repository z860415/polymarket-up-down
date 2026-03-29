"""
Package B - Reference Price Builder

建立可審計、可評分、可降級的 reference truth layer。
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
import bisect


# ============================================================================
# Enums
# ============================================================================

class ReferenceMethod(str, Enum):
    """參考價格計算方法"""
    WINDOW_FIRST_TICK = "window_first_tick"
    WINDOW_NEAREST_TICK = "window_nearest_tick"
    VWAP = "vwap"
    INTERPOLATED = "interpolated"
    FAILED = "failed"


class ReferenceStatus(str, Enum):
    """Reference 狀態"""
    PROVISIONAL = "provisional"  # target time 已到，但 window 尚未完整收齊
    FINALIZED = "finalized"      # target window 已完整關閉，reference 不再改變
    FAILED = "failed"            # 在既定等待條件後仍無法建立


class WarningCode(str, Enum):
    """Reference 建立警告"""
    NO_TICKS_IN_WINDOW = "NO_TICKS_IN_WINDOW"
    INTERPOLATED_FROM_OUTSIDE_WINDOW = "INTERPOLATED_FROM_OUTSIDE_WINDOW"
    USED_FALLBACK_SOURCE = "USED_FALLBACK_SOURCE"
    STALE_REFERENCE = "STALE_REFERENCE"
    CROSS_SOURCE_MISMATCH = "CROSS_SOURCE_MISMATCH"
    ONLY_SINGLE_SIDE_AVAILABLE = "ONLY_SINGLE_SIDE_AVAILABLE"
    NAIVE_DATETIME_REJECTED = "NAIVE_DATETIME_REJECTED"
    OUT_OF_ORDER_TICKS_NORMALIZED = "OUT_OF_ORDER_TICKS_NORMALIZED"
    LOW_QUALITY_REFERENCE = "LOW_QUALITY_REFERENCE"
    VWAP_VOLUME_MISSING = "VWAP_VOLUME_MISSING"


# ============================================================================
# Oracle Family (reuse from market_definition or define here for independence)
# ============================================================================

class OracleFamily(str, Enum):
    """Oracle 來源家族"""
    BINANCE = "binance"
    CHAINLINK = "chainlink"


# ============================================================================
# Data Classes
# ============================================================================

@dataclass(frozen=True)
class Tick:
    """單個 tick 數據"""
    timestamp: datetime      # 數據時間（timezone-aware UTC）
    price: float
    volume: Optional[float]  # None 時 VWAP 不可用


@dataclass(frozen=True)
class ReferencePrice:
    """可審計的參考價格"""
    
    # 價格
    value: Optional[float]
    
    # 來源
    source: OracleFamily
    symbol: str
    
    # 計算與狀態
    method: ReferenceMethod
    status: ReferenceStatus
    # status 定義：
    # - provisional: target time 已到，但 window 尚未完整收齊
    # - finalized: target window 已完整關閉，reference 不再改變
    # - failed: 在既定等待條件後仍無法建立
    
    # 時間
    target_timestamp: datetime
    source_timestamp: Optional[datetime]  # 實際使用的 tick 時間
    left_timestamp: Optional[datetime]    # 插值左支撐點
    right_timestamp: Optional[datetime]   # 插值右支撐點
    window_start: datetime
    window_end: datetime
    
    # 數據量
    num_ticks_in_window: int      # window 內 tick 數量
    num_ticks_total: int          # 考慮的所有 tick（含窗口外用於插值的）
    
    # 品質評分（全部正向 0.0-1.0）
    quality_score: float
    quality_components: Dict[str, float]  # temporal_proximity, tick_density, method_score, freshness_score
    
    # 警告（統一使用 WarningCode）
    warnings: List[WarningCode]
    
    # 序列化時保留原始參數
    prefer_method: ReferenceMethod
    allow_interpolation: bool
    
    def to_dict(self) -> Dict[str, Any]:
        """序列化為字典"""
        return {
            "value": self.value,
            "source": self.source.value,
            "symbol": self.symbol,
            "method": self.method.value,
            "status": self.status.value,
            "target_timestamp": self.target_timestamp.isoformat(),
            "source_timestamp": self.source_timestamp.isoformat() if self.source_timestamp else None,
            "left_timestamp": self.left_timestamp.isoformat() if self.left_timestamp else None,
            "right_timestamp": self.right_timestamp.isoformat() if self.right_timestamp else None,
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "num_ticks_in_window": self.num_ticks_in_window,
            "num_ticks_total": self.num_ticks_total,
            "quality_score": self.quality_score,
            "quality_components": self.quality_components,
            "warnings": [w.value for w in self.warnings],
            "prefer_method": self.prefer_method.value,
            "allow_interpolation": self.allow_interpolation,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ReferencePrice":
        """從字典反序列化"""
        def parse_dt(s: Optional[str]) -> Optional[datetime]:
            return datetime.fromisoformat(s) if s else None
        
        return cls(
            value=data["value"],
            source=OracleFamily(data["source"]),
            symbol=data["symbol"],
            method=ReferenceMethod(data["method"]),
            status=ReferenceStatus(data["status"]),
            target_timestamp=datetime.fromisoformat(data["target_timestamp"]),
            source_timestamp=parse_dt(data.get("source_timestamp")),
            left_timestamp=parse_dt(data.get("left_timestamp")),
            right_timestamp=parse_dt(data.get("right_timestamp")),
            window_start=datetime.fromisoformat(data["window_start"]),
            window_end=datetime.fromisoformat(data["window_end"]),
            num_ticks_in_window=data["num_ticks_in_window"],
            num_ticks_total=data["num_ticks_total"],
            quality_score=data["quality_score"],
            quality_components=data["quality_components"],
            warnings=[WarningCode(w) for w in data["warnings"]],
            prefer_method=ReferenceMethod(data["prefer_method"]),
            allow_interpolation=data["allow_interpolation"],
        )


# ============================================================================
# ReferencePriceBuilder
# ============================================================================

class ReferencePriceBuilder:
    """單一來源參考價格建立器"""
    
    def __init__(
        self,
        max_buffer_size: int = 10000,
        tick_density_full_threshold: Optional[int] = None,  # None = auto (window_seconds/2)
    ):
        self._buffers: Dict[Tuple[OracleFamily, str], List[Tick]] = {}
        self._max_size = max_buffer_size
        self._tick_density_full = tick_density_full_threshold
    
    def _ensure_utc_datetime(self, dt: datetime) -> datetime:
        """確保 datetime 是 timezone-aware UTC，拒絕 naive"""
        if dt.tzinfo is None:
            raise ValueError(f"Naive datetime not allowed: {dt}")
        return dt.astimezone(timezone.utc)
    
    def _get_buffer(self, source: OracleFamily, symbol: str) -> List[Tick]:
        """獲取或創建 buffer，symbol 標準化為大寫"""
        key = (source, symbol.strip().upper())
        if key not in self._buffers:
            self._buffers[key] = []
        return self._buffers[key]
    
    def add_tick(
        self,
        source: OracleFamily,
        symbol: str,
        timestamp: datetime,
        price: float,
        volume: Optional[float] = None,
    ) -> List[WarningCode]:
        """
        存儲 tick，強制要求 timezone-aware UTC。
        
        VWAP 說明：
        - 若 volume is None，VWAP 不可用
        - 請求 VWAP 但 volume 缺失時，降級為 window_nearest_tick，並標記 VWAP_VOLUME_MISSING
        
        回傳: warnings list
        """
        warnings: List[WarningCode] = []
        
        # 檢查 naive datetime
        if timestamp.tzinfo is None:
            warnings.append(WarningCode.NAIVE_DATETIME_REJECTED)
            raise ValueError(f"Naive datetime rejected: {timestamp}")
        
        # 標準化並存儲
        timestamp = self._ensure_utc_datetime(timestamp)
        symbol = symbol.strip().upper()
        
        tick = Tick(timestamp=timestamp, price=price, volume=volume)
        buffer = self._get_buffer(source, symbol)
        
        # 插入排序位置（保持有序）
        idx = bisect.bisect_right(buffer, tick.timestamp, key=lambda t: t.timestamp)
        buffer.insert(idx, tick)
        
        # 檢查是否亂序
        if idx > 0 and buffer[idx-1].timestamp > tick.timestamp:
            warnings.append(WarningCode.OUT_OF_ORDER_TICKS_NORMALIZED)
        
        # 限制 buffer 大小
        if len(buffer) > self._max_size:
            buffer.pop(0)  # 移除最舊的
        
        return warnings
    
    def _calculate_quality_score(
        self,
        method: ReferenceMethod,
        num_ticks: int,
        staleness_ms: float,
        window_seconds: int,
    ) -> Tuple[float, Dict[str, float]]:
        """
        計算品質分數，全部正向 0.0-1.0。
        
        Components:
        - temporal_proximity: 1 - min(staleness_ms / 5000, 1.0)
        - tick_density: min(num_ticks / expected_ticks, 1.0)
          expected_ticks = tick_density_full or (window_seconds / 2)
        - method_score: 根據 method 給分
        - freshness_score: staleness < 1000ms 滿分，之後線性遞減
        """
        # 計算期望 tick 數
        expected_ticks = self._tick_density_full or (window_seconds / 2)
        expected_ticks = max(1, expected_ticks)  # 至少 1
        
        components = {
            "temporal_proximity": max(0.0, 1.0 - min(staleness_ms / 5000, 1.0)),
            "tick_density": min(num_ticks / expected_ticks, 1.0),
            "method_score": {
                ReferenceMethod.WINDOW_FIRST_TICK: 0.95,
                ReferenceMethod.WINDOW_NEAREST_TICK: 1.0,
                ReferenceMethod.VWAP: 1.0,
                ReferenceMethod.INTERPOLATED: 0.7,
                ReferenceMethod.FAILED: 0.0,
            }[method],
            "freshness_score": 1.0 if staleness_ms < 1000 else max(0.0, 1.0 - (staleness_ms - 1000) / 4000),
        }
        
        # 加權平均
        weights = {
            "temporal_proximity": 0.35,
            "tick_density": 0.25,
            "method_score": 0.25,
            "freshness_score": 0.15,
        }
        
        total = sum(components[k] * weights[k] for k in weights)
        return round(min(total, 1.0), 4), components
    
    def _find_window_ticks(
        self,
        buffer: List[Tick],
        window_start: datetime,
        window_end: datetime,
    ) -> Tuple[List[Tick], List[Tick], List[Tick]]:
        """
        將 ticks 分為三組：窗口內、窗口前、窗口後
        
        回傳: (ticks_in_window, ticks_before, ticks_after)
        """
        ticks_in_window = []
        ticks_before = []
        ticks_after = []
        
        for tick in buffer:
            if tick.timestamp < window_start:
                ticks_before.append(tick)
            elif tick.timestamp > window_end:
                ticks_after.append(tick)
            else:
                ticks_in_window.append(tick)
        
        return ticks_in_window, ticks_before, ticks_after
    
    def _interpolate_price(
        self,
        target: datetime,
        left_tick: Optional[Tick],
        right_tick: Optional[Tick],
    ) -> Tuple[Optional[float], Optional[datetime], Optional[datetime]]:
        """
        線性插值計算目標時間的價格
        
        回傳: (price, left_timestamp, right_timestamp)
        """
        if left_tick is None or right_tick is None:
            return None, None, None
        
        # 如果 target 剛好等於某一點，直接回傳
        if left_tick.timestamp == target:
            return left_tick.price, left_tick.timestamp, None
        if right_tick.timestamp == target:
            return right_tick.price, None, right_tick.timestamp
        
        # 線性插值
        time_diff = (right_tick.timestamp - left_tick.timestamp).total_seconds()
        if time_diff == 0:
            return left_tick.price, left_tick.timestamp, None
        
        target_diff = (target - left_tick.timestamp).total_seconds()
        ratio = target_diff / time_diff
        
        price = left_tick.price + ratio * (right_tick.price - left_tick.price)
        return price, left_tick.timestamp, right_tick.timestamp
    
    def build_reference_price(
        self,
        source: OracleFamily,
        symbol: str,
        target_time: datetime,
        window_seconds: int,
        prefer_method: ReferenceMethod = ReferenceMethod.WINDOW_FIRST_TICK,
        allow_interpolation: bool = True,
        finalize_after_window_close: bool = True,
    ) -> ReferencePrice:
        """
        建立單一來源參考價格。
        
        策略優先級（allow_interpolation=True 時）：
        1. window_first_tick: 窗口內第一筆
        2. window_nearest_tick: 窗口內最接近目標時間
        3. vwap: 窗口內成交量加權（volume 缺失時降級）
        4. interpolated: 前後插值（需 allow_interpolation=True）
        5. failed: 完全無法建立
        
        策略優先級（allow_interpolation=False 時）：
        1-3 同上
        4. failed: 窗口內無 tick 即失敗
        """
        warnings: List[WarningCode] = []
        
        # 確保時間有效
        target_time = self._ensure_utc_datetime(target_time)
        now = datetime.now(timezone.utc)
        
        # 計算窗口
        window_start = target_time - timedelta(seconds=window_seconds)
        window_end = target_time + timedelta(seconds=window_seconds)
        
        # 獲取 buffer
        buffer = self._get_buffer(source, symbol)
        
        # 分類 ticks
        ticks_in_window, ticks_before, ticks_after = self._find_window_ticks(
            buffer, window_start, window_end
        )
        
        # 統計
        num_ticks_in_window = len(ticks_in_window)
        num_ticks_total = len(buffer)
        
        # 計算 staleness
        staleness_ms = abs((now - target_time).total_seconds() * 1000)
        
        # 嘗試建立 reference
        value: Optional[float] = None
        method = ReferenceMethod.FAILED
        source_timestamp: Optional[datetime] = None
        left_ts: Optional[datetime] = None
        right_ts: Optional[datetime] = None
        
        # 策略 1-3: 窗口內有 tick
        if ticks_in_window:
            if prefer_method == ReferenceMethod.WINDOW_FIRST_TICK:
                tick = ticks_in_window[0]
                value = tick.price
                source_timestamp = tick.timestamp
                method = ReferenceMethod.WINDOW_FIRST_TICK
                
            elif prefer_method == ReferenceMethod.WINDOW_NEAREST_TICK:
                # 找到最接近 target_time 的 tick
                tick = min(ticks_in_window, key=lambda t: abs((t.timestamp - target_time).total_seconds()))
                value = tick.price
                source_timestamp = tick.timestamp
                method = ReferenceMethod.WINDOW_NEAREST_TICK
                
            elif prefer_method == ReferenceMethod.VWAP:
                # 檢查是否有 volume
                if all(t.volume is not None for t in ticks_in_window):
                    total_volume = sum(t.volume for t in ticks_in_window)
                    if total_volume > 0:
                        value = sum(t.price * t.volume for t in ticks_in_window) / total_volume
                        # VWAP 的 source_timestamp 取窗口中點
                        source_timestamp = target_time
                        method = ReferenceMethod.VWAP
                    else:
                        warnings.append(WarningCode.VWAP_VOLUME_MISSING)
                        # 降級為 nearest_tick
                        tick = min(ticks_in_window, key=lambda t: abs((t.timestamp - target_time).total_seconds()))
                        value = tick.price
                        source_timestamp = tick.timestamp
                        method = ReferenceMethod.WINDOW_NEAREST_TICK
                else:
                    warnings.append(WarningCode.VWAP_VOLUME_MISSING)
                    # 降級為 nearest_tick
                    tick = min(ticks_in_window, key=lambda t: abs((t.timestamp - target_time).total_seconds()))
                    value = tick.price
                    source_timestamp = tick.timestamp
                    method = ReferenceMethod.WINDOW_NEAREST_TICK
        
        # 策略 4: 插值（如果允許且窗口內無 tick）
        if value is None and allow_interpolation:
            if ticks_before and ticks_after:
                left_tick = ticks_before[-1]  # 最後一筆在窗口前的
                right_tick = ticks_after[0]   # 第一筆在窗口後的
                
                value, left_ts, right_ts = self._interpolate_price(target_time, left_tick, right_tick)
                if value is not None:
                    source_timestamp = target_time  # 插值點就是目標時間
                    method = ReferenceMethod.INTERPOLATED
                    warnings.append(WarningCode.INTERPOLATED_FROM_OUTSIDE_WINDOW)
            elif ticks_before:
                # 只有單邊（前）
                warnings.append(WarningCode.ONLY_SINGLE_SIDE_AVAILABLE)
            elif ticks_after:
                # 只有單邊（後）
                warnings.append(WarningCode.ONLY_SINGLE_SIDE_AVAILABLE)
            else:
                # 完全無 tick
                warnings.append(WarningCode.NO_TICKS_IN_WINDOW)
        elif value is None and not allow_interpolation:
            warnings.append(WarningCode.NO_TICKS_IN_WINDOW)
        
        # 確定狀態
        if method == ReferenceMethod.FAILED:
            status = ReferenceStatus.FAILED
        elif finalize_after_window_close and now >= window_end:
            status = ReferenceStatus.FINALIZED
        else:
            status = ReferenceStatus.PROVISIONAL
        
        # 計算品質分數
        actual_staleness = 0.0
        if source_timestamp:
            actual_staleness = abs((source_timestamp - target_time).total_seconds() * 1000)
        
        quality_score, quality_components = self._calculate_quality_score(
            method, num_ticks_in_window, actual_staleness, window_seconds
        )
        
        # 低品質警告
        if quality_score < 0.5:
            warnings.append(WarningCode.LOW_QUALITY_REFERENCE)
        
        # 陳舊警告
        if actual_staleness > 5000:
            warnings.append(WarningCode.STALE_REFERENCE)
        
        return ReferencePrice(
            value=value,
            source=source,
            symbol=symbol.upper(),
            method=method,
            status=status,
            target_timestamp=target_time,
            source_timestamp=source_timestamp,
            left_timestamp=left_ts,
            right_timestamp=right_ts,
            window_start=window_start,
            window_end=window_end,
            num_ticks_in_window=num_ticks_in_window,
            num_ticks_total=num_ticks_total,
            quality_score=quality_score,
            quality_components=quality_components,
            warnings=warnings,
            prefer_method=prefer_method,
            allow_interpolation=allow_interpolation,
        )
    
    def clear_old_ticks(self, before: datetime) -> int:
        """清理舊數據，回傳清理數量"""
        before = self._ensure_utc_datetime(before)
        total_cleared = 0
        
        for key in list(self._buffers.keys()):
            original_len = len(self._buffers[key])
            self._buffers[key] = [
                t for t in self._buffers[key] if t.timestamp >= before
            ]
            cleared = original_len - len(self._buffers[key])
            total_cleared += cleared
            
            if not self._buffers[key]:
                del self._buffers[key]
        
        return total_cleared
    
    def get_buffer_stats(self) -> Dict[Tuple[OracleFamily, str], int]:
        """獲取各 buffer 的 tick 數量統計"""
        return {k: len(v) for k, v in self._buffers.items()}


# ============================================================================
# ReferenceConsistencyValidator
# ============================================================================

class ReferenceConsistencyValidator:
    """跨來源一致性校驗器"""
    
    def __init__(self, max_deviation: float = 0.001):  # 0.1%
        self.max_deviation = max_deviation
    
    def validate(
        self,
        primary: ReferencePrice,
        fallback: Optional[ReferencePrice],
    ) -> Tuple[bool, List[WarningCode], Optional[float]]:
        """
        對比 primary 和 fallback。
        
        回傳: (is_consistent, warnings, deviation_ratio)
        
        deviation_ratio = abs(primary.value - fallback.value) / primary.value
        若 primary.value == 0 或任一無效，deviation_ratio = None
        """
        warnings: List[WarningCode] = []
        
        # 檢查有效性
        if primary.value is None:
            return False, warnings, None
        
        if fallback is None or fallback.value is None:
            # fallback 無效，但不一定是錯誤
            return True, warnings, None
        
        # 防除零
        if primary.value == 0:
            warnings.append(WarningCode.CROSS_SOURCE_MISMATCH)
            return False, warnings, None
        
        # 計算偏差
        deviation = abs(primary.value - fallback.value) / abs(primary.value)
        
        if deviation > self.max_deviation:
            warnings.append(WarningCode.CROSS_SOURCE_MISMATCH)
            return False, warnings, deviation
        
        return True, warnings, deviation
    
    def select_best(
        self,
        primary: ReferencePrice,
        fallback: Optional[ReferencePrice],
    ) -> ReferencePrice:
        """
        確定性選擇最佳 reference。
        
        規則（按優先級）：
        1. primary.status == "finalized" and primary.quality_score >= 0.8 → 用 primary
        2. primary.status == "failed" and fallback.status != "failed" → 用 fallback，標記 USED_FALLBACK_SOURCE
        3. primary.quality_score < 0.5 and fallback.quality_score >= 0.8 → 用 fallback，標記 USED_FALLBACK_SOURCE
        4. otherwise → 用 primary（即使品質較低）
        
        注意：不根據「哪個 price 更合理」選擇，只根據品質與可用性。
        """
        # 規則 1: primary finalized 且高品質
        if primary.status == ReferenceStatus.FINALIZED and primary.quality_score >= 0.8:
            return primary
        
        # 規則 2: primary failed 但 fallback 可用
        if primary.status == ReferenceStatus.FAILED and fallback is not None and fallback.status != ReferenceStatus.FAILED:
            # 複製 fallback 並添加警告
            new_warnings = list(fallback.warnings) + [WarningCode.USED_FALLBACK_SOURCE]
            return ReferencePrice(
                value=fallback.value,
                source=fallback.source,
                symbol=fallback.symbol,
                method=fallback.method,
                status=fallback.status,
                target_timestamp=fallback.target_timestamp,
                source_timestamp=fallback.source_timestamp,
                left_timestamp=fallback.left_timestamp,
                right_timestamp=fallback.right_timestamp,
                window_start=fallback.window_start,
                window_end=fallback.window_end,
                num_ticks_in_window=fallback.num_ticks_in_window,
                num_ticks_total=fallback.num_ticks_total,
                quality_score=fallback.quality_score,
                quality_components=fallback.quality_components,
                warnings=new_warnings,
                prefer_method=fallback.prefer_method,
                allow_interpolation=fallback.allow_interpolation,
            )
        
        # 規則 3: primary 低品質但 fallback 高品質
        if fallback is not None and primary.quality_score < 0.5 and fallback.quality_score >= 0.8:
            new_warnings = list(fallback.warnings) + [WarningCode.USED_FALLBACK_SOURCE]
            return ReferencePrice(
                value=fallback.value,
                source=fallback.source,
                symbol=fallback.symbol,
                method=fallback.method,
                status=fallback.status,
                target_timestamp=fallback.target_timestamp,
                source_timestamp=fallback.source_timestamp,
                left_timestamp=fallback.left_timestamp,
                right_timestamp=fallback.right_timestamp,
                window_start=fallback.window_start,
                window_end=fallback.window_end,
                num_ticks_in_window=fallback.num_ticks_in_window,
                num_ticks_total=fallback.num_ticks_total,
                quality_score=fallback.quality_score,
                quality_components=fallback.quality_components,
                warnings=new_warnings,
                prefer_method=fallback.prefer_method,
                allow_interpolation=fallback.allow_interpolation,
            )
        
        # 規則 4: 默認用 primary
        return primary
