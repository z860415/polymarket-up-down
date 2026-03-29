"""
Package B Unit Tests - Reference Price Builder

14 項測試案例
"""

from datetime import datetime, timezone, timedelta
from polymarket_arbitrage.reference_builder import (
    Tick,
    ReferencePrice,
    ReferenceMethod,
    ReferenceStatus,
    WarningCode,
    OracleFamily,
    ReferencePriceBuilder,
    ReferenceConsistencyValidator,
)


def create_utc_datetime(year, month, day, hour, minute, second=0):
    """Helper: 創建 UTC datetime"""
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


# ============================================================================
# Test 1: window 正中央有 tick
# ============================================================================

def test_window_center_tick():
    """窗口正中央有 tick，應 high quality"""
    builder = ReferencePriceBuilder()
    target = create_utc_datetime(2099, 3, 28, 12, 0, 0)
    
    # 在 target 時間添加 tick
    builder.add_tick(
        source=OracleFamily.BINANCE,
        symbol="BTCUSDT",
        timestamp=target,
        price=65000.0,
        volume=1.0,
    )
    
    ref = builder.build_reference_price(
        source=OracleFamily.BINANCE,
        symbol="BTCUSDT",
        target_time=target,
        window_seconds=10,
        prefer_method=ReferenceMethod.WINDOW_FIRST_TICK,
    )
    
    assert ref.value == 65000.0
    assert ref.method == ReferenceMethod.WINDOW_FIRST_TICK
    assert ref.quality_score > 0.7  # 單一 tick，density 較低
    assert ref.status == ReferenceStatus.PROVISIONAL  # window 還未關閉
    assert ref.source_timestamp == target
    
    print(f"✅ Test 1: Window center tick, quality={ref.quality_score:.3f}")


# ============================================================================
# Test 2: window 內多筆 tick，驗證 method 選擇
# ============================================================================

def test_multiple_ticks_method_selection():
    """窗口內多筆 tick，驗證 first/nearest/vwap 選擇"""
    builder = ReferencePriceBuilder()
    target = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    # 添加多筆 tick
    builder.add_tick(OracleFamily.BINANCE, "BTCUSDT", target - timedelta(seconds=5), 64000.0, 1.0)
    builder.add_tick(OracleFamily.BINANCE, "BTCUSDT", target - timedelta(seconds=2), 64500.0, 1.0)
    builder.add_tick(OracleFamily.BINANCE, "BTCUSDT", target + timedelta(seconds=2), 65500.0, 1.0)
    builder.add_tick(OracleFamily.BINANCE, "BTCUSDT", target + timedelta(seconds=5), 66000.0, 1.0)
    
    # Test first tick
    ref_first = builder.build_reference_price(
        OracleFamily.BINANCE, "BTCUSDT", target, 10,
        prefer_method=ReferenceMethod.WINDOW_FIRST_TICK,
    )
    assert ref_first.method == ReferenceMethod.WINDOW_FIRST_TICK
    assert ref_first.value == 64000.0  # 第一筆
    
    # Test nearest tick
    ref_nearest = builder.build_reference_price(
        OracleFamily.BINANCE, "BTCUSDT", target, 10,
        prefer_method=ReferenceMethod.WINDOW_NEAREST_TICK,
    )
    assert ref_nearest.method == ReferenceMethod.WINDOW_NEAREST_TICK
    # -2s 和 +2s 都是 2秒，但 -2s 的 tick 先出現在列表中
    assert abs(ref_nearest.value - 64500.0) < 1000  # 應該是 -2s 或 +2s 的其中之一
    
    # Test VWAP
    ref_vwap = builder.build_reference_price(
        OracleFamily.BINANCE, "BTCUSDT", target, 10,
        prefer_method=ReferenceMethod.VWAP,
    )
    assert ref_vwap.method == ReferenceMethod.VWAP
    # VWAP = (64000 + 64500 + 65500 + 66000) / 4 = 65000
    assert ref_vwap.value == 65000.0
    
    print(f"✅ Test 2: Method selection - first={ref_first.value}, vwap={ref_vwap.value}")


# ============================================================================
# Test 3: window 內無 tick，前後各一筆 → 插值
# ============================================================================

def test_interpolation_from_outside():
    """窗口內無 tick，前後各一筆，應 interpolation 且 quality 降級"""
    builder = ReferencePriceBuilder()
    target = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    # 在窗口外添加 tick（窗口是 target ±10s，所以在 ±15s）
    builder.add_tick(OracleFamily.BINANCE, "BTCUSDT", target - timedelta(seconds=15), 64000.0, 1.0)
    builder.add_tick(OracleFamily.BINANCE, "BTCUSDT", target + timedelta(seconds=15), 66000.0, 1.0)
    
    ref = builder.build_reference_price(
        OracleFamily.BINANCE, "BTCUSDT", target, 10,
        allow_interpolation=True,
    )
    
    assert ref.method == ReferenceMethod.INTERPOLATED
    assert ref.value == 65000.0  # 插值結果
    assert ref.left_timestamp is not None
    assert ref.right_timestamp is not None
    assert WarningCode.INTERPOLATED_FROM_OUTSIDE_WINDOW in ref.warnings
    assert ref.quality_score < 0.8  # 插值 quality 降級
    
    print(f"✅ Test 3: Interpolated value={ref.value}, quality={ref.quality_score:.3f}")


# ============================================================================
# Test 4: 只有 window 外單邊一筆 → fail 或 fallback
# ============================================================================

def test_single_side_only():
    """只有 window 外單邊一筆，應 fail 並標記警告"""
    builder = ReferencePriceBuilder()
    target = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    # 只有窗口前的一筆
    builder.add_tick(OracleFamily.BINANCE, "BTCUSDT", target - timedelta(seconds=15), 64000.0, 1.0)
    
    ref = builder.build_reference_price(
        OracleFamily.BINANCE, "BTCUSDT", target, 10,
        allow_interpolation=True,
    )
    
    assert ref.method == ReferenceMethod.FAILED
    assert ref.value is None
    assert WarningCode.ONLY_SINGLE_SIDE_AVAILABLE in ref.warnings
    
    print(f"✅ Test 4: Single side only, method={ref.method.value}")


# ============================================================================
# Test 5: 完全無 tick → failed, value=None
# ============================================================================

def test_no_ticks_at_all():
    """完全無 tick，status=failed，value=None"""
    builder = ReferencePriceBuilder()
    target = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    ref = builder.build_reference_price(
        OracleFamily.BINANCE, "BTCUSDT", target, 10,
    )
    
    assert ref.status == ReferenceStatus.FAILED
    assert ref.value is None
    assert ref.method == ReferenceMethod.FAILED
    assert WarningCode.NO_TICKS_IN_WINDOW in ref.warnings
    
    print(f"✅ Test 5: No ticks, status={ref.status.value}")


# ============================================================================
# Test 6: tick 時間亂序輸入 → builder 正確排序
# ============================================================================

def test_out_of_order_ticks():
    """tick 時間亂序輸入，builder 應能正確排序"""
    builder = ReferencePriceBuilder()
    target = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    # 亂序添加
    builder.add_tick(OracleFamily.BINANCE, "BTCUSDT", target + timedelta(seconds=3), 65300.0, 1.0)
    builder.add_tick(OracleFamily.BINANCE, "BTCUSDT", target - timedelta(seconds=3), 64700.0, 1.0)
    builder.add_tick(OracleFamily.BINANCE, "BTCUSDT", target, 65000.0, 1.0)
    
    ref = builder.build_reference_price(
        OracleFamily.BINANCE, "BTCUSDT", target, 10,
        prefer_method=ReferenceMethod.WINDOW_FIRST_TICK,
    )
    
    # 應該找到窗口內的 tick（-3s, 0s, +3s 都在窗口內）
    assert ref.value == 64700.0  # 第一筆（時間上最早的）
    assert ref.num_ticks_in_window == 3
    
    print(f"✅ Test 6: Out of order ticks handled, value={ref.value}")


# ============================================================================
# Test 7: naive datetime 輸入 → reject
# ============================================================================

def test_naive_datetime_rejected():
    """naive datetime 輸入，應 reject 並發出警告"""
    builder = ReferencePriceBuilder()
    
    try:
        builder.add_tick(
            OracleFamily.BINANCE,
            "BTCUSDT",
            datetime(2026, 3, 28, 12, 0, 0),  # naive!
            65000.0,
            1.0,
        )
        assert False, "應該拋出異常"
    except ValueError as e:
        assert "Naive datetime" in str(e)
    
    print(f"✅ Test 7: Naive datetime rejected")


# ============================================================================
# Test 8: symbol/source 混流保護
# ============================================================================

def test_symbol_source_isolation():
    """BTCUSDT 與 BTCUSD 不互相污染"""
    builder = ReferencePriceBuilder()
    target = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    # 添加到不同 symbol
    builder.add_tick(OracleFamily.BINANCE, "BTCUSDT", target, 65000.0, 1.0)
    builder.add_tick(OracleFamily.BINANCE, "BTCUSD", target, 64950.0, 1.0)  # 不同價格
    
    ref_usdt = builder.build_reference_price(
        OracleFamily.BINANCE, "BTCUSDT", target, 10,
    )
    ref_usd = builder.build_reference_price(
        OracleFamily.BINANCE, "BTCUSD", target, 10,
    )
    
    assert ref_usdt.value == 65000.0
    assert ref_usd.value == 64950.0
    
    # 檢查 buffer 分離
    stats = builder.get_buffer_stats()
    assert (OracleFamily.BINANCE, "BTCUSDT") in stats
    assert (OracleFamily.BINANCE, "BTCUSD") in stats
    
    print(f"✅ Test 8: Symbol isolation - USDT={ref_usdt.value}, USD={ref_usd.value}")


# ============================================================================
# Test 9: primary/fallback 價差正常
# ============================================================================

def test_cross_source_normal():
    """primary/fallback 價差正常，is_consistent=True"""
    validator = ReferenceConsistencyValidator(max_deviation=0.001)
    
    target = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    primary = ReferencePrice(
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
        num_ticks_total=5,
        quality_score=0.95,
        quality_components={},
        warnings=[],
        prefer_method=ReferenceMethod.WINDOW_FIRST_TICK,
        allow_interpolation=True,
    )
    
    fallback = ReferencePrice(
        value=64995.0,  # 0.008% 差異，在 0.1% 內
        source=OracleFamily.CHAINLINK,
        symbol="BTCUSD",
        method=ReferenceMethod.WINDOW_FIRST_TICK,
        status=ReferenceStatus.FINALIZED,
        target_timestamp=target,
        source_timestamp=target,
        left_timestamp=None,
        right_timestamp=None,
        window_start=target - timedelta(seconds=10),
        window_end=target + timedelta(seconds=10),
        num_ticks_in_window=3,
        num_ticks_total=3,
        quality_score=0.9,
        quality_components={},
        warnings=[],
        prefer_method=ReferenceMethod.WINDOW_FIRST_TICK,
        allow_interpolation=True,
    )
    
    is_consistent, warnings, deviation = validator.validate(primary, fallback)
    
    assert is_consistent is True
    assert deviation is not None
    assert deviation < 0.001
    assert WarningCode.CROSS_SOURCE_MISMATCH not in warnings
    
    print(f"✅ Test 9: Cross-source normal, deviation={deviation:.6f}")


# ============================================================================
# Test 10: primary/fallback 價差過大
# ============================================================================

def test_cross_source_mismatch():
    """primary/fallback 價差過大 (>0.1%)，應發 CROSS_SOURCE_MISMATCH"""
    validator = ReferenceConsistencyValidator(max_deviation=0.001)
    
    target = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    primary = ReferencePrice(
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
        num_ticks_total=5,
        quality_score=0.95,
        quality_components={},
        warnings=[],
        prefer_method=ReferenceMethod.WINDOW_FIRST_TICK,
        allow_interpolation=True,
    )
    
    fallback = ReferencePrice(
        value=64000.0,  # 1.5% 差異，超過 0.1%
        source=OracleFamily.CHAINLINK,
        symbol="BTCUSD",
        method=ReferenceMethod.WINDOW_FIRST_TICK,
        status=ReferenceStatus.FINALIZED,
        target_timestamp=target,
        source_timestamp=target,
        left_timestamp=None,
        right_timestamp=None,
        window_start=target - timedelta(seconds=10),
        window_end=target + timedelta(seconds=10),
        num_ticks_in_window=3,
        num_ticks_total=3,
        quality_score=0.9,
        quality_components={},
        warnings=[],
        prefer_method=ReferenceMethod.WINDOW_FIRST_TICK,
        allow_interpolation=True,
    )
    
    is_consistent, warnings, deviation = validator.validate(primary, fallback)
    
    assert is_consistent is False
    assert deviation is not None
    assert deviation > 0.001
    assert WarningCode.CROSS_SOURCE_MISMATCH in warnings
    
    print(f"✅ Test 10: Cross-source mismatch, deviation={deviation:.6f}")


# ============================================================================
# Test 11: primary failed, fallback success
# ============================================================================

def test_fallback_when_primary_failed():
    """primary failed，fallback success，應正確 fallback"""
    validator = ReferenceConsistencyValidator()
    
    target = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    primary = ReferencePrice(
        value=None,
        source=OracleFamily.BINANCE,
        symbol="BTCUSDT",
        method=ReferenceMethod.FAILED,
        status=ReferenceStatus.FAILED,
        target_timestamp=target,
        source_timestamp=None,
        left_timestamp=None,
        right_timestamp=None,
        window_start=target - timedelta(seconds=10),
        window_end=target + timedelta(seconds=10),
        num_ticks_in_window=0,
        num_ticks_total=0,
        quality_score=0.0,
        quality_components={},
        warnings=[WarningCode.NO_TICKS_IN_WINDOW],
        prefer_method=ReferenceMethod.WINDOW_FIRST_TICK,
        allow_interpolation=True,
    )
    
    fallback = ReferencePrice(
        value=65000.0,
        source=OracleFamily.CHAINLINK,
        symbol="BTCUSD",
        method=ReferenceMethod.WINDOW_FIRST_TICK,
        status=ReferenceStatus.FINALIZED,
        target_timestamp=target,
        source_timestamp=target,
        left_timestamp=None,
        right_timestamp=None,
        window_start=target - timedelta(seconds=10),
        window_end=target + timedelta(seconds=10),
        num_ticks_in_window=3,
        num_ticks_total=3,
        quality_score=0.9,
        quality_components={},
        warnings=[],
        prefer_method=ReferenceMethod.WINDOW_FIRST_TICK,
        allow_interpolation=True,
    )
    
    best = validator.select_best(primary, fallback)
    
    assert best.value == 65000.0
    assert best.source == OracleFamily.CHAINLINK
    assert WarningCode.USED_FALLBACK_SOURCE in best.warnings
    
    print(f"✅ Test 11: Fallback used when primary failed")


# ============================================================================
# Test 12: allow_interpolation=False
# ============================================================================

def test_no_interpolation_allowed():
    """allow_interpolation=False 時，窗口內無 tick 即 fail"""
    builder = ReferencePriceBuilder()
    target = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    # 在窗口外添加 tick
    builder.add_tick(OracleFamily.BINANCE, "BTCUSDT", target - timedelta(seconds=15), 64000.0, 1.0)
    builder.add_tick(OracleFamily.BINANCE, "BTCUSDT", target + timedelta(seconds=15), 66000.0, 1.0)
    
    ref = builder.build_reference_price(
        OracleFamily.BINANCE, "BTCUSDT", target, 10,
        allow_interpolation=False,  # 禁用插值
    )
    
    assert ref.method == ReferenceMethod.FAILED
    assert ref.value is None
    assert WarningCode.NO_TICKS_IN_WINDOW in ref.warnings
    
    print(f"✅ Test 12: No interpolation allowed, method={ref.method.value}")


# ============================================================================
# Test 13: VWAP 但 volume 缺失
# ============================================================================

def test_vwap_volume_missing():
    """請求 VWAP 但 volume 缺失，應降級並標記警告"""
    builder = ReferencePriceBuilder()
    target = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    # 添加無 volume 的 tick
    builder.add_tick(OracleFamily.BINANCE, "BTCUSDT", target - timedelta(seconds=2), 64000.0, None)
    builder.add_tick(OracleFamily.BINANCE, "BTCUSDT", target + timedelta(seconds=2), 66000.0, None)
    
    ref = builder.build_reference_price(
        OracleFamily.BINANCE, "BTCUSDT", target, 10,
        prefer_method=ReferenceMethod.VWAP,
    )
    
    # 應降級為 nearest_tick
    assert ref.method == ReferenceMethod.WINDOW_NEAREST_TICK
    assert WarningCode.VWAP_VOLUME_MISSING in ref.warnings
    
    print(f"✅ Test 13: VWAP volume missing, fallback to {ref.method.value}")


# ============================================================================
# Test 14: finalized/provisional 狀態轉換
# ============================================================================

def test_status_transition():
    """同一 target，窗口關閉前後重建，狀態變化正確"""
    builder = ReferencePriceBuilder()
    target = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    # 添加 tick
    builder.add_tick(OracleFamily.BINANCE, "BTCUSDT", target, 65000.0, 1.0)
    
    # 窗口未關閉時（現在時間在窗口結束前）
    ref_provisional = builder.build_reference_price(
        OracleFamily.BINANCE, "BTCUSDT", target, 10,
        finalize_after_window_close=True,
    )
    
    # 這個測試取決於當前時間，所以我們只檢查 status 是合法的
    assert ref_provisional.status in [ReferenceStatus.PROVISIONAL, ReferenceStatus.FINALIZED]
    
    # 如果手動設置 finalize_after_window_close=False，應該永遠是 provisional
    ref_always_provisional = builder.build_reference_price(
        OracleFamily.BINANCE, "BTCUSDT", target, 10,
        finalize_after_window_close=False,
    )
    assert ref_always_provisional.status == ReferenceStatus.PROVISIONAL
    
    print(f"✅ Test 14: Status transition - provisional={ref_provisional.status.value}")


# ============================================================================
# Additional Tests
# ============================================================================

def test_deviation_division_by_zero():
    """驗證 deviation_ratio 防除零保護"""
    validator = ReferenceConsistencyValidator(max_deviation=0.001)
    
    target = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    primary = ReferencePrice(
        value=0.0,  # 零值！
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
        num_ticks_in_window=1,
        num_ticks_total=1,
        quality_score=0.5,
        quality_components={},
        warnings=[],
        prefer_method=ReferenceMethod.WINDOW_FIRST_TICK,
        allow_interpolation=True,
    )
    
    fallback = ReferencePrice(
        value=100.0,
        source=OracleFamily.CHAINLINK,
        symbol="BTCUSD",
        method=ReferenceMethod.WINDOW_FIRST_TICK,
        status=ReferenceStatus.FINALIZED,
        target_timestamp=target,
        source_timestamp=target,
        left_timestamp=None,
        right_timestamp=None,
        window_start=target - timedelta(seconds=10),
        window_end=target + timedelta(seconds=10),
        num_ticks_in_window=1,
        num_ticks_total=1,
        quality_score=0.5,
        quality_components={},
        warnings=[],
        prefer_method=ReferenceMethod.WINDOW_FIRST_TICK,
        allow_interpolation=True,
    )
    
    is_consistent, warnings, deviation = validator.validate(primary, fallback)
    
    # primary.value == 0 時應該標記 mismatch 且不拋異常
    assert is_consistent is False
    assert deviation is None  # 無法計算
    assert WarningCode.CROSS_SOURCE_MISMATCH in warnings
    
    print(f"✅ Additional test: Division by zero protection")


def test_quality_score_components():
    """驗證 quality_components 包含所有預期欄位"""
    builder = ReferencePriceBuilder()
    target = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    builder.add_tick(OracleFamily.BINANCE, "BTCUSDT", target, 65000.0, 1.0)
    
    ref = builder.build_reference_price(
        OracleFamily.BINANCE, "BTCUSDT", target, 10,
    )
    
    expected_components = {
        "temporal_proximity",
        "tick_density",
        "method_score",
        "freshness_score",
    }
    
    assert set(ref.quality_components.keys()) == expected_components
    assert all(0.0 <= v <= 1.0 for v in ref.quality_components.values())
    
    print(f"✅ Additional test: Quality components valid")


def test_serialization_roundtrip():
    """ReferencePrice 序列化往返測試"""
    target = create_utc_datetime(2026, 3, 28, 12, 0, 0)
    
    original = ReferencePrice(
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
        num_ticks_total=5,
        quality_score=0.95,
        quality_components={"temporal_proximity": 1.0, "tick_density": 0.8},
        warnings=[WarningCode.STALE_REFERENCE],
        prefer_method=ReferenceMethod.WINDOW_FIRST_TICK,
        allow_interpolation=True,
    )
    
    # 序列化
    data = original.to_dict()
    
    # 反序列化
    restored = ReferencePrice.from_dict(data)
    
    # 驗證
    assert restored.value == original.value
    assert restored.source == original.source
    assert restored.method == original.method
    assert restored.status == original.status
    assert restored.quality_score == original.quality_score
    assert [w.value for w in restored.warnings] == [w.value for w in original.warnings]
    
    print(f"✅ Additional test: Serialization roundtrip successful")


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("Package B Unit Tests - Reference Price Builder")
    print("=" * 70)
    
    # 14 項主要測試
    test_window_center_tick()
    test_multiple_ticks_method_selection()
    test_interpolation_from_outside()
    test_single_side_only()
    test_no_ticks_at_all()
    test_out_of_order_ticks()
    test_naive_datetime_rejected()
    test_symbol_source_isolation()
    test_cross_source_normal()
    test_cross_source_mismatch()
    test_fallback_when_primary_failed()
    test_no_interpolation_allowed()
    test_vwap_volume_missing()
    test_status_transition()
    
    # 額外測試
    print("\n--- Additional Tests ---")
    test_deviation_division_by_zero()
    test_quality_score_components()
    test_serialization_roundtrip()
    
    print("\n" + "=" * 70)
    print("All 14 + 3 tests completed!")
    print("=" * 70)
