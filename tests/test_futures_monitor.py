"""Tests for core.futures_monitor — FuturesMonitor (Issue #14, 指标 #4、5、6)."""
from __future__ import annotations

from pathlib import Path

import pytest

from core.futures_monitor import FuturesMonitor


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture
def cfg_file(tmp_path: Path) -> Path:
    """Minimal risk_params.yaml with known thresholds for predictable tests."""
    cfg = tmp_path / "risk_params.yaml"
    cfg.write_text(
        "futures_monitor:\n"
        "  max_orders_per_day: 10\n"
        "  max_cancels_per_day: 5\n"
        "  max_duplicate_orders: 3\n"
        "  max_lots_per_order: 100\n"
        "  warning_pct: 0.8\n"
    )
    return cfg


@pytest.fixture
def monitor(cfg_file: Path) -> FuturesMonitor:
    return FuturesMonitor(config_path=cfg_file)


# ── Counting ──────────────────────────────────────────────────────────


def test_record_order_increments_count(monitor: FuturesMonitor) -> None:
    monitor.record_order("o1")
    assert monitor.order_count == 1


def test_record_cancel_increments_count(monitor: FuturesMonitor) -> None:
    monitor.record_cancel("o1")
    assert monitor.cancel_count == 1


def test_record_fill_increments_count(monitor: FuturesMonitor) -> None:
    monitor.record_fill("o1")
    assert monitor.fill_count == 1


def test_duplicate_order_detected(monitor: FuturesMonitor) -> None:
    """Second record_order with same order_id → duplicate_count +1."""
    monitor.record_order("o1")
    monitor.record_order("o1")
    assert monitor.duplicate_count == 1
    assert monitor.order_count == 2


def test_reset_clears_all_counts(monitor: FuturesMonitor) -> None:
    monitor.record_order("o1")
    monitor.record_order("o1")  # duplicate
    monitor.record_cancel("o1")
    monitor.record_fill("o1")
    monitor.reset()
    assert monitor.order_count == 0
    assert monitor.cancel_count == 0
    assert monitor.fill_count == 0
    assert monitor.duplicate_count == 0


# ── Thresholds ────────────────────────────────────────────────────────


def test_warning_callback_fires_at_80_pct(cfg_file: Path) -> None:
    """order_count reaching 80% of max_orders_per_day triggers on_warning_callback."""
    warning_calls: list[tuple[str, int, int]] = []
    mon = FuturesMonitor(
        on_warning_callback=lambda f, c, l: warning_calls.append((f, c, l)),
        config_path=cfg_file,
    )
    # max_orders=10, int(10 * 0.8) = 8 → warning fires at count == 8
    for i in range(8):
        mon.record_order(f"o{i}")
    assert any(f == "order_count" for f, c, l in warning_calls), (
        f"Expected warning on order_count, got: {warning_calls}"
    )


def test_breach_callback_fires_at_100_pct(cfg_file: Path) -> None:
    """order_count reaching max_orders_per_day triggers on_breach_callback."""
    breach_calls: list[tuple[str, int, int]] = []
    mon = FuturesMonitor(
        on_breach_callback=lambda f, c, l: breach_calls.append((f, c, l)),
        config_path=cfg_file,
    )
    # max_orders=10
    for i in range(10):
        mon.record_order(f"o{i}")
    assert any(f == "order_count" for f, c, l in breach_calls), (
        f"Expected breach on order_count, got: {breach_calls}"
    )


def test_no_callback_below_threshold(cfg_file: Path) -> None:
    """Counts below 80% threshold must not trigger any callback."""
    warning_calls: list = []
    breach_calls: list = []
    mon = FuturesMonitor(
        on_warning_callback=lambda f, c, l: warning_calls.append((f, c, l)),
        on_breach_callback=lambda f, c, l: breach_calls.append((f, c, l)),
        config_path=cfg_file,
    )
    # max_orders=10, 7 < int(10*0.8)=8 → no callbacks
    for i in range(7):
        mon.record_order(f"o{i}")
    assert not warning_calls, f"Unexpected warnings: {warning_calls}"
    assert not breach_calls, f"Unexpected breaches: {breach_calls}"
