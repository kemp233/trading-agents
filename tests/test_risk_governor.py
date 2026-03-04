"""Tests for agents.risk_governor — RiskGovernor new API (Issue #14, 指标 #3、9、10)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from agents.risk_governor import RiskGovernor


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture
def gov() -> RiskGovernor:
    """Minimal RiskGovernor with no real event_bus or state_writer."""
    return RiskGovernor(config={}, event_bus=None, state_writer=None)


# ── State transitions ─────────────────────────────────────────────────


def test_initial_state_is_normal(gov: RiskGovernor) -> None:
    assert gov.state == "NORMAL"


def test_halt_sets_halted_state(gov: RiskGovernor) -> None:
    gov.halt("emergency stop")
    assert gov.state == "VENUE_HALT"
    assert gov.halt_reason == "emergency stop"
    assert gov.halted_at is not None


def test_resume_from_halt_returns_normal(gov: RiskGovernor) -> None:
    gov.halt("test halt")
    gov.resume()
    assert gov.state == "NORMAL"
    assert gov.halt_reason is None
    assert gov.halted_at is None


def test_set_degraded_sets_degraded_state(gov: RiskGovernor) -> None:
    gov.set_degraded("network issue")
    assert gov.state == "DEGRADED"


def test_recover_degraded_returns_normal(gov: RiskGovernor) -> None:
    gov.set_degraded("test")
    gov.recover_degraded()
    assert gov.state == "NORMAL"


# ── Priority rules ────────────────────────────────────────────────────


def test_halt_overrides_degraded(gov: RiskGovernor) -> None:
    """VENUE_HALT takes priority over DEGRADED."""
    gov.set_degraded("downgrade first")
    gov.halt("emergency")
    assert gov.state == "VENUE_HALT"


def test_degraded_cannot_override_halt(gov: RiskGovernor) -> None:
    """set_degraded() is silently ignored when current state is VENUE_HALT."""
    gov.halt("already halted")
    gov.set_degraded("should be ignored")
    assert gov.state == "VENUE_HALT"


# ── Order gate ────────────────────────────────────────────────────────


def test_check_order_allowed_normal(gov: RiskGovernor) -> None:
    assert gov.check_order_allowed() is True


def test_check_order_allowed_halted(gov: RiskGovernor) -> None:
    gov.halt("stop trading")
    assert gov.check_order_allowed() is False


def test_check_order_allowed_degraded(gov: RiskGovernor) -> None:
    """DEGRADED state still allows new orders."""
    gov.set_degraded("degraded but alive")
    assert gov.check_order_allowed() is True


# ── Batch cancel ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_cancel_all_orders_calls_adapter() -> None:
    """cancel_all_orders() queries state_writer and calls adapter.cancel_order per order."""
    mock_order = MagicMock()
    mock_order.client_order_id = "order-001"

    mock_writer = MagicMock()
    mock_writer.query_orders_by_status = AsyncMock(
        side_effect=[
            [mock_order],  # PENDING_SEND
            [],            # SENT
        ]
    )

    gov = RiskGovernor(config={}, event_bus=None, state_writer=mock_writer)
    gov.halt("batch cancel test")

    mock_adapter = MagicMock()
    mock_adapter.cancel_order = AsyncMock(return_value=None)

    await gov.cancel_all_orders(mock_adapter)

    mock_adapter.cancel_order.assert_called_once_with("order-001")
