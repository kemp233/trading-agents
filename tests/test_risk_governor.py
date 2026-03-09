from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_UP
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from agents.risk_governor import RecoveryPolicy, RiskGovernor
from core.state_writer import StateWriter
from core.venue_order_spec import VenuePosition


def _base_config() -> dict:
    return {
        "flash_crash": {
            "atr_multiplier": 3,
            "abs_return_1s_threshold": 0.03,
            "abs_return_5s_threshold": 0.05,
        },
        "reconciliation": {
            "drift_threshold_pct": 0.01,
            "max_drift_before_halt": 0.05,
        },
        "circuit_breaker": {
            "attempt_flatten_timeout_sec": 0.1,
            "staged_exit_batch_pct": 0.5,
            "staged_exit_interval_sec": 0,
        },
        "recovery": {
            "exponential_backoff_base_sec": 5,
            "max_backoff_sec": 300,
            "cooldown_after_recovery_sec": 60,
            "post_recovery_scale": 0.5,
            "post_recovery_duration_sec": 30,
        },
    }


def _init_sqlite_schema(db_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    schema_path = root / "db" / "schema.sql"
    schema_sql = schema_path.read_text(encoding="utf-8")
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(schema_sql)
        conn.commit()
    finally:
        conn.close()


class FakeStateWriter:
    def __init__(self) -> None:
        self.risk_states = []

    async def write_risk_state(self, risk_state) -> None:
        self.risk_states.append(risk_state)


class FakeAdapter:
    def __init__(
        self,
        positions: list[VenuePosition],
        *,
        flatten_mode: str = "success",
        stage_round_up: bool = True,
    ) -> None:
        self.positions = list(positions)
        self.flatten_mode = flatten_mode
        self.stage_round_up = stage_round_up
        self.cancelled_orders: list[str] = []
        self.submitted_orders = []

    async def cancel_order(self, client_order_id: str):
        self.cancelled_orders.append(client_order_id)
        return SimpleNamespace(client_order_id=client_order_id)

    async def query_positions(self) -> list[VenuePosition]:
        return [
            position
            for position in self.positions
            if Decimal(str(position.quantity)) > 0
        ]

    async def submit_order(self, spec):
        self.submitted_orders.append(spec)
        matching = next(
            position
            for position in self.positions
            if position.symbol == spec.symbol and position.venue == spec.venue
        )

        if "attempt" in spec.client_order_id:
            if self.flatten_mode == "success":
                matching.quantity = Decimal("0")
            elif self.flatten_mode == "timeout":
                await asyncio.sleep(0.2)
            return SimpleNamespace(client_order_id=spec.client_order_id)

        executed = Decimal(str(spec.quantity))
        if self.stage_round_up and executed < 1:
            executed = executed.quantize(Decimal("1"), rounding=ROUND_UP)
        matching.quantity = max(Decimal("0"), Decimal(str(matching.quantity)) - executed)
        return SimpleNamespace(client_order_id=spec.client_order_id)


@pytest.fixture
def gov() -> RiskGovernor:
    return RiskGovernor(config=_base_config(), event_bus=None, state_writer=None)


def _make_position(quantity: str = "2", side: str = "LONG") -> VenuePosition:
    return VenuePosition(
        symbol="rb2510",
        venue="ctp",
        side=side,
        quantity=Decimal(quantity),
        entry_price=Decimal("3500"),
        unrealized_pnl=Decimal("0"),
        updated_at=datetime.now(timezone.utc),
    )


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


def test_degraded_cannot_override_halt(gov: RiskGovernor) -> None:
    gov.halt("already halted")
    gov.set_degraded("should be ignored")
    assert gov.state == "VENUE_HALT"


def test_check_order_allowed_tracks_halt_only(gov: RiskGovernor) -> None:
    assert gov.check_order_allowed() is True
    gov.set_degraded("warn only")
    assert gov.check_order_allowed() is True
    gov.halt("hard stop")
    assert gov.check_order_allowed() is False


@pytest.mark.asyncio
async def test_cancel_all_orders_calls_adapter() -> None:
    mock_order = MagicMock()
    mock_order.client_order_id = "order-001"

    mock_writer = MagicMock()
    mock_writer.query_orders_by_status = AsyncMock(side_effect=[[mock_order], []])
    mock_writer.write_risk_state = AsyncMock(return_value=None)

    gov = RiskGovernor(config=_base_config(), event_bus=None, state_writer=mock_writer)
    gov.halt("batch cancel test")

    mock_adapter = MagicMock()
    mock_adapter.cancel_order = AsyncMock(return_value=None)

    await gov.cancel_all_orders(mock_adapter)

    mock_adapter.cancel_order.assert_called_once_with("order-001")


def test_market_risk_atr_triggers_circuit(gov: RiskGovernor) -> None:
    state = gov.evaluate_market_risk(
        last_price=110,
        reference_price=100,
        atr=3,
        return_1s=0.01,
        return_5s=0.01,
    )
    assert state == "CIRCUIT"


def test_market_risk_return_1s_triggers_circuit(gov: RiskGovernor) -> None:
    state = gov.evaluate_market_risk(
        last_price=100,
        reference_price=100,
        atr=10,
        return_1s=0.031,
        return_5s=0.01,
    )
    assert state == "CIRCUIT"


def test_market_risk_return_5s_triggers_circuit(gov: RiskGovernor) -> None:
    state = gov.evaluate_market_risk(
        last_price=100,
        reference_price=100,
        atr=10,
        return_1s=0.01,
        return_5s=0.051,
    )
    assert state == "CIRCUIT"


def test_reconciliation_risk_sets_degraded(gov: RiskGovernor) -> None:
    state = gov.evaluate_reconciliation_risk(drift_pct=0.02)
    assert state == "DEGRADED"


def test_reconciliation_risk_sets_reconciling(gov: RiskGovernor) -> None:
    state = gov.evaluate_reconciliation_risk(drift_pct=0.06)
    assert state == "RECONCILING"


def test_connection_risk_sets_offline(gov: RiskGovernor) -> None:
    state = gov.evaluate_connection_risk(
        gateway_connected=False,
        health_ok=True,
        consecutive_failures=3,
    )
    assert state == "OFFLINE"


@pytest.mark.asyncio
async def test_attempt_flatten_success_transitions_to_reconciling(gov: RiskGovernor) -> None:
    adapter = FakeAdapter([_make_position("2")], flatten_mode="success")
    ok = await gov.attempt_flatten(adapter)
    assert ok is True
    assert gov.state == "RECONCILING"
    assert len(adapter.submitted_orders) == 1
    assert adapter.submitted_orders[0].reduce_only is True


@pytest.mark.asyncio
async def test_attempt_flatten_failure_then_staged_exit_path(gov: RiskGovernor) -> None:
    gov.evaluate_market_risk(
        last_price=110,
        reference_price=100,
        atr=3,
        return_1s=0.01,
        return_5s=0.01,
    )
    adapter = FakeAdapter([_make_position("3")], flatten_mode="failure")
    ok = await gov.attempt_flatten(adapter)
    assert ok is False
    assert gov.state == "CIRCUIT"

    staged_ok = await gov.staged_exit(adapter, batch_pct=0.5, interval_sec=0)
    assert staged_ok is True
    assert gov.state == "RECONCILING"
    assert len(adapter.submitted_orders) >= 3


@pytest.mark.asyncio
async def test_attempt_flatten_timeout_returns_false(gov: RiskGovernor) -> None:
    adapter = FakeAdapter([_make_position("2")], flatten_mode="timeout")
    ok = await gov.attempt_flatten(adapter, timeout_sec=0.05)
    assert ok is False
    assert gov.state == "NORMAL"


@pytest.mark.asyncio
async def test_staged_exit_runs_in_multiple_batches(gov: RiskGovernor) -> None:
    adapter = FakeAdapter([_make_position("3")], flatten_mode="failure")
    ok = await gov.staged_exit(adapter, batch_pct=0.5, interval_sec=0)
    assert ok is True
    assert gov.state == "RECONCILING"
    assert len(adapter.submitted_orders) > 1


def test_recovery_cooldown_blocks_transition_from_circuit(gov: RiskGovernor) -> None:
    gov.evaluate_market_risk(
        last_price=110,
        reference_price=100,
        atr=3,
        return_1s=0.01,
        return_5s=0.01,
    )
    assert gov.state == "CIRCUIT"

    changed = gov.transition("DEGRADED", "cooldown_not_elapsed")
    assert changed is False
    assert gov.state == "CIRCUIT"


def test_recovery_path_returns_to_degraded_then_normal(gov: RiskGovernor) -> None:
    gov.evaluate_market_risk(
        last_price=110,
        reference_price=100,
        atr=3,
        return_1s=0.01,
        return_5s=0.01,
    )
    gov._recovery_policy.last_failure_time = _base_now_minus(seconds=61)

    changed = gov.transition("DEGRADED", "market_stabilized")
    assert changed is True
    assert gov.state == "DEGRADED"

    changed = gov.transition("NORMAL", "post_recovery_window_active")
    assert changed is False
    assert gov.state == "DEGRADED"

    gov._recovery_policy._degraded_until = _base_now_minus(seconds=1)
    changed = gov.transition("NORMAL", "fully_recovered")
    assert changed is True
    assert gov.state == "NORMAL"


def _base_now_minus(*, seconds: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(seconds=seconds)


def test_recovery_policy_backoff_sequence() -> None:
    policy = RecoveryPolicy(_base_config()["recovery"])
    sequence = []
    for _ in range(7):
        policy.on_failure(datetime.now(timezone.utc))
        sequence.append(policy.get_backoff_seconds())
    assert sequence == [5, 10, 20, 40, 80, 160, 300]


def test_sync_transition_queues_pending_write_without_event_loop() -> None:
    writer = FakeStateWriter()
    gov = RiskGovernor(config=_base_config(), event_bus=None, state_writer=writer)

    gov.halt("queued sync halt")

    assert writer.risk_states == []
    assert len(gov._pending_risk_states) == 1

    asyncio.run(gov.flush_pending_state_writes())
    assert len(writer.risk_states) == 1
    assert writer.risk_states[0].current_state.value == "VENUE_HALT"
    assert writer.risk_states[0].previous_state.value == "NORMAL"


@pytest.mark.asyncio
async def test_state_transitions_write_to_risk_state_log(tmp_path: Path) -> None:
    db_path = tmp_path / "risk-governor.sqlite"
    _init_sqlite_schema(db_path)

    sw = StateWriter(str(db_path))
    await sw.start()
    try:
        gov = RiskGovernor(config=_base_config(), event_bus=None, state_writer=sw)
        gov.halt("persisted halt")
        await gov.flush_pending_state_writes()
        await asyncio.sleep(0.2)

        got = await sw.query_risk_state()
        assert got is not None
        assert got.current_state.value == "VENUE_HALT"
        assert got.previous_state.value == "NORMAL"
        assert got.reason == "persisted halt"
        assert got.metadata["trigger"] == "manual_halt"
    finally:
        await sw.stop()


def test_state_history_tracks_reason_and_metadata(gov: RiskGovernor) -> None:
    gov.evaluate_connection_risk(
        gateway_connected=False,
        health_ok=False,
        consecutive_failures=2,
    )
    history = gov.state_history
    assert history[-1]["from"] == "NORMAL"
    assert history[-1]["to"] == "OFFLINE"
    assert history[-1]["reason"] == "connection_unhealthy"
    assert history[-1]["metadata"]["consecutive_failures"] == 2
