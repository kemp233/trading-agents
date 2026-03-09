from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
import pytest_asyncio

from agents.reconciler import Reconciler
from agents.risk_governor import RiskGovernor
from core.state_writer import StateWriter
from core.venue_order_spec import VenuePosition


def _base_config() -> dict:
    return {
        "reconciliation": {
            "check_interval_sec": 0.05,
            "drift_threshold_pct": 0.01,
            "max_drift_before_halt": 0.05,
        },
        "recovery": {
            "exponential_backoff_base_sec": 5,
            "max_backoff_sec": 300,
            "cooldown_after_recovery_sec": 0,
            "post_recovery_scale": 0.5,
            "post_recovery_duration_sec": 0,
        },
        "flash_crash": {
            "atr_multiplier": 3,
            "abs_return_1s_threshold": 0.03,
            "abs_return_5s_threshold": 0.05,
        },
        "circuit_breaker": {
            "attempt_flatten_timeout_sec": 0.1,
            "staged_exit_batch_pct": 0.5,
            "staged_exit_interval_sec": 0,
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


def _upsert_local_position(
    db_path: Path,
    *,
    symbol: str = "rb2510",
    venue: str = "ctp",
    side: str = "LONG",
    quantity: float,
    entry_price: float = 3500.0,
) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "INSERT OR REPLACE INTO positions (symbol, venue, side, quantity, entry_price, unrealized_pnl, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (symbol, venue, side, quantity, entry_price, 0.0, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
    finally:
        conn.close()


class StubVenueAdapter:
    def __init__(self, positions: list[VenuePosition] | None = None) -> None:
        self.positions = list(positions or [])
        self.query_calls = 0

    async def query_positions(self) -> list[VenuePosition]:
        self.query_calls += 1
        return list(self.positions)


def _make_venue_position(
    quantity: str,
    *,
    symbol: str = "rb2510",
    venue: str = "ctp",
    side: str = "LONG",
) -> VenuePosition:
    return VenuePosition(
        symbol=symbol,
        venue=venue,
        side=side,
        quantity=Decimal(quantity),
        entry_price=Decimal("3500"),
        unrealized_pnl=Decimal("0"),
        updated_at=datetime.now(timezone.utc),
    )


@pytest_asyncio.fixture
async def db_and_writer(tmp_path: Path):
    db_path = tmp_path / "reconciler.sqlite"
    _init_sqlite_schema(db_path)
    sw = StateWriter(str(db_path))
    await sw.start()
    yield db_path, sw
    await sw.stop()


@pytest.mark.asyncio
async def test_matching_positions_stay_balanced(db_and_writer) -> None:
    db_path, sw = db_and_writer
    _upsert_local_position(db_path, quantity=2.0)
    adapter = StubVenueAdapter([_make_venue_position("2")])
    gov = RiskGovernor(config=_base_config(), event_bus=None, state_writer=None)
    reconciler = Reconciler(sw, adapter, gov, _base_config())

    result = await reconciler.run_once()

    assert result["drift_pct"] == 0.0
    assert result["is_balanced"] is True
    assert result["mismatches"] == []
    assert gov.state == "NORMAL"
    assert reconciler.last_success_at is not None


@pytest.mark.asyncio
async def test_warning_drift_sets_degraded(db_and_writer) -> None:
    db_path, sw = db_and_writer
    _upsert_local_position(db_path, quantity=102.0)
    adapter = StubVenueAdapter([_make_venue_position("100")])
    gov = RiskGovernor(config=_base_config(), event_bus=None, state_writer=None)
    reconciler = Reconciler(sw, adapter, gov, _base_config())

    result = await reconciler.run_once()

    assert result["drift_pct"] == pytest.approx(0.02)
    assert gov.state == "DEGRADED"


@pytest.mark.asyncio
async def test_breach_drift_sets_reconciling(db_and_writer) -> None:
    db_path, sw = db_and_writer
    _upsert_local_position(db_path, quantity=105.0)
    adapter = StubVenueAdapter([_make_venue_position("100")])
    gov = RiskGovernor(config=_base_config(), event_bus=None, state_writer=None)
    reconciler = Reconciler(sw, adapter, gov, _base_config())

    result = await reconciler.run_once()

    assert result["drift_pct"] == pytest.approx(0.05)
    assert gov.state == "RECONCILING"


@pytest.mark.asyncio
async def test_local_only_position_counts_as_severe_drift(db_and_writer) -> None:
    db_path, sw = db_and_writer
    _upsert_local_position(db_path, quantity=2.0)
    adapter = StubVenueAdapter([])
    gov = RiskGovernor(config=_base_config(), event_bus=None, state_writer=None)
    reconciler = Reconciler(sw, adapter, gov, _base_config())

    result = await reconciler.run_once()

    assert result["drift_pct"] == 1.0
    assert len(result["mismatches"]) == 1
    assert gov.state == "RECONCILING"


@pytest.mark.asyncio
async def test_empty_positions_are_balanced(db_and_writer) -> None:
    _, sw = db_and_writer
    adapter = StubVenueAdapter([])
    gov = RiskGovernor(config=_base_config(), event_bus=None, state_writer=None)
    reconciler = Reconciler(sw, adapter, gov, _base_config())

    result = await reconciler.run_once()

    assert result["drift_pct"] == 0.0
    assert result["local_position_count"] == 0
    assert result["venue_position_count"] == 0
    assert gov.state == "NORMAL"


@pytest.mark.asyncio
async def test_missing_keys_are_compared_against_zero(db_and_writer) -> None:
    db_path, sw = db_and_writer
    _upsert_local_position(db_path, symbol="rb2510", side="LONG", quantity=2.0)
    adapter = StubVenueAdapter([
        _make_venue_position("2", symbol="rb2510", side="LONG"),
        _make_venue_position("1", symbol="ag2512", side="SHORT"),
    ])
    gov = RiskGovernor(config=_base_config(), event_bus=None, state_writer=None)
    reconciler = Reconciler(sw, adapter, gov, _base_config())

    result = await reconciler.run_once()

    assert len(result["mismatches"]) == 1
    assert result["mismatches"][0]["symbol"] == "ag2512"
    assert result["mismatches"][0]["local_qty"] == "0"
    assert result["mismatches"][0]["venue_qty"] == "1"


@pytest.mark.asyncio
async def test_start_and_stop_manage_polling_loop(db_and_writer) -> None:
    db_path, sw = db_and_writer
    _upsert_local_position(db_path, quantity=2.0)
    adapter = StubVenueAdapter([_make_venue_position("2")])
    gov = RiskGovernor(config=_base_config(), event_bus=None, state_writer=None)
    reconciler = Reconciler(sw, adapter, gov, _base_config())

    await reconciler.start()
    await asyncio.sleep(0.16)
    await reconciler.stop()

    assert adapter.query_calls >= 2
    assert reconciler.last_result is not None


@pytest.mark.asyncio
async def test_balanced_follow_up_recovers_back_to_normal(db_and_writer) -> None:
    db_path, sw = db_and_writer
    _upsert_local_position(db_path, quantity=105.0)
    adapter = StubVenueAdapter([_make_venue_position("100")])
    gov = RiskGovernor(config=_base_config(), event_bus=None, state_writer=None)
    reconciler = Reconciler(sw, adapter, gov, _base_config())

    await reconciler.run_once()
    assert gov.state == "RECONCILING"

    _upsert_local_position(db_path, quantity=100.0)
    result = await reconciler.run_once()

    assert result["is_balanced"] is True
    assert gov.state == "NORMAL"
    assert [item["to"] for item in gov.state_history[-3:]] == ["RECONCILING", "DEGRADED", "NORMAL"]


@pytest.mark.asyncio
async def test_reconciliation_writes_risk_state_log_metadata(db_and_writer) -> None:
    db_path, sw = db_and_writer
    _upsert_local_position(db_path, quantity=105.0)
    adapter = StubVenueAdapter([_make_venue_position("100")])
    gov = RiskGovernor(config=_base_config(), event_bus=None, state_writer=sw)
    reconciler = Reconciler(sw, adapter, gov, _base_config())

    await reconciler.run_once()
    await gov.flush_pending_state_writes()
    await asyncio.sleep(0.2)
    risk_state = await sw.query_risk_state()

    assert risk_state is not None
    assert risk_state.current_state.value == "RECONCILING"
    assert risk_state.reason == "reconciliation_drift_breach"
    assert risk_state.metadata["source"] == "reconciler"
    assert risk_state.metadata["mismatches"][0]["delta_qty"] == "5.0"

