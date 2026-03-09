from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import pytest_asyncio

from agents.risk_governor import RiskGovernor
from core.outbox_dispatcher import OutboxDispatcher
from core.state_writer import StateWriter
from venue.mock_adapter import MockVenueAdapter

_SEMANTIC_CONFIG = {"current_time": datetime.fromisoformat("2026-03-09T10:00:00+08:00")}


def _risk_config() -> dict:
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
            "cooldown_after_recovery_sec": 1,
            "post_recovery_scale": 0.5,
            "post_recovery_duration_sec": 0,
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


def _insert_order_and_outbox(db_path: Path, suffix: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    payload = json.dumps(
        {
            "symbol": "rb2510",
            "side": "BUY",
            "order_type": "LIMIT",
            "quantity": "1",
            "price": "3500",
            "time_in_force": "GTC",
            "reduce_only": False,
            "post_only": False,
            "hedge_flag": "SPEC",
            "client_order_id": f"client-{suffix}",
            "venue": "ctp",
        }
    )
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "INSERT INTO orders (order_id, client_order_id, symbol, venue, side, quantity, price, status, strategy_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (f"order-{suffix}", f"client-{suffix}", "rb2510", "ctp", "BUY", 1.0, 3500.0, "PENDING_SEND", "strategy-1", now, now),
        )
        conn.execute(
            "INSERT INTO outbox_orders (event_id, aggregate_id, event_type, payload, idempotency_key, status, retry_count, max_retries) VALUES (?, ?, ?, ?, ?, 'NEW', 0, 4)",
            (f"evt-{suffix}", f"order-{suffix}", "OrderCreated", payload, f"intent-{suffix}"),
        )
        conn.commit()
    finally:
        conn.close()


def _get_system_log_events(db_path: Path, event_type: str) -> list[str]:
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute(
            "SELECT detail FROM system_log WHERE event_type = ? ORDER BY id",
            (event_type,),
        )
        return [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()


class RateLimitError(Exception):
    def __init__(self, message: str = "429 rate limit") -> None:
        super().__init__(message)
        self.status_code = 429


class RateLimitThenRecoverAdapter(MockVenueAdapter):
    def __init__(self, failures_before_success: int = 2) -> None:
        super().__init__()
        self.failures_before_success = failures_before_success

    async def submit_order(self, spec):
        if self.call_count < self.failures_before_success:
            self.call_count += 1
            raise RateLimitError()
        return await super().submit_order(spec)


@pytest_asyncio.fixture()
async def db_and_writer(tmp_path: Path):
    db_path = tmp_path / "429.sqlite"
    _init_sqlite_schema(db_path)
    writer = StateWriter(str(db_path))
    await writer.start()
    try:
        yield db_path, writer
    finally:
        await writer.stop()


class TestRateLimitBackoff:
    @pytest.mark.asyncio
    async def test_429_triggers_degraded(self, db_and_writer) -> None:
        db_path, writer = db_and_writer
        _insert_order_and_outbox(db_path, "rate-limit-1")

        governor = RiskGovernor(config=_risk_config(), event_bus=None, state_writer=writer)
        adapter = RateLimitThenRecoverAdapter(failures_before_success=10)
        dispatcher = OutboxDispatcher(
            state_writer=writer,
            venue_adapter=adapter,
            poll_interval=0.05,
            backoff_base=0.01,
            max_retries=2,
            risk_governor=governor,
            semantic_config=_SEMANTIC_CONFIG,
        )
        await dispatcher.start()
        await asyncio.sleep(0.2)
        await dispatcher.stop()

        assert governor.state == "DEGRADED"
        assert _get_system_log_events(db_path, "VENUE_RATE_LIMIT_429")

    def test_exponential_backoff(self, db_and_writer) -> None:
        _, writer = db_and_writer
        dispatcher = OutboxDispatcher(
            state_writer=writer,
            venue_adapter=MockVenueAdapter(),
            backoff_base=5.0,
            semantic_config=_SEMANTIC_CONFIG,
        )
        assert [dispatcher._compute_backoff_seconds(i) for i in range(1, 8)] == [5, 10, 20, 40, 80, 160, 300]

    @pytest.mark.asyncio
    async def test_recovery_after_429(self, db_and_writer) -> None:
        db_path, writer = db_and_writer
        _insert_order_and_outbox(db_path, "rate-limit-2")

        governor = RiskGovernor(config=_risk_config(), event_bus=None, state_writer=writer)
        governor._recovery_policy.last_failure_time = datetime.now(timezone.utc) - timedelta(seconds=2)

        adapter = RateLimitThenRecoverAdapter(failures_before_success=1)
        dispatcher = OutboxDispatcher(
            state_writer=writer,
            venue_adapter=adapter,
            poll_interval=0.05,
            backoff_base=1.1,
            max_retries=4,
            risk_governor=governor,
            semantic_config=_SEMANTIC_CONFIG,
        )
        await dispatcher.start()
        await asyncio.sleep(1.5)
        await dispatcher.stop()

        assert governor.state == "NORMAL"
        assert _get_system_log_events(db_path, "VENUE_RATE_LIMIT_429")
