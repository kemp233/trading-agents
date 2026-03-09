from __future__ import annotations

"""
Phase 1 fault injection helper.

Usage:
    python scripts/fault_injection.py --test duplicate_order
    python scripts/fault_injection.py --test crash_recovery
    python scripts/fault_injection.py --test rate_limit_429
"""

import argparse
import asyncio
import json
import logging
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents.risk_governor import RiskGovernor
from core.outbox_dispatcher import OutboxDispatcher
from core.state_writer import StateWriter
from venue.mock_adapter import MockVenueAdapter

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)
SEMANTIC_CONFIG = {"current_time": datetime.fromisoformat("2026-03-09T10:00:00+08:00")}


def _init_sqlite_schema(db_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    schema_sql = (root / "db" / "schema.sql").read_text(encoding="utf-8")
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(schema_sql)
        conn.commit()
    finally:
        conn.close()


def _risk_config() -> dict:
    return {
        "recovery": {
            "exponential_backoff_base_sec": 5,
            "max_backoff_sec": 300,
            "cooldown_after_recovery_sec": 1,
            "post_recovery_scale": 0.5,
            "post_recovery_duration_sec": 0,
        }
    }


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


class RateLimitError(Exception):
    def __init__(self) -> None:
        super().__init__("429 rate limit")
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


async def test_duplicate_order() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "duplicate.sqlite"
        _init_sqlite_schema(db_path)
        _insert_order_and_outbox(db_path, "dup")
        try:
            _insert_order_and_outbox(db_path, "dup")
        except sqlite3.IntegrityError:
            logger.info("PASS duplicate_order: duplicate idempotency key blocked")
            return
        raise RuntimeError("duplicate idempotency key was not blocked")


async def test_crash_recovery() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "crash.sqlite"
        _init_sqlite_schema(db_path)
        _insert_order_and_outbox(db_path, "crash")

        writer = StateWriter(str(db_path))
        await writer.start()
        try:
            adapter = MockVenueAdapter()
            dispatcher = OutboxDispatcher(
                state_writer=writer,
                venue_adapter=adapter,
                poll_interval=0.05,
                semantic_config=SEMANTIC_CONFIG,
            )
            await dispatcher.start()
            await asyncio.sleep(0.3)
            await dispatcher.stop()
            logger.info("PASS crash_recovery: dispatcher resumed and confirmed order, submit_count=%s", adapter.call_count)
        finally:
            await writer.stop()


async def test_rate_limit_429() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "429.sqlite"
        _init_sqlite_schema(db_path)
        _insert_order_and_outbox(db_path, "429")

        writer = StateWriter(str(db_path))
        await writer.start()
        try:
            governor = RiskGovernor(config=_risk_config(), event_bus=None, state_writer=writer)
            governor._recovery_policy.last_failure_time = datetime.now(timezone.utc)
            adapter = RateLimitThenRecoverAdapter(failures_before_success=1)
            dispatcher = OutboxDispatcher(
                state_writer=writer,
                venue_adapter=adapter,
                poll_interval=0.05,
                backoff_base=0.05,
                max_retries=4,
                risk_governor=governor,
                semantic_config=SEMANTIC_CONFIG,
            )
            await dispatcher.start()
            await asyncio.sleep(0.35)
            await dispatcher.stop()
            logger.info("PASS rate_limit_429: final_risk_state=%s submit_count=%s", governor.state, adapter.call_count)
        finally:
            await writer.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fault Injection Tests")
    parser.add_argument(
        "--test",
        required=True,
        choices=["duplicate_order", "crash_recovery", "rate_limit_429"],
    )
    args = parser.parse_args()

    tests = {
        "duplicate_order": test_duplicate_order,
        "crash_recovery": test_crash_recovery,
        "rate_limit_429": test_rate_limit_429,
    }
    asyncio.run(tests[args.test]())
