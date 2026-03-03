from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import aiosqlite

from core.state_writer import StateWriter
from core.venue_order_spec import VenueOrderSpec
from venue.base import VenueAdapter

logger = logging.getLogger(__name__)


class OutboxDispatcher:
    """
    Polls outbox_orders table for NEW entries and dispatches them to the venue.

    Uses its own dedicated aiosqlite connection (separate from StateWriter's
    connection) to avoid shared-connection transaction interleaving under WAL mode.
    """

    def __init__(
        self,
        state_writer: StateWriter,
        venue_adapter: VenueAdapter,
        poll_interval: float = 0.5,
        max_retries: int = 3,
        backoff_base: float = 5.0,
    ) -> None:
        self._state_writer = state_writer
        self._venue_adapter = venue_adapter
        self._poll_interval = poll_interval
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._running: bool = False
        self._dispatch_task: Optional[asyncio.Task] = None
        self._db: Optional[aiosqlite.Connection] = None

    async def start(self) -> None:
        """Start the dispatcher: open own DB connection and launch dispatch loop."""
        if self._running:
            return
        self._db = await aiosqlite.connect(self._state_writer._db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL;")
        await self._db.execute("PRAGMA busy_timeout=5000;")
        self._running = True
        self._dispatch_task = asyncio.create_task(self._dispatch_loop())

    async def stop(self) -> None:
        """Stop the dispatcher: signal loop to exit, wait for current batch, close DB."""
        if not self._running:
            return
        self._running = False
        if self._dispatch_task is not None:
            await self._dispatch_task
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def _dispatch_loop(self) -> None:
        """Main poll loop: fetch NEW outbox entries and process each one."""
        while self._running:
            try:
                rows = await self._fetch_pending_orders()
                for row in rows:
                    if not self._running:
                        break
                    await self._process_one(row)
            except Exception as e:
                logger.error(f"Unhandled error in dispatch loop: {e}", exc_info=True)
            await asyncio.sleep(self._poll_interval)

    async def _fetch_pending_orders(self) -> list[dict]:
        """Query outbox_orders WHERE status='NEW', return as list of dicts."""
        if self._db is None:
            raise RuntimeError("Dispatcher not started")
        rows: list[dict] = []
        async with self._db.execute(
            "SELECT * FROM outbox_orders WHERE status = 'NEW' ORDER BY created_at LIMIT 10"
        ) as cursor:
            async for row in cursor:
                rows.append(dict(row))
        return rows

    async def _process_one(self, row: dict) -> None:
        """
        Process a single outbox entry:
        - Deserialize payload → VenueOrderSpec
        - Call venue_adapter.submit_order()
        - On success: atomically mark CONFIRMED + order SENT
        - On failure: increment retry_count or mark FAILED
        """
        if self._db is None:
            raise RuntimeError("Dispatcher not started")

        event_id: str = row["event_id"]
        aggregate_id: str = row["aggregate_id"]
        retry_count: int = row["retry_count"]
        max_retries: int = row.get("max_retries") or self._max_retries

        try:
            payload_dict = json.loads(row["payload"])
            spec = VenueOrderSpec.from_dict(payload_dict)

            await self._venue_adapter.submit_order(spec)

            now = datetime.now(timezone.utc).isoformat()

            # Atomic: both UPDATEs in one transaction (no manual BEGIN needed —
            # aiosqlite auto-begins on first DML, commit() ends it)
            await self._db.execute(
                "UPDATE outbox_orders SET status = 'CONFIRMED', sent_at = ? WHERE event_id = ?",
                (now, event_id),
            )
            await self._db.execute(
                "UPDATE orders SET status = 'SENT', updated_at = ? WHERE order_id = ?",
                (now, aggregate_id),
            )
            await self._db.commit()

        except Exception as e:
            new_retry_count = retry_count + 1

            if new_retry_count < max_retries:
                backoff_seconds = min(self._backoff_base * (2 ** new_retry_count), 300.0)
                logger.warning(
                    f"Order {aggregate_id} failed "
                    f"(attempt {new_retry_count}/{max_retries}), "
                    f"retrying in {backoff_seconds}s: {e}"
                )
                await self._db.execute(
                    "UPDATE outbox_orders SET retry_count = ? WHERE event_id = ?",
                    (new_retry_count, event_id),
                )
                await self._db.commit()
                await asyncio.sleep(backoff_seconds)

            else:
                error_message = str(e)
                logger.error(
                    f"Order {aggregate_id} permanently failed after "
                    f"{new_retry_count} attempts: {error_message}",
                    exc_info=True,
                )
                await self._db.execute(
                    "UPDATE outbox_orders SET status = 'FAILED', error_message = ? WHERE event_id = ?",
                    (error_message, event_id),
                )
                await self._db.execute(
                    "UPDATE orders SET status = 'FAILED' WHERE order_id = ?",
                    (aggregate_id,),
                )
                await self._db.commit()

    async def get_pending_count(self) -> int:
        """Return count of outbox_orders WHERE status='NEW' (for monitoring)."""
        if self._db is None:
            raise RuntimeError("Dispatcher not started")
        async with self._db.execute(
            "SELECT COUNT(*) AS count FROM outbox_orders WHERE status = 'NEW'"
        ) as cursor:
            row = await cursor.fetchone()
            return int(row["count"]) if row else 0

    async def get_failed_count(self) -> int:
        """Return count of outbox_orders WHERE status='FAILED' (for alerting)."""
        if self._db is None:
            raise RuntimeError("Dispatcher not started")
        async with self._db.execute(
            "SELECT COUNT(*) AS count FROM outbox_orders WHERE status = 'FAILED'"
        ) as cursor:
            row = await cursor.fetchone()
            return int(row["count"]) if row else 0
