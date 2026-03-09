from __future__ import annotations

import asyncio
import json
from datetime import datetime
from typing import Dict, List, Optional, Set

import aiosqlite

from .state_schema import (
    ErrorLogEntry,
    MonitorLogEntry,
    OrderState,
    PositionState,
    RiskState,
    SystemLogEntry,
)


class StateWriter:
    def __init__(
        self,
        db_path: str,
        queue_size: int = 1000,
        batch_size: int = 50,
        batch_timeout: float = 0.1,
    ) -> None:
        self._db_path = db_path
        self._queue_size = queue_size
        self._batch_size = batch_size
        self._batch_timeout = batch_timeout
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
        self._writer_task: Optional[asyncio.Task] = None
        self._db: Optional[aiosqlite.Connection] = None
        self._running = False

    async def start(self) -> None:
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL;")
        await self._db.execute("PRAGMA busy_timeout=5000;")
        self._running = True
        self._writer_task = asyncio.create_task(self._writer_loop())

    async def stop(self) -> None:
        self._running = False
        await self._queue.put(None)
        if self._writer_task is not None:
            await self._writer_task
        if self._db is not None:
            await self._db.close()

    async def write_order(self, order: OrderState) -> None:
        await self._queue.put(("order", order.to_dict()))

    async def write_position(self, pos: PositionState) -> None:
        await self._queue.put(("position", pos.to_dict()))

    async def write_risk_state(self, risk: RiskState) -> None:
        await self._queue.put(("risk", risk.to_dict()))

    async def query_order(self, order_id: str) -> Optional[OrderState]:
        if self._db is None:
            raise RuntimeError("Database not initialized")
        async with self._db.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None
            return OrderState.from_dict(dict(row))

    async def query_orders_by_status(self, status: str) -> List[OrderState]:
        if self._db is None:
            raise RuntimeError("Database not initialized")
        orders: list[OrderState] = []
        async with self._db.execute("SELECT * FROM orders WHERE status = ?", (status,)) as cursor:
            async for row in cursor:
                orders.append(OrderState.from_dict(dict(row)))
        return orders

    async def query_positions(self) -> List[PositionState]:
        if self._db is None:
            raise RuntimeError("Database not initialized")
        positions: list[PositionState] = []
        async with self._db.execute("SELECT * FROM positions") as cursor:
            async for row in cursor:
                positions.append(PositionState.from_dict(dict(row)))
        return positions

    async def query_risk_state(self) -> Optional[RiskState]:
        if self._db is None:
            raise RuntimeError("Database not initialized")
        async with self._db.execute("SELECT * FROM risk_state_log ORDER BY id DESC LIMIT 1") as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None
            payload = dict(row)
            if isinstance(payload.get("metadata"), str):
                payload["metadata"] = json.loads(payload["metadata"])
            return RiskState.from_dict(payload)

    async def save_checkpoint(self, stream_sequences: Dict[str, int], processed_keys: Set[str]) -> None:
        if self._db is None:
            raise RuntimeError("Database not initialized")
        now = datetime.utcnow().isoformat()
        for stream_id, last_seq in stream_sequences.items():
            await self._db.execute(
                "INSERT OR REPLACE INTO stream_checkpoints(stream_id, last_seq, updated_at) VALUES (?, ?, ?)",
                (stream_id, last_seq, now),
            )
        for idempotency_key in processed_keys:
            parts = idempotency_key.rsplit(":", 1)
            if len(parts) != 2:
                continue
            stream_id, stream_seq = parts
            try:
                seq_int = int(stream_seq)
            except ValueError:
                continue
            await self._db.execute(
                "INSERT OR IGNORE INTO processed_events(stream_id, stream_seq, idempotency_key, processed_at) VALUES (?, ?, ?, ?)",
                (stream_id, seq_int, idempotency_key, now),
            )
        await self._db.commit()

    async def load_checkpoints(self) -> Dict[str, int]:
        if self._db is None:
            raise RuntimeError("Database not initialized")
        checkpoints: dict[str, int] = {}
        async with self._db.execute("SELECT stream_id, last_seq FROM stream_checkpoints") as cursor:
            async for row in cursor:
                checkpoints[row["stream_id"]] = row["last_seq"]
        return checkpoints

    async def load_processed_events(self, limit: int = 10000) -> Set[str]:
        if self._db is None:
            raise RuntimeError("Database not initialized")
        keys: set[str] = set()
        async with self._db.execute(
            "SELECT idempotency_key FROM processed_events ORDER BY processed_at DESC LIMIT ?",
            (limit,),
        ) as cursor:
            async for row in cursor:
                keys.add(row["idempotency_key"])
        return keys

    async def write_monitor_log(self, entry: MonitorLogEntry) -> None:
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "INSERT INTO monitor_log (ts, field, current_value, limit_value, level) VALUES (?, ?, ?, ?, ?)",
                (
                    entry.ts.isoformat(),
                    entry.field,
                    entry.current_value,
                    entry.limit_value,
                    entry.level,
                ),
            )
            await db.commit()

    async def write_system_log(self, entry: SystemLogEntry) -> None:
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "INSERT INTO system_log (ts, event_type, detail) VALUES (?, ?, ?)",
                (entry.ts.isoformat(), entry.event_type, entry.detail),
            )
            await db.commit()

    async def write_error_log(self, entry: ErrorLogEntry) -> None:
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "INSERT INTO error_log (ts, error_id, error_msg, context) VALUES (?, ?, ?, ?)",
                (entry.ts.isoformat(), entry.error_id, entry.error_msg, entry.context),
            )
            await db.commit()

    async def write_connection_log(
        self,
        status: str,
        front_addr: str = "",
        session_id: str = "",
        detail: str = "",
        ts: datetime | None = None,
    ) -> None:
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "INSERT INTO connection_log (ts, status, front_addr, session_id, detail) VALUES (?, ?, ?, ?, ?)",
                ((ts or datetime.utcnow()).isoformat(), status, front_addr, session_id, detail),
            )
            await db.commit()

    async def write_account_info(
        self,
        user_id: str,
        broker_id: str,
        trading_day: str,
        available: float,
        margin: float,
        equity: float,
        ts: datetime | None = None,
    ) -> None:
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "INSERT INTO account_info (ts, user_id, broker_id, trading_day, available, margin, equity) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ((ts or datetime.utcnow()).isoformat(), user_id, broker_id, trading_day, available, margin, equity),
            )
            await db.commit()

    async def query_latest_account_info(self) -> dict | None:
        if self._db is None:
            raise RuntimeError("Database not initialized")
        async with self._db.execute(
            "SELECT * FROM account_info ORDER BY ts DESC, id DESC LIMIT 1"
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def replace_positions(self, positions: list[PositionState], venue: str = "CTP") -> None:
        if self._db is None:
            raise RuntimeError("Database not initialized")
        await self._db.execute("DELETE FROM positions WHERE venue = ?", (venue,))
        for pos in positions:
            data = pos.to_dict()
            await self._db.execute(
                "INSERT OR REPLACE INTO positions (symbol, venue, side, quantity, entry_price, unrealized_pnl, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    data["symbol"],
                    data["venue"],
                    data["side"],
                    data["quantity"],
                    data["entry_price"],
                    data["unrealized_pnl"],
                    data["updated_at"],
                ),
            )
        await self._db.commit()

    async def _writer_loop(self) -> None:
        while self._running:
            batch = []
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=self._batch_timeout)
                if item is None:
                    break
                batch.append(item)
                while len(batch) < self._batch_size:
                    try:
                        item = self._queue.get_nowait()
                        if item is None:
                            break
                        batch.append(item)
                    except asyncio.QueueEmpty:
                        break
            except asyncio.TimeoutError:
                pass
            if batch:
                await self._flush_batch(batch)
        await self._drain_remaining()

    async def _flush_batch(self, batch: List) -> None:
        if self._db is None:
            raise RuntimeError("Database not initialized")
        for attempt in range(3):
            try:
                for item_type, data in batch:
                    if item_type == "order":
                        await self._db.execute(
                            "INSERT OR REPLACE INTO orders (order_id, client_order_id, symbol, venue, side, quantity, price, status, strategy_id, created_at, updated_at, filled_quantity, filled_price) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (
                                data["order_id"],
                                data["client_order_id"],
                                data["symbol"],
                                data["venue"],
                                data["side"],
                                data["quantity"],
                                data["price"],
                                data["status"],
                                data["strategy_id"],
                                data["created_at"],
                                data["updated_at"],
                                data["filled_quantity"],
                                data["filled_price"],
                            ),
                        )
                    elif item_type == "position":
                        await self._db.execute(
                            "INSERT OR REPLACE INTO positions (symbol, venue, side, quantity, entry_price, unrealized_pnl, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (
                                data["symbol"],
                                data["venue"],
                                data["side"],
                                data["quantity"],
                                data["entry_price"],
                                data["unrealized_pnl"],
                                data["updated_at"],
                            ),
                        )
                    elif item_type == "risk":
                        await self._db.execute(
                            "INSERT INTO risk_state_log (current_state, previous_state, state_changed_at, reason, metadata) VALUES (?, ?, ?, ?, ?)",
                            (
                                data["current_state"],
                                data["previous_state"],
                                data["state_changed_at"],
                                data["reason"],
                                json.dumps(data["metadata"]),
                            ),
                        )
                await self._db.commit()
                break
            except Exception:
                if attempt == 2:
                    raise
                await asyncio.sleep(0.01 * (attempt + 1))

    async def _drain_remaining(self) -> None:
        batch = []
        while True:
            try:
                item = self._queue.get_nowait()
                if item is None:
                    continue
                batch.append(item)
            except asyncio.QueueEmpty:
                break
        if batch:
            await self._flush_batch(batch)

    async def __aenter__(self) -> "StateWriter":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()

    async def query_monitor_log(self, limit: int = 100) -> list[dict]:
        if self._db is None:
            raise RuntimeError("Database not initialized")
        rows: list[dict] = []
        async with self._db.execute("SELECT * FROM monitor_log ORDER BY ts DESC LIMIT ?", (limit,)) as cursor:
            async for row in cursor:
                rows.append(dict(row))
        return rows

    async def query_system_log(self, limit: int = 100) -> list[dict]:
        if self._db is None:
            raise RuntimeError("Database not initialized")
        rows: list[dict] = []
        async with self._db.execute("SELECT * FROM system_log ORDER BY ts DESC LIMIT ?", (limit,)) as cursor:
            async for row in cursor:
                rows.append(dict(row))
        return rows

    async def query_error_log(self, limit: int = 100) -> list[dict]:
        if self._db is None:
            raise RuntimeError("Database not initialized")
        rows: list[dict] = []
        async with self._db.execute("SELECT * FROM error_log ORDER BY ts DESC LIMIT ?", (limit,)) as cursor:
            async for row in cursor:
                rows.append(dict(row))
        return rows

