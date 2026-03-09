from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import aiosqlite

from core.state_schema import SystemLogEntry
from core.instrument_master import get_instrument_spec, load_instruments_from_yaml
from core.state_writer import StateWriter
from core.venue_order_spec import VenueOrderSpec
from validators.semantic_validators import (
    SemanticValidationError,
    SemanticValidators,
    build_validation_intent,
)
from venue.base import VenueAdapter

logger = logging.getLogger(__name__)

_CONFIRMED_ORDER_STATUSES = {"SENT", "ACKED", "ACCEPTED", "PARTIAL", "PARTIALLY_FILLED", "FILLED"}
_FAILED_ORDER_STATUSES = {"REJECTED"}


class OutboxDispatcher:
    """
    Polls outbox_orders for NEW entries and dispatches them to the venue.
    """

    def __init__(
        self,
        state_writer: StateWriter,
        venue_adapter: VenueAdapter,
        poll_interval: float = 0.5,
        max_retries: int = 3,
        backoff_base: float = 5.0,
        instrument_config_path: str | None = None,
        semantic_config: dict | None = None,
        risk_governor=None,
    ) -> None:
        self._state_writer = state_writer
        self._venue_adapter = venue_adapter
        self._poll_interval = poll_interval
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._running: bool = False
        self._dispatch_task: Optional[asyncio.Task] = None
        self._db: Optional[aiosqlite.Connection] = None
        self._semantic_config = semantic_config or {}
        self._validator = SemanticValidators(state_reader=state_writer, config=self._semantic_config)
        self._instrument_specs = self._load_instrument_specs(instrument_config_path)
        self._risk_governor = risk_governor

    async def start(self) -> None:
        if self._running:
            return
        self._db = await aiosqlite.connect(self._state_writer._db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL;")
        await self._db.execute("PRAGMA busy_timeout=5000;")
        self._running = True
        self._dispatch_task = asyncio.create_task(self._dispatch_loop())

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        if self._dispatch_task is not None:
            await self._dispatch_task
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def _dispatch_loop(self) -> None:
        while self._running:
            try:
                rows = await self._fetch_pending_orders()
                for row in rows:
                    if not self._running:
                        break
                    await self._process_one(row)
            except Exception as exc:
                logger.error("Unhandled error in dispatch loop: %s", exc, exc_info=True)
            await asyncio.sleep(self._poll_interval)

    async def _fetch_pending_orders(self) -> list[dict]:
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
        if self._db is None:
            raise RuntimeError("Dispatcher not started")

        event_id: str = row["event_id"]
        aggregate_id: str = row["aggregate_id"]
        retry_count: int = row["retry_count"]
        max_retries: int = row.get("max_retries") or self._max_retries

        try:
            if self._risk_governor is not None and self._risk_governor.state == "RECONCILING":
                await self._mark_failed(
                    event_id,
                    aggregate_id,
                    "risk governor blocked order while RECONCILING",
                )
                return

            payload_dict = json.loads(row["payload"])
            spec = VenueOrderSpec.from_dict(payload_dict)
            instrument_spec = self._resolve_instrument_spec(spec.symbol)
            validation_intent = await self._build_validation_intent(spec)
            validation = self._validator.assert_trade_intent(validation_intent, instrument_spec)
            if validation.warnings:
                logger.warning(
                    "Semantic validation warnings for %s: %s",
                    aggregate_id,
                    "; ".join(validation.warnings),
                )

            recovered = False
            if retry_count > 0:
                recovered = await self._reconcile_existing_submission(event_id, aggregate_id, spec)
            if recovered:
                await self._maybe_recover_from_degraded()
                return

            await self._venue_adapter.submit_order(spec)
            await self._maybe_recover_from_degraded()

            now = datetime.now(timezone.utc).isoformat()
            await self._db.execute(
                "UPDATE outbox_orders SET status = 'CONFIRMED', sent_at = ? WHERE event_id = ?",
                (now, event_id),
            )
            await self._db.execute(
                "UPDATE orders SET status = 'SENT', updated_at = ? WHERE order_id = ?",
                (now, aggregate_id),
            )
            await self._db.commit()

        except SemanticValidationError as exc:
            await self._mark_failed(event_id, aggregate_id, str(exc))
        except Exception as exc:
            if self._is_rate_limit_error(exc):
                await self._handle_rate_limit(exc, aggregate_id)
            new_retry_count = retry_count + 1
            if new_retry_count < max_retries:
                backoff_seconds = self._compute_backoff_seconds(new_retry_count)
                logger.warning(
                    "Order %s failed (attempt %s/%s), retrying in %ss: %s",
                    aggregate_id,
                    new_retry_count,
                    max_retries,
                    backoff_seconds,
                    exc,
                )
                await self._db.execute(
                    "UPDATE outbox_orders SET retry_count = ? WHERE event_id = ?",
                    (new_retry_count, event_id),
                )
                await self._db.commit()
                await asyncio.sleep(backoff_seconds)
            else:
                logger.error(
                    "Order %s permanently failed after %s attempts: %s",
                    aggregate_id,
                    new_retry_count,
                    exc,
                    exc_info=True,
                )
                await self._emit_system_alert(
                    "OUTBOX_MAX_RETRIES_EXCEEDED",
                    f"order_id={aggregate_id} event_id={event_id} error={exc}",
                )
                await self._mark_failed(event_id, aggregate_id, str(exc))

    async def _mark_failed(self, event_id: str, aggregate_id: str, error_message: str) -> None:
        if self._db is None:
            raise RuntimeError("Dispatcher not started")
        await self._db.execute(
            "UPDATE outbox_orders SET status = 'FAILED', error_message = ? WHERE event_id = ?",
            (error_message, event_id),
        )
        await self._db.execute(
            "UPDATE orders SET status = 'FAILED', updated_at = ? WHERE order_id = ?",
            (datetime.now(timezone.utc).isoformat(), aggregate_id),
        )
        await self._db.commit()

    async def _reconcile_existing_submission(
        self,
        event_id: str,
        aggregate_id: str,
        spec: VenueOrderSpec,
    ) -> bool:
        persisted_client_order_id = await self._get_persisted_client_order_id(aggregate_id)
        if persisted_client_order_id and persisted_client_order_id != spec.client_order_id:
            logger.warning(
                "Skipping query_order reconciliation for %s because payload client_order_id=%s does not match persisted client_order_id=%s",
                aggregate_id,
                spec.client_order_id,
                persisted_client_order_id,
            )
            return False
        status = await self._query_existing_status(spec.client_order_id)
        if status is None:
            return False
        normalized = str(status.status).upper()
        if normalized in _CONFIRMED_ORDER_STATUSES:
            await self._confirm_recovered_order(
                event_id=event_id,
                aggregate_id=aggregate_id,
                order_status=normalized,
                filled_quantity=float(status.filled_quantity),
                filled_price=float(status.filled_price),
                detail=f"reconciled via query_order for {spec.client_order_id}",
            )
            return True
        if normalized in _FAILED_ORDER_STATUSES:
            await self._mark_failed(event_id, aggregate_id, f"venue rejected recovered order {spec.client_order_id}")
            return True
        return False

    async def _get_persisted_client_order_id(self, aggregate_id: str) -> str | None:
        if self._db is None:
            raise RuntimeError("Dispatcher not started")
        async with self._db.execute(
            "SELECT client_order_id FROM orders WHERE order_id = ?",
            (aggregate_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return str(row["client_order_id"]) if row else None

    async def _query_existing_status(self, client_order_id: str):
        try:
            return await self._venue_adapter.query_order(client_order_id)
        except Exception as exc:
            logger.info("query_order failed for %s during reconciliation: %s", client_order_id, exc)
            return None

    async def _confirm_recovered_order(
        self,
        *,
        event_id: str,
        aggregate_id: str,
        order_status: str,
        filled_quantity: float,
        filled_price: float,
        detail: str,
    ) -> None:
        if self._db is None:
            raise RuntimeError("Dispatcher not started")
        now = datetime.now(timezone.utc).isoformat()
        terminal_status = "FILLED" if order_status in {"FILLED", "PARTIAL", "PARTIALLY_FILLED"} else "SENT"
        await self._db.execute(
            "UPDATE outbox_orders SET status = 'CONFIRMED', sent_at = COALESCE(sent_at, ?) WHERE event_id = ?",
            (now, event_id),
        )
        await self._db.execute(
            "UPDATE orders SET status = ?, filled_quantity = ?, filled_price = ?, updated_at = ? WHERE order_id = ?",
            (terminal_status, filled_quantity, filled_price, now, aggregate_id),
        )
        await self._db.commit()
        await self._emit_system_alert("OUTBOX_RECOVERED_CONFIRMATION", detail)

    async def _handle_rate_limit(self, exc: Exception, aggregate_id: str) -> None:
        logger.warning("Rate limit on order %s: %s", aggregate_id, exc)
        if self._risk_governor is not None:
            try:
                self._risk_governor._recovery_policy.on_failure(datetime.now(timezone.utc))
                self._risk_governor.set_degraded("venue_rate_limited")
            except Exception:
                logger.exception("Failed to update risk governor on rate limit")
        await self._emit_system_alert("VENUE_RATE_LIMIT_429", f"order_id={aggregate_id} error={exc}")

    async def _maybe_recover_from_degraded(self) -> None:
        if self._risk_governor is None or self._risk_governor.state != "DEGRADED":
            return
        try:
            if self._risk_governor._recovery_policy.can_begin_recovery():
                self._risk_governor.transition(
                    "NORMAL",
                    "venue_rate_limit_recovered",
                    {"trigger": "venue_rate_limit_recovered"},
                )
        except Exception:
            logger.exception("Failed to recover risk governor from DEGRADED")

    async def _emit_system_alert(self, event_type: str, detail: str) -> None:
        try:
            await self._state_writer.write_system_log(
                SystemLogEntry(
                    ts=datetime.now(timezone.utc),
                    event_type=event_type,
                    detail=detail,
                )
            )
        except Exception:
            logger.exception("Failed to emit system alert %s", event_type)

    def _compute_backoff_seconds(self, retry_count: int) -> float:
        return min(self._backoff_base * (2 ** max(retry_count - 1, 0)), 300.0)

    def _is_rate_limit_error(self, exc: Exception) -> bool:
        status_code = getattr(exc, "status_code", None)
        if status_code == 429:
            return True
        message = str(exc).lower()
        return "429" in message or "rate limit" in message

    def _load_instrument_specs(self, instrument_config_path: str | None) -> dict:
        if not instrument_config_path:
            return {}
        try:
            return load_instruments_from_yaml(instrument_config_path)
        except Exception as exc:
            logger.error("Failed to load instrument config %s: %s", instrument_config_path, exc)
            raise

    def _resolve_instrument_spec(self, symbol: str):
        if symbol in self._instrument_specs:
            return self._instrument_specs[symbol]
        try:
            return get_instrument_spec(symbol)
        except KeyError as exc:
            raise SemanticValidationError(f"unknown instrument symbol: {symbol}") from exc

    async def _build_validation_intent(self, spec: VenueOrderSpec):
        account_snapshot = await self._state_writer.query_latest_account_info()
        context = {
            "current_time": self._semantic_config.get("current_time", datetime.now(timezone.utc)),
        }
        if account_snapshot:
            context.update(
                {
                    "available_funds": account_snapshot.get("available"),
                    "account_equity": account_snapshot.get("equity"),
                }
            )
        return build_validation_intent(spec, **context)

    async def get_pending_count(self) -> int:
        if self._db is None:
            raise RuntimeError("Dispatcher not started")
        async with self._db.execute(
            "SELECT COUNT(*) AS count FROM outbox_orders WHERE status = 'NEW'"
        ) as cursor:
            row = await cursor.fetchone()
            return int(row["count"]) if row else 0

    async def get_failed_count(self) -> int:
        if self._db is None:
            raise RuntimeError("Dispatcher not started")
        async with self._db.execute(
            "SELECT COUNT(*) AS count FROM outbox_orders WHERE status = 'FAILED'"
        ) as cursor:
            row = await cursor.fetchone()
            return int(row["count"]) if row else 0


