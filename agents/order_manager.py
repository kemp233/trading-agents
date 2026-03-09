from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

import aiosqlite
import yaml

from core.event_envelope import EventEnvelope, EventType
from core.instrument_master import get_instrument_spec, load_instruments_from_yaml
from core.state_schema import OrderSide, OrderState, OrderStatus
from core.venue_order_spec import VenueOrderSpec, VenueOrderStatus
from validators.semantic_validators import (
    SemanticValidationError,
    SemanticValidators,
    build_validation_intent,
)

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "risk_params.yaml"
_TERMINAL_STATUSES = {
    OrderStatus.FILLED.value,
    OrderStatus.CANCELED.value,
    OrderStatus.REJECTED.value,
    OrderStatus.FAILED.value,
}
_ACTIVE_POLL_STATUSES = (OrderStatus.SENT.value, OrderStatus.PARTIALLY_FILLED.value)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _load_config(config: dict[str, Any] | None) -> dict[str, Any]:
    file_cfg: dict[str, Any] = {}
    if _DEFAULT_CONFIG_PATH.exists():
        with _DEFAULT_CONFIG_PATH.open("r", encoding="utf-8") as fh:
            loaded = yaml.safe_load(fh) or {}
            if isinstance(loaded, dict):
                file_cfg = loaded
    if not config:
        return file_cfg
    return _deep_merge(file_cfg, config)


def _as_order_side(value: str) -> OrderSide:
    return OrderSide.BUY if value == "BUY" else OrderSide.SELL


@dataclass(slots=True)
class SubmissionResult:
    order_id: str
    client_order_id: str
    status: str
    accepted: bool
    error: str | None = None
    duplicate: bool = False


class OrderManager:
    def __init__(
        self,
        event_bus,
        state_writer,
        venue_adapter,
        risk_governor,
        config: dict[str, Any] | None,
        instrument_config_path: str | None = None,
    ) -> None:
        self._config = _load_config(config)
        self._event_bus = event_bus
        self._state_writer = state_writer
        self._venue_adapter = venue_adapter
        self._risk_governor = risk_governor
        self._poll_interval = float(self._config.get("order_manager", {}).get("status_poll_interval_sec", 0.1))
        self._validator = SemanticValidators(state_reader=state_writer, config=self._config)
        self._instrument_specs = self._load_instrument_specs(instrument_config_path)
        self._status_task: Optional[asyncio.Task] = None
        self._started = False
        self._subscribed = False
        self._event_sequences: dict[str, int] = defaultdict(int)
        self._last_published_status: dict[str, str] = {}
        self._publish_lock = asyncio.Lock()

    async def start(self) -> None:
        if self._started:
            return
        if self._event_bus is not None and not self._subscribed:
            self._event_bus.subscribe(EventType.TRADE_INTENT, self.handle_trade_intent)
            self._subscribed = True
        self._started = True
        self._status_task = asyncio.create_task(self._status_loop())

    async def stop(self) -> None:
        self._started = False
        if self._status_task is not None:
            self._status_task.cancel()
            try:
                await self._status_task
            except asyncio.CancelledError:
                pass
            self._status_task = None

    async def submit_trade_intent(self, payload: dict[str, Any]) -> SubmissionResult:
        return await self._submit_trade_intent(payload, allow_reduce_only_override=False)

    async def handle_trade_intent(self, envelope: EventEnvelope) -> SubmissionResult:
        payload = dict(envelope.payload)
        payload.setdefault("idempotency_key", envelope.idempotency_key)
        return await self.submit_trade_intent(payload)

    async def cancel_all(self) -> dict[str, Any]:
        if self._risk_governor is None:
            raise RuntimeError("risk_governor is required for cancel_all")

        await self._risk_governor.cancel_all_orders(self._venue_adapter)
        canceled = await self._cancel_pending_orders()
        synced = await self._sync_orders_once(include_pending=False)
        return {"canceled_locally": canceled, "synced": synced}

    async def flatten(self, symbol: str | None = None) -> list[SubmissionResult]:
        positions = await self._venue_adapter.query_positions()
        results: list[SubmissionResult] = []
        for position in positions:
            quantity = Decimal(str(position.quantity))
            if quantity <= 0:
                continue
            if symbol and position.symbol != symbol:
                continue
            side = "SELL" if position.side == "LONG" else "BUY"
            payload = {
                "symbol": position.symbol,
                "side": side,
                "order_type": "MARKET",
                "quantity": str(quantity),
                "price": None,
                "time_in_force": "IOC",
                "reduce_only": True,
                "post_only": False,
                "hedge_flag": "SPEC",
                "venue": position.venue,
                "client_order_id": f"flatten-{position.symbol}-{uuid4().hex[:8]}",
            }
            results.append(
                await self._submit_trade_intent(payload, allow_reduce_only_override=True)
            )
        return results

    async def _submit_trade_intent(
        self,
        payload: dict[str, Any],
        *,
        allow_reduce_only_override: bool,
    ) -> SubmissionResult:
        order_id = str(payload.get("order_id") or uuid4())
        client_order_id = str(payload.get("client_order_id") or f"ord-{uuid4().hex[:12]}")
        payload = dict(payload)
        payload["client_order_id"] = client_order_id
        idempotency_key = str(payload.get("idempotency_key") or client_order_id)

        try:
            spec = self._build_spec(payload)
            existing = await self._find_existing_order(client_order_id, idempotency_key)
            if existing is not None:
                return SubmissionResult(
                    order_id=existing.order_id,
                    client_order_id=existing.client_order_id,
                    status=existing.status.value,
                    accepted=existing.status.value not in {
                        OrderStatus.REJECTED.value,
                        OrderStatus.FAILED.value,
                    },
                    duplicate=True,
                )
            instrument_spec = self._resolve_instrument_spec(spec.symbol)
            self._assert_order_allowed(spec, allow_reduce_only_override=allow_reduce_only_override)
            validation_intent = await self._build_validation_intent(spec, payload)
            self._validator.assert_trade_intent(validation_intent, instrument_spec)
        except (SemanticValidationError, ValueError, KeyError) as exc:
            order = self._build_order_state(
                order_id=order_id,
                spec=self._build_rejected_spec(payload),
                status=OrderStatus.REJECTED,
            )
            await self._insert_terminal_order(order)
            await self._publish_order_update(order, reason=str(exc), error=str(exc))
            return SubmissionResult(
                order_id=order_id,
                client_order_id=client_order_id,
                status=OrderStatus.REJECTED.value,
                accepted=False,
                error=str(exc),
            )

        order = self._build_order_state(
            order_id=order_id,
            spec=spec,
            status=OrderStatus.PENDING_SEND,
        )
        await self._insert_order_and_outbox(order, spec, idempotency_key)
        await self._publish_order_update(order)
        return SubmissionResult(
            order_id=order.order_id,
            client_order_id=order.client_order_id,
            status=order.status.value,
            accepted=True,
        )

    def _build_spec(self, payload: dict[str, Any]) -> VenueOrderSpec:
        spec_payload = dict(payload)
        if "client_order_id" not in spec_payload or not spec_payload["client_order_id"]:
            spec_payload["client_order_id"] = f"ord-{uuid4().hex[:12]}"
        return VenueOrderSpec.from_dict(spec_payload)

    def _build_rejected_spec(self, payload: dict[str, Any]) -> VenueOrderSpec:
        quantity = payload.get("quantity", "0")
        try:
            quantity_decimal = Decimal(str(quantity))
        except Exception:
            quantity_decimal = Decimal("0")
        price = payload.get("price")
        try:
            parsed_price = None if price is None else Decimal(str(price))
        except Exception:
            parsed_price = None
        return VenueOrderSpec(
            symbol=str(payload.get("symbol", "UNKNOWN")),
            side=str(payload.get("side", "BUY")) if str(payload.get("side", "BUY")) in {"BUY", "SELL"} else "BUY",
            order_type="MARKET" if parsed_price is None else "LIMIT",
            quantity=quantity_decimal,
            price=parsed_price,
            time_in_force="IOC" if parsed_price is None else "GTC",
            reduce_only=bool(payload.get("reduce_only", False)),
            post_only=bool(payload.get("post_only", False)),
            hedge_flag=str(payload.get("hedge_flag", "SPEC")) if str(payload.get("hedge_flag", "SPEC")) in {"SPEC", "HEDGE"} else "SPEC",
            client_order_id=str(payload.get("client_order_id", "")),
            venue=str(payload.get("venue", "")),
        )

    def _assert_order_allowed(
        self,
        spec: VenueOrderSpec,
        *,
        allow_reduce_only_override: bool,
    ) -> None:
        if self._risk_governor is None:
            return
        if allow_reduce_only_override and spec.reduce_only:
            return
        if not self._risk_governor.can_trade():
            raise ValueError(f"risk governor blocked order while {self._risk_governor.state}")
        if not spec.reduce_only and not self._risk_governor.can_open_new_position():
            raise ValueError(f"risk governor blocked new position while {self._risk_governor.state}")

    def _build_order_state(
        self,
        *,
        order_id: str,
        spec: VenueOrderSpec,
        status: OrderStatus,
    ) -> OrderState:
        now = _now_utc()
        return OrderState(
            order_id=order_id,
            client_order_id=spec.client_order_id,
            symbol=spec.symbol,
            venue=spec.venue,
            side=_as_order_side(spec.side),
            quantity=float(spec.quantity),
            price=None if spec.price is None else float(spec.price),
            status=status,
            strategy_id=None,
            created_at=now,
            updated_at=now,
        )

    async def _insert_terminal_order(self, order: OrderState) -> None:
        async with aiosqlite.connect(self._state_writer._db_path) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA busy_timeout=5000;")
            await db.execute(
                "INSERT OR REPLACE INTO orders (order_id, client_order_id, symbol, venue, side, quantity, price, status, strategy_id, created_at, updated_at, filled_quantity, filled_price) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    order.order_id,
                    order.client_order_id,
                    order.symbol,
                    order.venue,
                    order.side.value,
                    order.quantity,
                    order.price,
                    order.status.value,
                    order.strategy_id,
                    order.created_at.isoformat(),
                    order.updated_at.isoformat(),
                    order.filled_quantity,
                    order.filled_price,
                ),
            )
            await db.commit()

    async def _insert_order_and_outbox(
        self,
        order: OrderState,
        spec: VenueOrderSpec,
        idempotency_key: str,
    ) -> None:
        outbox_event_id = str(uuid4())
        async with aiosqlite.connect(self._state_writer._db_path) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA busy_timeout=5000;")
            await db.execute("BEGIN IMMEDIATE")
            try:
                await db.execute(
                    "INSERT INTO orders (order_id, client_order_id, symbol, venue, side, quantity, price, status, strategy_id, created_at, updated_at, filled_quantity, filled_price) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        order.order_id,
                        order.client_order_id,
                        order.symbol,
                        order.venue,
                        order.side.value,
                        order.quantity,
                        order.price,
                        order.status.value,
                        order.strategy_id,
                        order.created_at.isoformat(),
                        order.updated_at.isoformat(),
                        order.filled_quantity,
                        order.filled_price,
                    ),
                )
                await db.execute(
                    "INSERT INTO outbox_orders (event_id, aggregate_id, event_type, payload, idempotency_key, status, retry_count, max_retries) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        outbox_event_id,
                        order.order_id,
                        "OrderCreated",
                        json.dumps(spec.to_dict()),
                        idempotency_key,
                        "NEW",
                        0,
                        3,
                    ),
                )
                await db.commit()
            except Exception:
                await db.rollback()
                raise

    async def _find_existing_order(
        self,
        client_order_id: str,
        idempotency_key: str,
    ) -> Optional[OrderState]:
        async with aiosqlite.connect(self._state_writer._db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM orders WHERE client_order_id = ? LIMIT 1",
                (client_order_id,),
            ) as cursor:
                row = await cursor.fetchone()
                if row is not None:
                    return OrderState.from_dict(dict(row))
            async with db.execute(
                "SELECT aggregate_id FROM outbox_orders WHERE idempotency_key = ? LIMIT 1",
                (idempotency_key,),
            ) as cursor:
                row = await cursor.fetchone()
                if row is None:
                    return None
                order_id = row["aggregate_id"]
            async with db.execute(
                "SELECT * FROM orders WHERE order_id = ? LIMIT 1",
                (order_id,),
            ) as cursor:
                row = await cursor.fetchone()
                return OrderState.from_dict(dict(row)) if row is not None else None

    async def _build_validation_intent(self, spec: VenueOrderSpec, payload: dict[str, Any]) -> Any:
        account_snapshot = await self._state_writer.query_latest_account_info()
        context = {
            "current_time": payload.get("current_time", _now_utc()),
        }
        for name in ("reference_price", "settlement_price", "prev_close"):
            if name in payload:
                context[name] = payload[name]
        if account_snapshot:
            context.update(
                {
                    "available_funds": account_snapshot.get("available"),
                    "account_equity": account_snapshot.get("equity"),
                }
            )
        return build_validation_intent(spec, **context)

    def _load_instrument_specs(self, instrument_config_path: str | None) -> dict[str, Any]:
        if not instrument_config_path:
            return {}
        return load_instruments_from_yaml(instrument_config_path)

    def _resolve_instrument_spec(self, symbol: str):
        if symbol in self._instrument_specs:
            return self._instrument_specs[symbol]
        return get_instrument_spec(symbol)

    async def _status_loop(self) -> None:
        while self._started:
            try:
                await self._sync_orders_once(include_pending=False)
            except Exception as exc:
                logger.error("Unhandled error in order manager status loop: %s", exc, exc_info=True)
            await asyncio.sleep(self._poll_interval)

    async def _cancel_pending_orders(self) -> int:
        pending = await self._state_writer.query_orders_by_status(OrderStatus.PENDING_SEND.value)
        count = 0
        for order in pending:
            updated = await self._update_order_record(
                order,
                status=OrderStatus.CANCELED.value,
                filled_quantity=order.filled_quantity,
                filled_price=order.filled_price,
            )
            if updated is not None:
                count += 1
                await self._publish_order_update(updated, reason="cancel_all")
        return count

    async def _sync_orders_once(self, *, include_pending: bool) -> int:
        statuses = list(_ACTIVE_POLL_STATUSES)
        if include_pending:
            statuses.append(OrderStatus.PENDING_SEND.value)

        orders: list[OrderState] = []
        for status in statuses:
            orders.extend(await self._state_writer.query_orders_by_status(status))

        changed = 0
        for order in orders:
            if self._last_published_status.get(order.order_id) != order.status.value:
                await self._publish_order_update(order)
            venue_status = await self._venue_adapter.query_order(order.client_order_id)
            mapped_status = self._map_venue_status(venue_status.status)
            if (
                mapped_status == order.status.value
                and float(venue_status.filled_quantity) == order.filled_quantity
                and float(venue_status.filled_price) == order.filled_price
            ):
                continue
            updated = await self._update_order_record(
                order,
                status=mapped_status,
                filled_quantity=float(venue_status.filled_quantity),
                filled_price=float(venue_status.filled_price),
            )
            if updated is None:
                continue
            changed += 1
            await self._publish_order_update(updated)
        return changed

    def _map_venue_status(self, venue_status: str) -> str:
        mapping = {
            "PENDING": OrderStatus.SENT.value,
            "SENT": OrderStatus.SENT.value,
            "PARTIAL": OrderStatus.PARTIALLY_FILLED.value,
            "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED.value,
            "FILLED": OrderStatus.FILLED.value,
            "CANCELED": OrderStatus.CANCELED.value,
            "REJECTED": OrderStatus.REJECTED.value,
        }
        return mapping.get(venue_status, OrderStatus.FAILED.value)

    async def _update_order_record(
        self,
        order: OrderState,
        *,
        status: str,
        filled_quantity: float,
        filled_price: float,
    ) -> Optional[OrderState]:
        updated_at = _now_utc()
        async with aiosqlite.connect(self._state_writer._db_path) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA busy_timeout=5000;")
            await db.execute(
                "UPDATE orders SET status = ?, filled_quantity = ?, filled_price = ?, updated_at = ? WHERE order_id = ?",
                (
                    status,
                    filled_quantity,
                    filled_price,
                    updated_at.isoformat(),
                    order.order_id,
                ),
            )
            await db.commit()
        return await self._state_writer.query_order(order.order_id)

    async def _publish_order_update(
        self,
        order: OrderState,
        *,
        reason: str | None = None,
        error: str | None = None,
    ) -> None:
        if self._event_bus is None:
            return
        payload = {
            "order_id": order.order_id,
            "client_order_id": order.client_order_id,
            "symbol": order.symbol,
            "venue": order.venue,
            "status": order.status.value,
            "filled_quantity": order.filled_quantity,
            "filled_price": order.filled_price,
        }
        if reason:
            payload["reason"] = reason
        if error:
            payload["error"] = error

        async with self._publish_lock:
            stream_key = f"{EventType.ORDER_UPDATE}:{order.symbol}"
            stream_seq = self._event_sequences[stream_key]
            envelope = EventEnvelope.make(
                EventType.ORDER_UPDATE,
                order.symbol,
                payload,
                stream_seq=stream_seq,
            )
            self._event_sequences[stream_key] += 1
            self._last_published_status[order.order_id] = order.status.value
        await self._event_bus.publish(envelope)
