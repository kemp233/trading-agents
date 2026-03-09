from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

from vnpy.trader.constant import Direction, Exchange
from vnpy.trader.object import CancelRequest, OrderData, OrderRequest, PositionData, SubscribeRequest, TradeData

from core.state_schema import OrderSide, OrderState, OrderStatus, PositionState
from core.venue_order_spec import VenueOrderSpec, VenueOrderStatus, VenuePosition, VenueReceipt
from venue.base import MarketStatus as BaseMarketStatus
from venue.ctp_gateway import CtpGatewayWrapper
from venue.ctp_utils import (
    load_instrument_exchange_map,
    order_type_to_vnpy,
    position_to_side,
    reduce_only_to_offset,
    side_to_direction,
    status_to_receipt,
)

if TYPE_CHECKING:
    from core.state_writer import StateWriter
    from vnpy.trader.object import AccountData

logger = logging.getLogger(__name__)


@dataclass
class VenueAccountInfo:
    account_id: str
    broker_id: str
    balance: Decimal
    available: Decimal
    margin: Decimal
    frozen_margin: Decimal
    frozen_cash: Decimal
    profit_loss: Decimal
    commission: Decimal
    updated_at: datetime


class CTPAdapter:
    """CTP adapter backed by the current vnpy_ctp gateway objects."""

    def __init__(
        self,
        config: dict,
        state_writer: StateWriter | None = None,
        gateway_wrapper: CtpGatewayWrapper | None = None,
        instrument_config_path: Path | None = None,
    ) -> None:
        self._config = config
        self._state_writer = state_writer
        self._gateway = gateway_wrapper or CtpGatewayWrapper(config, state_writer=state_writer)
        self._instrument_exchange = load_instrument_exchange_map(instrument_config_path)

        self._gateway.register_order_listener(self._on_order_event)
        self._gateway.register_trade_listener(self._on_trade_event)

        self._pending_futures: dict[str, asyncio.Future[VenueReceipt]] = {}
        self._submitted_orders: set[str] = set()
        self._order_status_by_client_id: dict[str, VenueOrderStatus] = {}
        self._order_data_by_client_id: dict[str, OrderData] = {}
        self._exchange_to_client_id: dict[str, str] = {}
        self._client_order_specs: dict[str, VenueOrderSpec] = {}
        self._created_at_by_client_id: dict[str, datetime] = {}

        self.submit_count = 0
        self.cancel_count = 0

    @property
    def gateway_wrapper(self) -> CtpGatewayWrapper:
        return self._gateway

    async def connect(self) -> None:
        await self._gateway.connect()

    async def disconnect(self) -> None:
        await self._gateway.disconnect()

    async def submit_order(self, spec: VenueOrderSpec) -> VenueReceipt:
        if not self._gateway.is_connected:
            raise ConnectionError("CTP gateway not connected")
        if spec.client_order_id in self._submitted_orders:
            return VenueReceipt(
                client_order_id=spec.client_order_id,
                exchange_order_id="",
                status="REJECTED",
                raw_response={"error": "Duplicate client_order_id"},
                timestamp=datetime.now(timezone.utc),
            )

        exchange = self._resolve_exchange(spec.symbol)
        request = OrderRequest(
            symbol=spec.symbol,
            exchange=exchange,
            direction=side_to_direction(spec.side),
            type=order_type_to_vnpy(spec.order_type, spec.time_in_force),
            volume=float(spec.quantity),
            price=float(spec.price or Decimal("0")),
            offset=reduce_only_to_offset(spec.reduce_only),
            reference=spec.client_order_id,
        )

        future: asyncio.Future[VenueReceipt] = asyncio.get_running_loop().create_future()
        self._pending_futures[spec.client_order_id] = future
        self._submitted_orders.add(spec.client_order_id)
        self._client_order_specs[spec.client_order_id] = spec
        self._created_at_by_client_id.setdefault(spec.client_order_id, datetime.now(timezone.utc))
        self.submit_count += 1

        order_id = self._gateway.send_order(request)
        if order_id:
            self._exchange_to_client_id[order_id] = spec.client_order_id

        try:
            return await asyncio.wait_for(future, timeout=10.0)
        except asyncio.TimeoutError as exc:
            raise TimeoutError(f"Order submission timeout: {spec.client_order_id}") from exc
        finally:
            self._pending_futures.pop(spec.client_order_id, None)

    async def cancel_order(self, client_order_id: str) -> VenueReceipt:
        if not self._gateway.is_connected:
            raise ConnectionError("CTP gateway not connected")

        order = self._order_data_by_client_id.get(client_order_id)
        if order is None:
            raise LookupError(f"Cannot cancel unknown client_order_id: {client_order_id}")

        future: asyncio.Future[VenueReceipt] = asyncio.get_running_loop().create_future()
        self._pending_futures[client_order_id] = future
        self.cancel_count += 1

        request = CancelRequest(
            orderid=order.orderid,
            symbol=order.symbol,
            exchange=order.exchange,
        )
        self._gateway.cancel_order(request)

        try:
            return await asyncio.wait_for(future, timeout=10.0)
        except asyncio.TimeoutError as exc:
            raise TimeoutError(f"Cancel order timeout: {client_order_id}") from exc
        finally:
            self._pending_futures.pop(client_order_id, None)

    async def query_order(self, client_order_id: str) -> VenueOrderStatus:
        status = self._order_status_by_client_id.get(client_order_id)
        if status is None:
            raise LookupError(
                f"Remote single-order query is not supported by vnpy_ctp; no cached status for {client_order_id}"
            )
        return status

    async def query_positions(self) -> list[VenuePosition]:
        raw_positions = await self._gateway.refresh_positions()
        positions: list[VenuePosition] = []
        state_positions: list[PositionState] = []

        for item in raw_positions:
            if float(item.volume or 0) <= 0:
                continue
            side = position_to_side(item)
            position = VenuePosition(
                symbol=item.symbol,
                venue="CTP",
                side=side,
                quantity=Decimal(str(item.volume or 0)),
                entry_price=Decimal(str(item.price or 0)),
                unrealized_pnl=Decimal(str(item.pnl or 0)),
                updated_at=datetime.now(timezone.utc),
            )
            positions.append(position)
            state_positions.append(
                PositionState(
                    symbol=item.symbol,
                    venue="CTP",
                    side=side,
                    quantity=float(item.volume or 0),
                    entry_price=float(item.price or 0),
                    unrealized_pnl=float(item.pnl or 0),
                    updated_at=datetime.now(timezone.utc),
                )
            )

        if self._state_writer is not None:
            await self._state_writer.replace_positions(state_positions, venue="CTP")

        return positions

    async def query_account(self) -> VenueAccountInfo:
        account = await self._gateway.refresh_account()
        return self._account_to_info(account)

    async def subscribe_market_data(self, symbols: list[str]) -> None:
        requests = [
            SubscribeRequest(symbol=symbol, exchange=self._resolve_exchange(symbol))
            for symbol in symbols
        ]
        self._gateway.subscribe(requests)

    async def get_market_status(self, symbol: str) -> BaseMarketStatus:
        is_connected = self._gateway.is_connected
        return BaseMarketStatus(
            symbol=symbol,
            can_market_order=is_connected,
            can_limit_order=is_connected,
            is_halted=not is_connected,
            best_bid=None,
            best_ask=None,
            updated_at=datetime.now(timezone.utc),
        )

    def _resolve_exchange(self, symbol: str) -> Exchange:
        exchange = self._instrument_exchange.get(symbol)
        if exchange is None:
            raise ValueError(f"Unknown exchange for symbol {symbol}; add it to futures/config/instruments_cn.yaml")
        return exchange

    def _on_order_event(self, order: OrderData) -> None:
        client_order_id = order.reference or self._exchange_to_client_id.get(order.orderid, "")
        if not client_order_id:
            return

        self._exchange_to_client_id[order.orderid] = client_order_id
        self._order_data_by_client_id[client_order_id] = order

        timestamp = self._as_utc(order.datetime)
        receipt = VenueReceipt(
            client_order_id=client_order_id,
            exchange_order_id=order.orderid,
            status=status_to_receipt(order.status),
            raw_response={
                "symbol": order.symbol,
                "exchange": order.exchange.value,
                "orderid": order.orderid,
                "status": order.status.value,
                "traded": order.traded,
                "volume": order.volume,
            },
            timestamp=timestamp,
        )

        self._order_status_by_client_id[client_order_id] = VenueOrderStatus(
            client_order_id=client_order_id,
            exchange_order_id=order.orderid,
            status=receipt.status,
            filled_quantity=Decimal(str(order.traded or 0)),
            filled_price=Decimal(str(order.price or 0)),
            updated_at=timestamp,
        )

        future = self._pending_futures.get(client_order_id)
        if future is not None and not future.done():
            future.set_result(receipt)

        if self._state_writer is not None:
            spec = self._client_order_specs.get(client_order_id)
            side = self._state_side(order.direction, spec.side if spec else "BUY")
            order_state = OrderState(
                order_id=order.orderid,
                client_order_id=client_order_id,
                symbol=order.symbol,
                venue="CTP",
                side=side,
                quantity=float(order.volume or 0),
                price=float(order.price) if order.price else None,
                status=self._state_status(receipt.status),
                strategy_id=None,
                created_at=self._created_at_by_client_id.setdefault(client_order_id, timestamp),
                updated_at=timestamp,
                filled_quantity=float(order.traded or 0),
                filled_price=float(order.price or 0),
            )
            self._schedule_state_write(order_state)

    def _on_trade_event(self, trade: TradeData) -> None:
        client_order_id = self._exchange_to_client_id.get(trade.orderid)
        if not client_order_id:
            return

        existing = self._order_status_by_client_id.get(client_order_id)
        if existing is None:
            return

        timestamp = self._as_utc(trade.datetime)
        self._order_status_by_client_id[client_order_id] = VenueOrderStatus(
            client_order_id=client_order_id,
            exchange_order_id=trade.orderid,
            status=existing.status,
            filled_quantity=Decimal(str(trade.volume or existing.filled_quantity)),
            filled_price=Decimal(str(trade.price or existing.filled_price)),
            updated_at=timestamp,
        )

    def _schedule_state_write(self, order_state: OrderState) -> None:
        if self._state_writer is None:
            return
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._state_writer.write_order(order_state))
        except RuntimeError:
            loop = self._gateway._loop
            if loop is not None:
                loop.call_soon_threadsafe(loop.create_task, self._state_writer.write_order(order_state))

    def _account_to_info(self, account: AccountData) -> VenueAccountInfo:
        balance = Decimal(str(account.balance or 0))
        available = Decimal(str(getattr(account, "available", 0) or 0))
        frozen = Decimal(str(account.frozen or 0))
        return VenueAccountInfo(
            account_id=account.accountid,
            broker_id=self._config.get("broker_id", getattr(self._gateway, "broker_id", "")),
            balance=balance,
            available=available,
            margin=frozen,
            frozen_margin=frozen,
            frozen_cash=Decimal("0"),
            profit_loss=Decimal("0"),
            commission=Decimal("0"),
            updated_at=datetime.now(timezone.utc),
        )

    @staticmethod
    def _state_side(direction: Direction | None, fallback: str) -> OrderSide:
        if direction == Direction.SHORT:
            return OrderSide.SELL
        if direction == Direction.LONG:
            return OrderSide.BUY
        return OrderSide(fallback)

    @staticmethod
    def _state_status(status: str) -> OrderStatus:
        mapping = {
            "SENT": OrderStatus.SENT,
            "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
            "FILLED": OrderStatus.FILLED,
            "CANCELED": OrderStatus.CANCELED,
            "REJECTED": OrderStatus.REJECTED,
        }
        return mapping.get(status, OrderStatus.FAILED)

    @staticmethod
    def _as_utc(value: datetime | None) -> datetime:
        if value is None:
            return datetime.now(timezone.utc)
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


CtpAdapter = CTPAdapter

