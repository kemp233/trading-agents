# venue/mock_adapter.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List
from uuid import uuid4

from core.venue_order_spec import VenueOrderSpec, VenueOrderStatus, VenuePosition, VenueReceipt
from venue.base import MarketStatus, VenueAdapter


class MockVenueAdapter(VenueAdapter):
    """
    Phase 1: mock venue adapter for unit tests (no real exchange IO).

    Features:
      - Controlled failure injection: should_fail / fail_after_n
      - Symbol-level rejection: reject_symbols
      - Submit/cancel/query are deterministic and fast
      - Records submit call history for assertions
    """

    def __init__(
        self,
        should_fail: bool = False,
        fail_after_n: int = 0,
        reject_symbols: List[str] | None = None,
        fail_before_n: int = 0,
    ) -> None:
        self.should_fail: bool = should_fail
        self.fail_after_n: int = fail_after_n
        self.fail_before_n: int = fail_before_n
        self.reject_symbols: List[str] = reject_symbols or []

        # metrics / history
        self.submitted_orders: List[VenueOrderSpec] = []
        self.call_count: int = 0  # counts every submit attempt (including failures)

        # internal state
        self._spec_by_client_id: Dict[str, VenueOrderSpec] = {}
        self._exchange_id_by_client_id: Dict[str, str] = {}
        self._status_by_client_id: Dict[str, str] = {}
        self.canceled_order_ids: List[str] = []

    async def submit_order(self, spec: VenueOrderSpec) -> VenueReceipt:
        await asyncio.sleep(0)
        self.call_count += 1

        # hard fail: always down
        if self.should_fail:
            raise ConnectionError("mock exchange down")

        # fail before N successful calls:
        # "前 N 次失败，第 N+1 次开始成功"
        if self.fail_before_n > 0 and self.call_count <= self.fail_before_n:
            raise ConnectionError("mock exchange down")

        # fail after N successful calls:
        # "前 N 次成功，第 N+1 次开始失败" -> count by submit attempts for determinism
        if self.fail_after_n > 0 and self.call_count > self.fail_after_n:
            raise ConnectionError("mock exchange down")

        now = datetime.now(timezone.utc)

        # reject certain symbols (no exception; returns receipt with REJECTED)
        if spec.symbol in self.reject_symbols:
            exchange_order_id = f"MOCK-REJECT-{uuid4()}"
            receipt = VenueReceipt(
                client_order_id=spec.client_order_id,
                exchange_order_id=exchange_order_id,
                status="REJECTED",
                raw_response={
                    "mock": True,
                    "action": "submit_order",
                    "reason": "symbol rejected",
                    "symbol": spec.symbol,
                },
                timestamp=now,
            )
            # treat as "successful submit call" (no exception) and record
            self.submitted_orders.append(spec)
            self._spec_by_client_id[spec.client_order_id] = spec
            self._exchange_id_by_client_id[spec.client_order_id] = exchange_order_id
            self._status_by_client_id[spec.client_order_id] = "REJECTED"
            return receipt

        # normal success
        exchange_order_id = f"MOCK-{uuid4()}"
        receipt = VenueReceipt(
            client_order_id=spec.client_order_id,
            exchange_order_id=exchange_order_id,
            status="SENT",
            raw_response={
                "mock": True,
                "action": "submit_order",
                "symbol": spec.symbol,
                "side": spec.side,
                "order_type": spec.order_type,
                "quantity": str(spec.quantity),
                "price": None if spec.price is None else str(spec.price),
            },
            timestamp=now,
        )

        self.submitted_orders.append(spec)
        self._spec_by_client_id[spec.client_order_id] = spec
        self._exchange_id_by_client_id[spec.client_order_id] = exchange_order_id
        self._status_by_client_id[spec.client_order_id] = "SENT"
        return receipt

    async def cancel_order(self, client_order_id: str) -> VenueReceipt:
        await asyncio.sleep(0)

        now = datetime.now(timezone.utc)
        exchange_order_id = self._exchange_id_by_client_id.get(client_order_id, f"MOCK-CANCEL-{uuid4()}")
        self._status_by_client_id[client_order_id] = "CANCELED"
        self.canceled_order_ids.append(client_order_id)
        return VenueReceipt(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            status="CANCELED",
            raw_response={
                "mock": True,
                "action": "cancel_order",
                "client_order_id": client_order_id,
            },
            timestamp=now,
        )

    async def query_order(self, client_order_id: str) -> VenueOrderStatus:
        await asyncio.sleep(0)

        now = datetime.now(timezone.utc)
        spec = self._spec_by_client_id.get(client_order_id)
        exchange_order_id = self._exchange_id_by_client_id.get(client_order_id, f"MOCK-QUERY-{uuid4()}")
        current_status = self._status_by_client_id.get(client_order_id, "NOT_FOUND")

        if spec is None and current_status == "NOT_FOUND":
            return VenueOrderStatus(
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id,
                status="NOT_FOUND",
                filled_quantity=Decimal("0"),
                filled_price=Decimal("0"),
                updated_at=now,
            )

        if current_status == "REJECTED":
            filled_qty = Decimal("0")
            filled_px = Decimal("0")
        elif current_status == "CANCELED":
            filled_qty = Decimal("0")
            filled_px = Decimal("0")
        else:
            # simulate immediate fill for accepted orders
            filled_qty = spec.quantity if spec is not None else Decimal("0")
            # if MARKET has None price, keep filled_price as 0 for determinism
            filled_px = (
                spec.price
                if (spec is not None and spec.price is not None)
                else Decimal("0")
            )
            current_status = "FILLED"
            self._status_by_client_id[client_order_id] = current_status

        return VenueOrderStatus(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            status=current_status,
            filled_quantity=filled_qty,
            filled_price=filled_px,
            updated_at=now,
        )

    async def query_positions(self) -> list[VenuePosition]:
        await asyncio.sleep(0)
        return []

    async def get_market_status(self, symbol: str) -> MarketStatus:
        await asyncio.sleep(0)

        now = datetime.now(timezone.utc)
        return MarketStatus(
            symbol=symbol,
            can_market_order=True,
            can_limit_order=True,
            is_halted=False,
            best_bid=None,
            best_ask=None,
            updated_at=now,
        )
