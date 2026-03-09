from __future__ import annotations

import asyncio
import logging
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

import yaml

from core.state_schema import RiskState, RiskStateType
from core.venue_order_spec import VenueOrderSpec

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "risk_params.yaml"
_STATE_ALIAS = {"CIRCUIT_BREAKER": RiskStateType.CIRCUIT.value}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _load_risk_config(config: dict[str, Any] | None) -> dict[str, Any]:
    file_cfg: dict[str, Any] = {}
    if _DEFAULT_CONFIG_PATH.exists():
        with _DEFAULT_CONFIG_PATH.open("r", encoding="utf-8") as fh:
            loaded = yaml.safe_load(fh) or {}
            if isinstance(loaded, dict):
                file_cfg = loaded
    if not config:
        return file_cfg
    return _deep_merge(file_cfg, config)


def _to_decimal(value: Decimal | float | int | str) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


class RecoveryPolicy:
    def __init__(self, config: dict[str, Any]) -> None:
        self.consecutive_failures = 0
        self.last_failure_time: Optional[datetime] = None
        self.last_recovery_time: Optional[datetime] = None
        self._degraded_until: Optional[datetime] = None
        self._cooldown = int(config.get("cooldown_after_recovery_sec", 60))
        self._backoff_base = int(config.get("exponential_backoff_base_sec", 5))
        self._max_backoff = int(config.get("max_backoff_sec", 300))
        self.post_recovery_scale = float(config.get("post_recovery_scale", 0.5))
        self.post_recovery_duration_sec = int(config.get("post_recovery_duration_sec", 3600))

    def on_failure(self, now: Optional[datetime] = None) -> None:
        self.consecutive_failures += 1
        self.last_failure_time = now or _now_utc()

    def get_backoff_seconds(self) -> int:
        failures = max(self.consecutive_failures, 1)
        return min(self._backoff_base * (2 ** (failures - 1)), self._max_backoff)

    def can_begin_recovery(self, now: Optional[datetime] = None) -> bool:
        if self.last_failure_time is None:
            return True
        current = now or _now_utc()
        elapsed = (current - self.last_failure_time).total_seconds()
        return elapsed >= self._cooldown

    def begin_recovery(self, now: Optional[datetime] = None) -> None:
        current = now or _now_utc()
        self.last_recovery_time = current
        self._degraded_until = current + timedelta(seconds=self.post_recovery_duration_sec)

    def can_finish_recovery(self, now: Optional[datetime] = None) -> bool:
        if self._degraded_until is None:
            return True
        current = now or _now_utc()
        return current >= self._degraded_until

    def complete_recovery(self) -> None:
        self.consecutive_failures = 0
        self.last_failure_time = None
        self._degraded_until = None

    @property
    def degraded_until(self) -> Optional[datetime]:
        return self._degraded_until


class RiskGovernor:
    STATE_PRIORITY = {
        RiskStateType.NORMAL.value: 0,
        RiskStateType.DEGRADED.value: 1,
        RiskStateType.OFFLINE.value: 2,
        RiskStateType.CIRCUIT.value: 3,
        RiskStateType.RECONCILING.value: 4,
        RiskStateType.VENUE_HALT.value: 5,
    }

    _RECOVERY_PATHS = {
        RiskStateType.CIRCUIT.value: RiskStateType.DEGRADED.value,
        RiskStateType.OFFLINE.value: RiskStateType.DEGRADED.value,
        RiskStateType.RECONCILING.value: RiskStateType.DEGRADED.value,
        RiskStateType.DEGRADED.value: RiskStateType.NORMAL.value,
        RiskStateType.VENUE_HALT.value: RiskStateType.NORMAL.value,
    }

    def __init__(self, config: dict[str, Any], event_bus, state_writer) -> None:
        self._config = _load_risk_config(config)
        self._event_bus = event_bus
        self._state_writer = state_writer
        self._state = RiskStateType.NORMAL.value
        self._state_history: list[dict[str, Any]] = []
        self._halt_reason: Optional[str] = None
        self._halted_at: Optional[datetime] = None
        self._recovery_policy = RecoveryPolicy(self._config.get("recovery", {}))
        self._pending_risk_states: list[RiskState] = []
        self._persist_tasks: set[asyncio.Task] = set()
        self._started = False

        self._flash_cfg = self._config.get("flash_crash", {})
        self._reconcile_cfg = self._config.get("reconciliation", {})
        self._circuit_cfg = self._config.get("circuit_breaker", {})

    @property
    def state(self) -> str:
        return self._state

    @property
    def state_history(self) -> list[dict[str, Any]]:
        return list(self._state_history)

    @property
    def halt_reason(self) -> Optional[str]:
        return self._halt_reason

    @property
    def halted_at(self) -> Optional[datetime]:
        return self._halted_at

    def _normalize_state(self, state: str | RiskStateType) -> str:
        raw = state.value if isinstance(state, RiskStateType) else str(state)
        normalized = _STATE_ALIAS.get(raw, raw)
        if normalized not in self.STATE_PRIORITY:
            raise ValueError(f"Unknown risk state: {state}")
        return normalized

    def _is_valid_recovery(self, current: str, target: str) -> bool:
        return self._RECOVERY_PATHS.get(current) == target

    def _build_risk_state(self, previous_state: str, current_state: str, reason: str, metadata: dict[str, Any], changed_at: datetime) -> RiskState:
        return RiskState(
            current_state=RiskStateType(current_state),
            previous_state=RiskStateType(previous_state),
            state_changed_at=changed_at,
            reason=reason,
            metadata=metadata,
        )

    def _schedule_risk_state_write(self, risk_state: RiskState) -> None:
        if self._state_writer is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            self._pending_risk_states.append(risk_state)
            return

        task = loop.create_task(self._state_writer.write_risk_state(risk_state))
        self._persist_tasks.add(task)
        task.add_done_callback(self._persist_tasks.discard)

    def _transition_to(
        self,
        new_state: str | RiskStateType,
        reason: str,
        metadata: Optional[dict[str, Any]] = None,
        *,
        force: bool = False,
    ) -> bool:
        target = self._normalize_state(new_state)
        if target == self._state:
            return False

        current = self._state
        current_priority = self.STATE_PRIORITY[current]
        target_priority = self.STATE_PRIORITY[target]
        now = _now_utc()
        meta = {
            "backoff_seconds": self._recovery_policy.get_backoff_seconds(),
            "post_recovery_scale": self._recovery_policy.post_recovery_scale,
        }
        if metadata:
            meta.update(metadata)

        if not force and target_priority < current_priority:
            if not self._is_valid_recovery(current, target):
                logger.info("Ignoring invalid recovery path %s -> %s", current, target)
                return False
            if target == RiskStateType.DEGRADED.value:
                if not self._recovery_policy.can_begin_recovery(now):
                    logger.info("Recovery cooldown not met for %s -> %s", current, target)
                    return False
                self._recovery_policy.begin_recovery(now)
                meta["degraded_until"] = self._recovery_policy.degraded_until.isoformat() if self._recovery_policy.degraded_until else None
            elif target == RiskStateType.NORMAL.value:
                if current == RiskStateType.VENUE_HALT.value:
                    pass
                elif not self._recovery_policy.can_finish_recovery(now):
                    logger.info("Post-recovery window still active for %s -> %s", current, target)
                    return False
                else:
                    self._recovery_policy.complete_recovery()
        elif target_priority > current_priority and target in {
            RiskStateType.CIRCUIT.value,
            RiskStateType.OFFLINE.value,
            RiskStateType.RECONCILING.value,
        }:
            self._recovery_policy.on_failure(now)
            meta["backoff_seconds"] = self._recovery_policy.get_backoff_seconds()

        self._state = target
        if target == RiskStateType.VENUE_HALT.value:
            self._halt_reason = reason
            self._halted_at = now
        elif current == RiskStateType.VENUE_HALT.value:
            self._halt_reason = None
            self._halted_at = None

        record = {
            "from": current,
            "to": target,
            "reason": reason,
            "timestamp": now.isoformat(),
            "metadata": deepcopy(meta),
        }
        self._state_history.append(record)
        self._schedule_risk_state_write(
            self._build_risk_state(current, target, reason, record["metadata"], now)
        )
        logger.warning("Risk state: %s -> %s (%s)", current, target, reason)
        return True

    def transition(self, new_state: str, reason: str, metadata: Optional[dict[str, Any]] = None) -> bool:
        return self._transition_to(new_state, reason, metadata)

    def halt(self, reason: str) -> None:
        self._transition_to(RiskStateType.VENUE_HALT.value, reason, {"trigger": "manual_halt"}, force=True)

    def resume(self) -> None:
        if self._state != RiskStateType.VENUE_HALT.value:
            logger.info("resume() ignored because current state is %s", self._state)
            return
        self._transition_to(RiskStateType.NORMAL.value, "manual_resume", {"trigger": "manual_resume"}, force=True)

    def set_degraded(self, reason: str) -> None:
        if self.STATE_PRIORITY[self._state] > self.STATE_PRIORITY[RiskStateType.DEGRADED.value]:
            logger.info("set_degraded ignored because current state %s has higher priority", self._state)
            return
        self._transition_to(RiskStateType.DEGRADED.value, reason, {"trigger": "manual_degrade"})

    def recover_degraded(self) -> None:
        if self._state != RiskStateType.DEGRADED.value:
            logger.info("recover_degraded() ignored because current state is %s", self._state)
            return
        self._transition_to(RiskStateType.NORMAL.value, "manual_recover", {"trigger": "manual_recover"})

    def resolve_conflict(self, triggered_states: list[str]) -> str:
        if not triggered_states:
            raise ValueError("triggered_states must not be empty")
        normalized = [self._normalize_state(state) for state in triggered_states]
        return max(normalized, key=lambda state: self.STATE_PRIORITY[state])

    def check_order_allowed(self) -> bool:
        return self._state != RiskStateType.VENUE_HALT.value

    def can_open_new_position(self) -> bool:
        return self._state == RiskStateType.NORMAL.value

    def can_trade(self) -> bool:
        return self._state in {RiskStateType.NORMAL.value, RiskStateType.DEGRADED.value}

    def evaluate_market_risk(
        self,
        *,
        last_price: Decimal | float | int | str,
        reference_price: Decimal | float | int | str,
        atr: Decimal | float | int | str,
        return_1s: Decimal | float | int | str,
        return_5s: Decimal | float | int | str,
    ) -> str:
        last_px = _to_decimal(last_price)
        ref_px = _to_decimal(reference_price)
        atr_value = _to_decimal(atr)
        ret_1s = abs(_to_decimal(return_1s))
        ret_5s = abs(_to_decimal(return_5s))
        atr_multiplier = _to_decimal(self._flash_cfg.get("atr_multiplier", 3))
        return_1s_threshold = _to_decimal(self._flash_cfg.get("abs_return_1s_threshold", "0.03"))
        return_5s_threshold = _to_decimal(self._flash_cfg.get("abs_return_5s_threshold", "0.05"))

        atr_trigger = atr_value > 0 and abs(last_px - ref_px) >= atr_value * atr_multiplier
        if atr_trigger or ret_1s >= return_1s_threshold or ret_5s >= return_5s_threshold:
            self._transition_to(
                RiskStateType.CIRCUIT.value,
                "market_risk_triggered",
                {
                    "last_price": str(last_px),
                    "reference_price": str(ref_px),
                    "atr": str(atr_value),
                    "return_1s": str(ret_1s),
                    "return_5s": str(ret_5s),
                    "atr_trigger": atr_trigger,
                },
            )
        return self._state

    def evaluate_reconciliation_risk(
        self,
        *,
        drift_pct: Decimal | float | int | str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> str:
        drift = abs(_to_decimal(drift_pct))
        warn_threshold = _to_decimal(self._reconcile_cfg.get("drift_threshold_pct", "0.01"))
        halt_threshold = _to_decimal(self._reconcile_cfg.get("max_drift_before_halt", "0.05"))
        transition_metadata = {"drift_pct": str(drift)}
        if metadata:
            transition_metadata.update(metadata)

        if drift >= halt_threshold:
            self._transition_to(
                RiskStateType.RECONCILING.value,
                "reconciliation_drift_breach",
                transition_metadata,
            )
        elif drift >= warn_threshold:
            self._transition_to(
                RiskStateType.DEGRADED.value,
                "reconciliation_drift_warning",
                transition_metadata,
            )
        return self._state

    def evaluate_connection_risk(
        self,
        *,
        gateway_connected: bool,
        health_ok: bool,
        consecutive_failures: int,
    ) -> str:
        if not gateway_connected or not health_ok:
            self._transition_to(
                RiskStateType.OFFLINE.value,
                "connection_unhealthy",
                {
                    "gateway_connected": gateway_connected,
                    "health_ok": health_ok,
                    "consecutive_failures": consecutive_failures,
                },
            )
        return self._state

    async def cancel_all_orders(self, adapter) -> None:
        logger.warning("cancel_all_orders triggered in state: %s", self._state)
        all_orders: list[Any] = []
        if self._state_writer is not None:
            try:
                pending = await self._state_writer.query_orders_by_status("PENDING_SEND")
                sent = await self._state_writer.query_orders_by_status("SENT")
                all_orders = pending + sent
            except Exception as exc:
                logger.error("cancel_all_orders: failed to query orders: %s", exc)
                return

        if not all_orders:
            logger.info("cancel_all_orders: no active orders found")
            return

        for order in all_orders:
            try:
                await adapter.cancel_order(order.client_order_id)
            except Exception as exc:
                logger.error("cancel_all_orders: failed to cancel %s: %s", order.client_order_id, exc)

    async def _query_open_positions(self, adapter) -> list[Any]:
        positions = await adapter.query_positions()
        return [position for position in positions if _to_decimal(position.quantity) > 0]

    def _build_reduce_only_spec(self, position: Any, quantity: Decimal, phase: str, batch_index: int) -> VenueOrderSpec:
        side = "SELL" if position.side == "LONG" else "BUY"
        return VenueOrderSpec(
            symbol=position.symbol,
            side=side,
            order_type="MARKET",
            quantity=quantity,
            price=None,
            reduce_only=True,
            client_order_id=f"risk-{phase}-{batch_index}-{uuid4().hex[:8]}",
            venue=position.venue,
        )

    async def attempt_flatten(self, adapter, timeout_sec: Optional[float] = None) -> bool:
        timeout = float(timeout_sec or self._circuit_cfg.get("attempt_flatten_timeout_sec", 10))

        async def _run() -> bool:
            await self.cancel_all_orders(adapter)
            positions = await self._query_open_positions(adapter)
            if not positions:
                self._transition_to(
                    RiskStateType.RECONCILING.value,
                    "attempt_flatten_success",
                    {"positions_closed": 0, "phase": "attempt_flatten"},
                )
                return True
            for index, position in enumerate(positions, start=1):
                quantity = _to_decimal(position.quantity)
                await adapter.submit_order(
                    self._build_reduce_only_spec(position, quantity, "attempt", index)
                )
            remaining = await self._query_open_positions(adapter)
            if remaining:
                return False
            self._transition_to(
                RiskStateType.RECONCILING.value,
                "attempt_flatten_success",
                {"positions_closed": len(positions), "phase": "attempt_flatten"},
            )
            return True

        try:
            return await asyncio.wait_for(_run(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("attempt_flatten timed out after %ss", timeout)
            return False

    async def staged_exit(
        self,
        adapter,
        batch_pct: Optional[float] = None,
        interval_sec: Optional[float] = None,
    ) -> bool:
        pct = _to_decimal(batch_pct if batch_pct is not None else self._circuit_cfg.get("staged_exit_batch_pct", 0.25))
        sleep_sec = float(interval_sec if interval_sec is not None else self._circuit_cfg.get("staged_exit_interval_sec", 2))
        if pct <= 0:
            raise ValueError("batch_pct must be > 0")

        batch_count = 0
        while True:
            positions = await self._query_open_positions(adapter)
            if not positions:
                self._transition_to(
                    RiskStateType.RECONCILING.value,
                    "staged_exit_complete",
                    {"batches": batch_count, "phase": "staged_exit"},
                )
                return True

            batch_count += 1
            if batch_count > 100:
                raise RuntimeError("staged_exit exceeded 100 batches without flattening positions")

            for position in positions:
                full_qty = _to_decimal(position.quantity)
                exit_qty = full_qty if pct >= 1 else full_qty * pct
                if exit_qty <= 0:
                    exit_qty = full_qty
                await adapter.submit_order(
                    self._build_reduce_only_spec(position, exit_qty, "staged", batch_count)
                )

            if sleep_sec > 0:
                await asyncio.sleep(sleep_sec)

    async def flush_pending_state_writes(self) -> None:
        if self._state_writer is not None:
            while self._pending_risk_states:
                risk_state = self._pending_risk_states.pop(0)
                await self._state_writer.write_risk_state(risk_state)
        if self._persist_tasks:
            tasks = list(self._persist_tasks)
            await asyncio.gather(*tasks)

    async def start(self) -> None:
        self._started = True
        await self.flush_pending_state_writes()

    async def stop(self) -> None:
        await self.flush_pending_state_writes()
        self._started = False

    async def health_check(self):
        from types import SimpleNamespace

        return SimpleNamespace(ok=True, reason="", state=self._state)

