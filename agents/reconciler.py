from __future__ import annotations

import asyncio
import logging
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

import yaml

from core.state_schema import PositionState, RiskStateType
from core.venue_order_spec import VenuePosition

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "risk_params.yaml"


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


def _to_decimal(value: Decimal | float | int | str) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


class Reconciler:
    def __init__(self, state_writer, venue_adapter, risk_governor, config: dict[str, Any] | None = None) -> None:
        self._config = _load_config(config)
        self._state_writer = state_writer
        self._venue_adapter = venue_adapter
        self._risk_governor = risk_governor
        self._reconcile_cfg = self._config.get("reconciliation", {})
        self._check_interval = float(self._reconcile_cfg.get("check_interval_sec", 30))
        self._warn_threshold = _to_decimal(self._reconcile_cfg.get("drift_threshold_pct", "0.01"))
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._last_result: Optional[dict[str, Any]] = None
        self._last_success_at: Optional[datetime] = None
        self._reconciliation_active = False

    @property
    def last_result(self) -> Optional[dict[str, Any]]:
        return deepcopy(self._last_result)

    @property
    def last_success_at(self) -> Optional[datetime]:
        return self._last_success_at

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run_loop(self) -> None:
        while self._running:
            try:
                await self.run_once()
            except Exception as exc:
                logger.error("Unhandled error in reconciler loop: %s", exc, exc_info=True)
            await asyncio.sleep(self._check_interval)

    async def run_once(self) -> dict[str, Any]:
        checked_at = _now_utc()
        local_positions = await self._state_writer.query_positions()
        venue_positions = await self._venue_adapter.query_positions()

        local_map = self._normalize_local_positions(local_positions)
        venue_map = self._normalize_venue_positions(venue_positions)
        drift_pct = self._calculate_drift(local_map, venue_map)
        mismatches = self._build_mismatches(local_map, venue_map)
        is_balanced = drift_pct < self._warn_threshold

        result = {
            "checked_at": checked_at.isoformat(),
            "drift_pct": float(drift_pct),
            "local_position_count": len(local_map),
            "venue_position_count": len(venue_map),
            "mismatches": mismatches,
            "is_balanced": is_balanced,
        }
        self._last_result = result

        metadata = {
            "source": "reconciler",
            "checked_at": result["checked_at"],
            "drift_pct": str(drift_pct),
            "local_position_count": result["local_position_count"],
            "venue_position_count": result["venue_position_count"],
            "mismatches": deepcopy(mismatches),
            "is_balanced": is_balanced,
        }

        if is_balanced:
            self._last_success_at = checked_at
            await self._attempt_recovery(metadata)
            if self._risk_governor.state == RiskStateType.NORMAL.value:
                self._reconciliation_active = False
        else:
            self._reconciliation_active = True
            self._risk_governor.evaluate_reconciliation_risk(drift_pct=drift_pct, metadata=metadata)

        return deepcopy(result)

    def _normalize_local_positions(self, positions: list[PositionState]) -> dict[tuple[str, str, str], Decimal]:
        normalized: dict[tuple[str, str, str], Decimal] = {}
        for position in positions:
            quantity = _to_decimal(position.quantity)
            if quantity == 0:
                continue
            key = (position.symbol, position.venue, position.side)
            normalized[key] = normalized.get(key, Decimal("0")) + quantity
        return normalized

    def _normalize_venue_positions(self, positions: list[VenuePosition]) -> dict[tuple[str, str, str], Decimal]:
        normalized: dict[tuple[str, str, str], Decimal] = {}
        for position in positions:
            quantity = _to_decimal(position.quantity)
            if quantity == 0:
                continue
            key = (position.symbol, position.venue, position.side)
            normalized[key] = normalized.get(key, Decimal("0")) + quantity
        return normalized

    def _calculate_drift(
        self,
        local_map: dict[tuple[str, str, str], Decimal],
        venue_map: dict[tuple[str, str, str], Decimal],
    ) -> Decimal:
        keys = set(local_map) | set(venue_map)
        delta_total = sum(abs(local_map.get(key, Decimal("0")) - venue_map.get(key, Decimal("0"))) for key in keys)
        venue_total = sum(abs(quantity) for quantity in venue_map.values())
        if venue_total == 0:
            return Decimal("0") if delta_total == 0 else Decimal("1")
        return delta_total / venue_total

    def _build_mismatches(
        self,
        local_map: dict[tuple[str, str, str], Decimal],
        venue_map: dict[tuple[str, str, str], Decimal],
    ) -> list[dict[str, Any]]:
        mismatches: list[dict[str, Any]] = []
        for key in sorted(set(local_map) | set(venue_map)):
            local_qty = local_map.get(key, Decimal("0"))
            venue_qty = venue_map.get(key, Decimal("0"))
            delta_qty = local_qty - venue_qty
            if delta_qty == 0:
                continue
            symbol, venue, side = key
            mismatches.append(
                {
                    "symbol": symbol,
                    "venue": venue,
                    "side": side,
                    "local_qty": str(local_qty),
                    "venue_qty": str(venue_qty),
                    "delta_qty": str(delta_qty),
                }
            )
        return mismatches

    async def _attempt_recovery(self, metadata: dict[str, Any]) -> None:
        if not self._reconciliation_active:
            return

        if self._risk_governor.state == RiskStateType.RECONCILING.value:
            changed = self._risk_governor.transition(
                RiskStateType.DEGRADED.value,
                "reconciliation_balanced",
                metadata,
            )
            if changed:
                self._risk_governor.transition(
                    RiskStateType.NORMAL.value,
                    "reconciliation_balanced",
                    metadata,
                )
        elif self._risk_governor.state == RiskStateType.DEGRADED.value:
            self._risk_governor.transition(
                RiskStateType.NORMAL.value,
                "reconciliation_balanced",
                metadata,
            )
