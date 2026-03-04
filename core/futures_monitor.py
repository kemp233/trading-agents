from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

import yaml

from .state_schema import MonitorSnapshot

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = Path(__file__).parent.parent / "config" / "risk_params.yaml"


def _load_futures_monitor_cfg(config_path: Path) -> dict:
    with config_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("futures_monitor", {})


class FuturesMonitor:
    """报单/撤单/成交/重复报单计数，阈值预警（测试指标 #4、5、6）。

    使用方式::

        monitor = FuturesMonitor(
            on_warning_callback=my_warn,
            on_breach_callback=my_breach,
        )
        monitor.record_order("order-001")
    """

    def __init__(
        self,
        on_warning_callback: Optional[Callable[[str, int, int], None]] = None,
        on_breach_callback: Optional[Callable[[str, int, int], None]] = None,
        config_path: Optional[Path] = None,
    ) -> None:
        self._on_warning = on_warning_callback
        self._on_breach = on_breach_callback

        cfg = _load_futures_monitor_cfg(config_path or _DEFAULT_CONFIG_PATH)
        self._max_orders: int = int(cfg.get("max_orders_per_day", 1000))
        self._max_cancels: int = int(cfg.get("max_cancels_per_day", 500))
        self._max_duplicates: int = int(cfg.get("max_duplicate_orders", 10))
        self._max_lots: int = int(cfg.get("max_lots_per_order", 100))
        self._warning_pct: float = float(cfg.get("warning_pct", 0.8))

        self.order_count: int = 0
        self.cancel_count: int = 0
        self.fill_count: int = 0
        self.duplicate_count: int = 0
        self._seen_order_ids: set[str] = set()

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def record_order(self, order_id: str) -> None:
        """记录一笔报单；若 order_id 已存在则同时计入重复报单。"""
        self.order_count += 1
        if order_id in self._seen_order_ids:
            self.duplicate_count += 1
            logger.warning(
                "Duplicate order detected: %s (duplicate_count=%d)",
                order_id,
                self.duplicate_count,
            )
        else:
            self._seen_order_ids.add(order_id)
        self._check_thresholds()

    def record_cancel(self, order_id: str) -> None:
        """记录一笔撤单。"""
        self.cancel_count += 1
        self._check_thresholds()

    def record_fill(self, order_id: str) -> None:
        """记录一笔成交。"""
        self.fill_count += 1
        self._check_thresholds()

    # ------------------------------------------------------------------
    # Reset / Snapshot
    # ------------------------------------------------------------------

    def reset(self) -> None:
        """每日开盘前重置所有计数。"""
        self.order_count = 0
        self.cancel_count = 0
        self.fill_count = 0
        self.duplicate_count = 0
        self._seen_order_ids.clear()

    def snapshot(self) -> MonitorSnapshot:
        """返回当前计数的不可变快照。"""
        return MonitorSnapshot(
            order_count=self.order_count,
            cancel_count=self.cancel_count,
            fill_count=self.fill_count,
            duplicate_count=self.duplicate_count,
            ts=datetime.now(timezone.utc),
        )

    # ------------------------------------------------------------------
    # Threshold check
    # ------------------------------------------------------------------

    def _check_thresholds(self) -> None:
        checks = [
            ("order_count", self.order_count, self._max_orders),
            ("cancel_count", self.cancel_count, self._max_cancels),
            ("duplicate_count", self.duplicate_count, self._max_duplicates),
        ]
        for field, current, limit in checks:
            if limit <= 0:
                continue
            if current >= limit:
                logger.error(
                    "BREACH threshold: %s=%d >= limit=%d", field, current, limit
                )
                if self._on_breach:
                    self._on_breach(field, current, limit)
            elif current >= int(limit * self._warning_pct):
                logger.warning(
                    "WARNING threshold: %s=%d >= %.0f%% of limit=%d",
                    field,
                    current,
                    self._warning_pct * 100,
                    limit,
                )
                if self._on_warning:
                    self._on_warning(field, current, limit)
