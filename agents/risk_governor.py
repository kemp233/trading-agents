"""Risk Governor — 六状态机 + 优先级 + 分阶段平仓 (v3)"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

logger = logging.getLogger(__name__)


class RiskGovernor:
    """风控状态机

    状态优先级 (高→低):
    P4: VENUE_HALT
    P3: RECONCILING
    P2: CIRCUIT_BREAKER
    P1: DEGRADED
    P0: NORMAL
    """

    STATE_PRIORITY = {
        'NORMAL': 0,
        'DEGRADED': 1,
        'CIRCUIT_BREAKER': 2,
        'RECONCILING': 3,
        'VENUE_HALT': 4,
    }

    def __init__(self, config: dict, event_bus, state_writer) -> None:
        self._config = config
        self._event_bus = event_bus
        self._state_writer = state_writer
        self._state = 'NORMAL'
        self._state_history: list[dict] = []
        self._recovery_policy = RecoveryPolicy(config.get('recovery', {}))
        self._halt_reason: Optional[str] = None
        self._halted_at: Optional[datetime] = None

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def state(self) -> str:
        return self._state

    @property
    def halt_reason(self) -> Optional[str]:
        return self._halt_reason

    @property
    def halted_at(self) -> Optional[datetime]:
        return self._halted_at

    # ------------------------------------------------------------------
    # Legacy transition API (preserved for existing tests)
    # ------------------------------------------------------------------

    def transition(self, new_state: str, reason: str) -> None:
        if new_state == self._state:
            return
        old_state = self._state
        # 优先级检查: 只允许升级或由高优先级主动降级
        if (
            self.STATE_PRIORITY.get(new_state, 0)
            < self.STATE_PRIORITY.get(self._state, 0)
        ):
            # 降级需要满足恢复条件
            if not self._recovery_policy.can_recover():
                logger.warning(
                    "Cannot transition %s → %s: recovery conditions not met",
                    self._state,
                    new_state,
                )
                return

        self._state = new_state
        self._state_history.append({
            'from': old_state,
            'to': new_state,
            'reason': reason,
            'timestamp': datetime.now(timezone.utc).isoformat(),
        })
        logger.warning("Risk state: %s → %s (%s)", old_state, new_state, reason)

    # ------------------------------------------------------------------
    # Issue #14: halt / resume / degraded API
    # ------------------------------------------------------------------

    def halt(self, reason: str) -> None:
        """强制切换到 VENUE_HALT（最高优先级），拒绝所有新单。"""
        old_state = self._state
        self._halt_reason = reason
        self._halted_at = datetime.now(timezone.utc)
        self._state = 'VENUE_HALT'
        self._state_history.append({
            'from': old_state,
            'to': 'VENUE_HALT',
            'reason': reason,
            'timestamp': self._halted_at.isoformat(),
        })
        logger.warning("Risk HALTED (from %s): %s", old_state, reason)

    def resume(self) -> None:
        """从 VENUE_HALT 恢复到 NORMAL（仅当当前状态为 VENUE_HALT 时生效）。"""
        if self._state != 'VENUE_HALT':
            logger.info(
                "resume() called but current state is %s — no-op", self._state
            )
            return
        ts = datetime.now(timezone.utc).isoformat()
        self._state = 'NORMAL'
        self._halt_reason = None
        self._halted_at = None
        self._state_history.append({
            'from': 'VENUE_HALT',
            'to': 'NORMAL',
            'reason': 'manual_resume',
            'timestamp': ts,
        })
        logger.warning("Risk state: VENUE_HALT → NORMAL (manual_resume)")

    def set_degraded(self, reason: str) -> None:
        """切换到 DEGRADED；若当前优先级更高则静默忽略。"""
        if self.STATE_PRIORITY.get(self._state, 0) > self.STATE_PRIORITY['DEGRADED']:
            logger.info(
                "set_degraded ignored: current state %s has higher priority",
                self._state,
            )
            return
        old_state = self._state
        self._state = 'DEGRADED'
        self._state_history.append({
            'from': old_state,
            'to': 'DEGRADED',
            'reason': reason,
            'timestamp': datetime.now(timezone.utc).isoformat(),
        })
        logger.warning("Risk state: %s → DEGRADED (%s)", old_state, reason)

    def recover_degraded(self) -> None:
        """从 DEGRADED 恢复到 NORMAL。"""
        if self._state != 'DEGRADED':
            logger.info(
                "recover_degraded() called but current state is %s — no-op",
                self._state,
            )
            return
        self._state = 'NORMAL'
        self._state_history.append({
            'from': 'DEGRADED',
            'to': 'NORMAL',
            'reason': 'manual_recover',
            'timestamp': datetime.now(timezone.utc).isoformat(),
        })
        logger.warning("Risk state: DEGRADED → NORMAL (manual_recover)")

    def check_order_allowed(self) -> bool:
        """VENUE_HALT 状态下拒绝新单，其余状态允许。"""
        return self._state != 'VENUE_HALT'

    async def cancel_all_orders(self, adapter) -> None:
        """撤销所有未成交委托；单笔失败记录 error 后继续执行。"""
        logger.warning("cancel_all_orders triggered in state: %s", self._state)

        all_orders: list = []
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
                logger.info("Cancelled order: %s", order.client_order_id)
            except Exception as exc:
                logger.error(
                    "cancel_all_orders: failed to cancel %s: %s",
                    order.client_order_id,
                    exc,
                )

    # ------------------------------------------------------------------
    # Conflict resolution & guards
    # ------------------------------------------------------------------

    def resolve_conflict(self, triggered_states: list[str]) -> str:
        """多状态同时触发时，取最高优先级。"""
        return max(triggered_states, key=lambda s: self.STATE_PRIORITY.get(s, 0))

    def can_open_new_position(self) -> bool:
        return self._state == 'NORMAL'

    def can_trade(self) -> bool:
        return self._state in ('NORMAL', 'DEGRADED')

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def health_check(self):
        from types import SimpleNamespace
        return SimpleNamespace(ok=True, reason='')


class RecoveryPolicy:
    def __init__(self, config: dict) -> None:
        self.consecutive_failures = 0
        self.last_recovery_time: Optional[datetime] = None
        self._cooldown = config.get('cooldown_after_recovery_sec', 60)
        self._backoff_base = config.get('exponential_backoff_base_sec', 5)
        self._max_backoff = config.get('max_backoff_sec', 300)

    def on_failure(self) -> None:
        self.consecutive_failures += 1

    def get_backoff_seconds(self) -> int:
        return min(
            self._backoff_base * (2 ** self.consecutive_failures),
            self._max_backoff,
        )

    def can_recover(self) -> bool:
        if self.last_recovery_time is None:
            return True
        elapsed = (datetime.utcnow() - self.last_recovery_time).total_seconds()
        return elapsed > self._cooldown

    def on_recovery(self) -> None:
        self.last_recovery_time = datetime.utcnow()
        self.consecutive_failures = 0
