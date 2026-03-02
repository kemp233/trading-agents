"""Risk Governor — 六状态机 + 优先级 + 分阶段平仓 (v3)"""
import asyncio
import logging
from datetime import datetime
from decimal import Decimal

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

    def __init__(self, config: dict, event_bus, state_writer):
        self._config = config
        self._event_bus = event_bus
        self._state_writer = state_writer
        self._state = 'NORMAL'
        self._state_history = []
        self._recovery_policy = RecoveryPolicy(config.get('recovery', {}))

    @property
    def state(self) -> str:
        return self._state

    def transition(self, new_state: str, reason: str):
        if new_state == self._state:
            return
        old_state = self._state
        # 优先级检查: 只允许升级或由高优先级主动降级
        if (self.STATE_PRIORITY.get(new_state, 0) < 
            self.STATE_PRIORITY.get(self._state, 0)):
            # 降级需要满足恢复条件
            if not self._recovery_policy.can_recover():
                logger.warning(
                    f"Cannot transition {self._state} → {new_state}: "
                    f"recovery conditions not met"
                )
                return

        self._state = new_state
        self._state_history.append({
            'from': old_state,
            'to': new_state,
            'reason': reason,
            'timestamp': datetime.utcnow().isoformat(),
        })
        logger.warning(f"Risk state: {old_state} → {new_state} ({reason})")

    def resolve_conflict(self, triggered_states: list[str]) -> str:
        """多状态同时触发时, 取最高优先级"""
        return max(triggered_states, 
                   key=lambda s: self.STATE_PRIORITY.get(s, 0))

    def can_open_new_position(self) -> bool:
        return self._state == 'NORMAL'

    def can_trade(self) -> bool:
        return self._state in ('NORMAL', 'DEGRADED')

    async def start(self):
        pass

    async def stop(self):
        pass

    async def health_check(self):
        from types import SimpleNamespace
        return SimpleNamespace(ok=True, reason='')


class RecoveryPolicy:
    def __init__(self, config: dict):
        self.consecutive_failures = 0
        self.last_recovery_time = None
        self._cooldown = config.get('cooldown_after_recovery_sec', 60)
        self._backoff_base = config.get('exponential_backoff_base_sec', 5)
        self._max_backoff = config.get('max_backoff_sec', 300)

    def on_failure(self):
        self.consecutive_failures += 1

    def get_backoff_seconds(self) -> int:
        return min(
            self._backoff_base * (2 ** self.consecutive_failures),
            self._max_backoff
        )

    def can_recover(self) -> bool:
        if self.last_recovery_time is None:
            return True
        elapsed = (datetime.utcnow() - self.last_recovery_time).total_seconds()
        return elapsed > self._cooldown

    def on_recovery(self):
        self.last_recovery_time = datetime.utcnow()
        self.consecutive_failures = 0
