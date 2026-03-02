"""Phase 1 故障注入测试 — 限频退避"""
import pytest


class TestRateLimitBackoff:
    def test_429_triggers_degraded(self):
        """模拟 429 → 进入 DEGRADED 状态"""
        pass

    def test_exponential_backoff(self):
        """验证指数退避: 5s, 10s, 20s, ..., max 300s"""
        pass

    def test_recovery_after_429(self):
        """429 恢复后 → DEGRADED → NORMAL + 冷却窗口"""
        pass
