"""Phase 1 故障注入测试 — Outbox 原子性"""
import pytest


class TestOutboxAtomicity:
    def test_duplicate_trade_intent(self):
        """重复投递同一个 TradeIntent, 验证 idempotency_key 去重"""
        # TODO: Phase 1 实现
        pass

    def test_crash_after_outbox_write_before_send(self):
        """写入 outbox 后、发单前崩溃 → 重启后 dispatcher 自动补发"""
        pass

    def test_crash_after_send_before_confirm(self):
        """发单后、确认前崩溃 → 重启后通过 query_order 补确认"""
        pass

    def test_max_retries_exceeded(self):
        """超过 max_retries → 进入 FAILED + Telegram 告警"""
        pass
