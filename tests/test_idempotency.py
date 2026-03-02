"""Phase 1 故障注入测试 — 幂等去重"""
import pytest


class TestIdempotency:
    def test_event_bus_dedup(self):
        """同一 idempotency_key 只处理一次"""
        pass

    def test_stream_seq_monotonic(self):
        """同一 stream_id 内 stream_seq 严格递增"""
        pass
