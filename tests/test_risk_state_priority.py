"""Phase 1 故障注入测试 — 风控状态机优先级"""
import pytest
from agents.risk_governor import RiskGovernor


class TestRiskStatePriority:
    def test_venue_halt_overrides_circuit(self):
        """VENUE_HALT 优先级高于 CIRCUIT_BREAKER"""
        gov = RiskGovernor.__new__(RiskGovernor)
        gov.STATE_PRIORITY = RiskGovernor.STATE_PRIORITY
        result = gov.resolve_conflict(['CIRCUIT_BREAKER', 'VENUE_HALT'])
        assert result == 'VENUE_HALT'

    def test_reconciling_overrides_degraded(self):
        """RECONCILING 优先级高于 DEGRADED"""
        gov = RiskGovernor.__new__(RiskGovernor)
        gov.STATE_PRIORITY = RiskGovernor.STATE_PRIORITY
        result = gov.resolve_conflict(['DEGRADED', 'RECONCILING'])
        assert result == 'RECONCILING'

    def test_triple_conflict_resolution(self):
        """三状态同时触发 → 取最高优先级"""
        gov = RiskGovernor.__new__(RiskGovernor)
        gov.STATE_PRIORITY = RiskGovernor.STATE_PRIORITY
        result = gov.resolve_conflict(
            ['CIRCUIT_BREAKER', 'RECONCILING', 'DEGRADED']
        )
        assert result == 'RECONCILING'
