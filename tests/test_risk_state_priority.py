from __future__ import annotations

from agents.risk_governor import RiskGovernor


def _gov() -> RiskGovernor:
    return RiskGovernor(config={}, event_bus=None, state_writer=None)


def test_venue_halt_overrides_circuit() -> None:
    gov = _gov()
    result = gov.resolve_conflict(["CIRCUIT", "VENUE_HALT"])
    assert result == "VENUE_HALT"


def test_reconciling_overrides_degraded() -> None:
    gov = _gov()
    result = gov.resolve_conflict(["DEGRADED", "RECONCILING"])
    assert result == "RECONCILING"


def test_offline_overrides_degraded() -> None:
    gov = _gov()
    result = gov.resolve_conflict(["OFFLINE", "DEGRADED"])
    assert result == "OFFLINE"


def test_multi_state_conflict_returns_highest_priority() -> None:
    gov = _gov()
    result = gov.resolve_conflict(["DEGRADED", "CIRCUIT", "RECONCILING", "OFFLINE"])
    assert result == "RECONCILING"
