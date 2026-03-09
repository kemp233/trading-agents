from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def infer_trading_day(ts: datetime | None = None) -> str:
    current = ts or utc_now()
    return current.astimezone(timezone.utc).strftime("%Y%m%d")


TASK_PRIORITY = {"LOW", "NORMAL", "HIGH", "CRITICAL"}
ANALYSIS_STATUSES = {"QUEUED", "RUNNING", "WAITING_DATA", "PARTIAL", "COMPLETED", "FAILED", "CANCELED"}
EXECUTION_STATUSES = {
    "QUEUED", "VALIDATING", "WAITING_RISK", "WAITING_APPROVAL", "APPROVED",
    "EXECUTING", "RECONCILING", "COMPLETED", "FAILED", "BLOCKED", "EXPIRED",
    "SUPERSEDED", "AUTO_CANCELED",
}
APPROVAL_STATUSES = {"PENDING", "APPROVED", "REJECTED", "ESCALATED", "EXPIRED", "SUPERSEDED", "AUTO_CANCELED"}
SYSTEM_MODES = {"NORMAL", "CAUTION", "RESTRICTED", "PROTECT_ONLY", "HALTED", "RECONCILING"}


@dataclass(slots=True)
class ResolvedIntent:
    raw_text: str
    source_type: str
    target_role: str
    intent_type: str
    workflow_type: str
    workflow_class: str
    channel: str
    priority: str = "NORMAL"
    preemptible: bool = True
    requires_approval: bool = False
    visibility: str = "channel"
    arguments: dict[str, Any] = field(default_factory=dict)
    summary: str = ""
    deterministic: bool = False
    suggested_steps: list[dict[str, Any]] = field(default_factory=list)


def coerce_priority(value: str | None, default: str = "NORMAL") -> str:
    candidate = str(value or default).upper()
    return candidate if candidate in TASK_PRIORITY else default
