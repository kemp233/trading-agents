from __future__ import annotations

import re
from typing import Any

from dashboard.backend.agent_registry import get_agent_capability
from dashboard.backend.chatops_models import ResolvedIntent, coerce_priority


ROLE_ALIASES = {
    "all": "orchestration",
    "orchestration": "orchestration",
    "strategy": "strategy",
    "risk_governor": "risk",
    "risk": "risk",
    "order_manager": "execution",
    "execution": "execution",
    "market_data": "market-data",
    "market-data": "market-data",
    "news": "news",
    "reconciler": "reconciliation",
    "reconciliation": "reconciliation",
    "portfolio": "portfolio",
    "log": "reconciliation",
}

TRADE_OPEN_KEYWORDS = (
    "open", "long", "buy", "\u5f00\u4ed3", "\u5f00\u591a", "\u505a\u591a", "\u4e70\u5165", "\u52a0\u4ed3", "\u505a\u4e00\u4e2a\u591a\u5355",
)
TRADE_CLOSE_KEYWORDS = (
    "close", "reduce", "sell", "\u5e73\u4ed3", "\u51cf\u4ed3", "\u5e73\u591a", "\u5e73\u7a7a", "\u6b62\u76c8", "\u6b62\u635f",
)
EMERGENCY_KEYWORDS = (
    "emergency", "halt", "flatten", "cancel all", "\u7d27\u6025", "\u7194\u65ad", "\u51bb\u7ed3", "\u4e00\u952e\u5e73\u4ed3", "\u5168\u90e8\u64a4\u5355", "\u5f3a\u5e73", "\u6682\u505c\u4ea4\u6613",
)
REPORT_KEYWORDS = (
    "report", "summary", "brief", "recap", "\u65e5\u62a5", "\u7b80\u62a5", "\u603b\u7ed3", "\u6c47\u603b", "\u590d\u76d8", "\u76d8\u524d", "\u76d8\u540e",
)
LOG_KEYWORDS = (
    "log", "\u65e5\u5fd7", "records", "record", "\u5ba1\u8ba1", "\u5bf9\u8d26\u8bb0\u5f55", "\u8fd0\u884c\u8bb0\u5f55",
)
WATCHLIST_KEYWORDS = (
    "watchlist", "scan", "\u76ef\u76d8", "\u626b\u63cf", "\u89c2\u5bdf", "\u76d1\u63a7", "\u770b\u770b\u54ea\u4e9b", "\u5173\u6ce8\u54ea\u4e9b",
)
QUESTION_KEYWORDS = (
    "why", "blocked", "reject", "rejected", "\u4e3a\u4ec0\u4e48", "\u4e0d\u8ba9", "\u88ab\u62d2\u7edd", "\u88ab\u62e6\u622a", "\u4e3a\u4ec0\u4e48\u4e0d\u80fd",
)


def parse_command(text: str) -> dict[str, str] | None:
    match = re.match(r"@([\w-]+)\s+((?:/\w+)|(?:-command[\w-]*))\s*(.*)?", text.strip())
    if not match:
        return None
    return {
        "agent": match.group(1),
        "command": match.group(2),
        "args": match.group(3).strip() if match.group(3) else "",
    }


def resolve_intent(text: str) -> ResolvedIntent:
    command = parse_command(text)
    if command:
        role = ROLE_ALIASES.get(command["agent"].lower(), "orchestration")
        workflow_type = "command_execution"
        if command["command"].startswith("-command"):
            workflow_type = "command_shorthand"
        return ResolvedIntent(
            raw_text=text,
            source_type="slash_command",
            target_role=role,
            intent_type="command",
            workflow_type=workflow_type,
            workflow_class="analysis",
            channel="group-chat",
            priority="NORMAL",
            preemptible=True,
            requires_approval=False,
            visibility="channel",
            arguments={"command": command["command"], "args": command["args"]},
            summary=f"execute {command['command']}",
            deterministic=True,
            suggested_steps=[{"role": role, "step_type": "command"}],
        )

    role, body = _extract_role(text)
    lowered = body.lower()
    workflow_type = "analysis_query"
    workflow_class = "analysis"
    intent_type = "analysis_query"
    requires_approval = False
    priority = "NORMAL"
    preemptible = True

    if role == "risk" and _contains_any(body, QUESTION_KEYWORDS):
        workflow_type = "analysis_query"
        intent_type = "analysis_query"
    elif _contains_any(body, WATCHLIST_KEYWORDS):
        workflow_type = "watchlist_scan"
        intent_type = "summary_request"
    elif _contains_any(body, TRADE_OPEN_KEYWORDS):
        workflow_type = "open_position"
        workflow_class = "execution"
        intent_type = "trade_proposal"
        requires_approval = True
        priority = "HIGH"
    elif _contains_any(body, TRADE_CLOSE_KEYWORDS):
        workflow_type = "close_position"
        workflow_class = "execution"
        intent_type = "trade_execution_request"
        requires_approval = True
        priority = "HIGH"
    elif _contains_any(body, EMERGENCY_KEYWORDS):
        workflow_type = "incident_response"
        workflow_class = "execution"
        intent_type = "system_action"
        priority = "CRITICAL"
        preemptible = False
    elif _contains_any(body, REPORT_KEYWORDS):
        workflow_type = "daily_briefing"
        intent_type = "report_request"
    elif _contains_any(body, LOG_KEYWORDS):
        workflow_type = "analysis_query"
        intent_type = "log_request"

    capability = get_agent_capability(role)
    if capability and not capability.get("supports_natural_language", True):
        intent_type = "structured_input_required"
        workflow_class = "execution"
        requires_approval = False

    return ResolvedIntent(
        raw_text=text,
        source_type="natural_language",
        target_role=role,
        intent_type=intent_type,
        workflow_type=workflow_type,
        workflow_class=workflow_class,
        channel="group-chat",
        priority=coerce_priority(priority),
        preemptible=preemptible,
        requires_approval=requires_approval,
        visibility="channel",
        arguments={"query": body or text, "target_role": role, "query_lower": lowered},
        summary=body or text,
        deterministic=False,
        suggested_steps=_suggest_steps(role, workflow_type),
    )


def _extract_role(text: str) -> tuple[str, str]:
    stripped = text.strip()
    match = re.match(r"@([\w-]+)\s*(.*)$", stripped)
    if not match:
        return "orchestration", stripped
    alias = match.group(1).lower()
    return ROLE_ALIASES.get(alias, "orchestration"), match.group(2).strip()


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    lowered = text.lower()
    for keyword in keywords:
        if keyword.lower() in lowered:
            return True
    return False


def _suggest_steps(role: str, workflow_type: str) -> list[dict[str, Any]]:
    if workflow_type == "open_position":
        return [
            {"role": role or "strategy", "step_type": "analysis"},
            {"role": "portfolio", "step_type": "portfolio_check"},
            {"role": "risk", "step_type": "risk_gate"},
            {"role": "execution", "step_type": "execution"},
        ]
    if workflow_type == "close_position":
        return [
            {"role": "portfolio", "step_type": "position_check"},
            {"role": "market-data", "step_type": "liquidity_check"},
            {"role": "risk", "step_type": "risk_gate"},
            {"role": "execution", "step_type": "execution"},
        ]
    if workflow_type == "incident_response":
        return [
            {"role": "risk", "step_type": "risk_gate"},
            {"role": "portfolio", "step_type": "exposure_check"},
            {"role": "execution", "step_type": "protective_action"},
        ]
    return [{"role": role, "step_type": workflow_type}]
