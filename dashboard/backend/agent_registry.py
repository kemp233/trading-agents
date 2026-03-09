from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


DEFAULT_AGENT_CAPABILITIES: dict[str, dict[str, Any]] = {
    "orchestration": {
        "provider": "qwen",
        "allowed_workflows": ["analysis_query", "daily_briefing", "post_market_report", "watchlist_scan"],
        "can_read_market_data": True,
        "can_read_positions": True,
        "can_read_logs": True,
        "can_generate_trade_advice": False,
        "can_request_approval": True,
        "can_trigger_execution": False,
        "can_force_protective_action": False,
        "requires_structured_input": False,
        "supports_natural_language": True,
        "fallback_provider": "deepseek-chat",
        "enabled": True,
    },
    "strategy": {
        "provider": "deepseek-reasoner",
        "allowed_workflows": ["analysis_query", "watchlist_scan", "open_position", "close_position", "post_market_review"],
        "can_read_market_data": True,
        "can_read_positions": True,
        "can_read_logs": False,
        "can_generate_trade_advice": True,
        "can_request_approval": True,
        "can_trigger_execution": False,
        "can_force_protective_action": False,
        "requires_structured_input": False,
        "supports_natural_language": True,
        "fallback_provider": "qwen",
        "enabled": True,
    },
    "risk": {
        "provider": "qwen",
        "allowed_workflows": ["incident_response", "open_position", "close_position", "protective_cancel", "forced_liquidation"],
        "can_read_market_data": True,
        "can_read_positions": True,
        "can_read_logs": True,
        "can_generate_trade_advice": False,
        "can_request_approval": False,
        "can_trigger_execution": False,
        "can_force_protective_action": True,
        "requires_structured_input": True,
        "supports_natural_language": True,
        "fallback_provider": "doubao",
        "enabled": True,
    },
    "execution": {
        "provider": None,
        "allowed_workflows": ["open_position", "close_position", "protective_cancel", "forced_liquidation"],
        "can_read_market_data": True,
        "can_read_positions": True,
        "can_read_logs": False,
        "can_generate_trade_advice": False,
        "can_request_approval": False,
        "can_trigger_execution": True,
        "can_force_protective_action": False,
        "requires_structured_input": True,
        "supports_natural_language": False,
        "fallback_provider": None,
        "enabled": True,
    },
    "market-data": {
        "provider": None,
        "allowed_workflows": ["watchlist_scan", "trading_session", "off_session"],
        "can_read_market_data": True,
        "can_read_positions": False,
        "can_read_logs": False,
        "can_generate_trade_advice": False,
        "can_request_approval": False,
        "can_trigger_execution": False,
        "can_force_protective_action": False,
        "requires_structured_input": True,
        "supports_natural_language": False,
        "fallback_provider": None,
        "enabled": True,
    },
    "news": {
        "provider": "doubao",
        "allowed_workflows": ["analysis_query", "watchlist_scan", "daily_briefing"],
        "can_read_market_data": False,
        "can_read_positions": False,
        "can_read_logs": False,
        "can_generate_trade_advice": False,
        "can_request_approval": False,
        "can_trigger_execution": False,
        "can_force_protective_action": False,
        "requires_structured_input": False,
        "supports_natural_language": True,
        "fallback_provider": "deepseek-chat",
        "enabled": True,
    },
    "reconciliation": {
        "provider": "doubao",
        "allowed_workflows": ["incident_response", "post_close_archive", "post_market_report"],
        "can_read_market_data": False,
        "can_read_positions": True,
        "can_read_logs": True,
        "can_generate_trade_advice": False,
        "can_request_approval": False,
        "can_trigger_execution": False,
        "can_force_protective_action": False,
        "requires_structured_input": True,
        "supports_natural_language": True,
        "fallback_provider": "qwen",
        "enabled": True,
    },
    "portfolio": {
        "provider": "qwen",
        "allowed_workflows": ["analysis_query", "open_position", "close_position", "daily_briefing"],
        "can_read_market_data": True,
        "can_read_positions": True,
        "can_read_logs": False,
        "can_generate_trade_advice": False,
        "can_request_approval": False,
        "can_trigger_execution": False,
        "can_force_protective_action": False,
        "requires_structured_input": True,
        "supports_natural_language": True,
        "fallback_provider": "deepseek-reasoner",
        "enabled": True,
    },
}


def get_agent_capability(role: str) -> dict[str, Any]:
    return dict(DEFAULT_AGENT_CAPABILITIES.get(role, {}))


def ensure_seeded(db_path: str) -> None:
    if not Path(db_path).exists():
        return
    with sqlite3.connect(db_path) as conn:
        for agent_name, config in DEFAULT_AGENT_CAPABILITIES.items():
            conn.execute(
                """
                INSERT OR REPLACE INTO agent_capabilities (
                    agent_name, provider, allowed_workflows, can_read_market_data,
                    can_read_positions, can_read_logs, can_generate_trade_advice,
                    can_request_approval, can_trigger_execution, can_force_protective_action,
                    requires_structured_input, supports_natural_language, fallback_provider, enabled
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    agent_name,
                    config.get("provider"),
                    json.dumps(config.get("allowed_workflows", []), ensure_ascii=False),
                    int(bool(config.get("can_read_market_data"))),
                    int(bool(config.get("can_read_positions"))),
                    int(bool(config.get("can_read_logs"))),
                    int(bool(config.get("can_generate_trade_advice"))),
                    int(bool(config.get("can_request_approval"))),
                    int(bool(config.get("can_trigger_execution"))),
                    int(bool(config.get("can_force_protective_action"))),
                    int(bool(config.get("requires_structured_input"))),
                    int(bool(config.get("supports_natural_language", True))),
                    config.get("fallback_provider"),
                    int(bool(config.get("enabled", True))),
                ),
            )
        conn.commit()
