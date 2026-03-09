from __future__ import annotations

from dashboard.backend.context_policy import get_policy, trim_context_items


class ConversationContextStore:
    def __init__(self, db_reader) -> None:
        self._db_reader = db_reader

    def build(self, role: str, query: str) -> list[dict]:
        policy = get_policy(role)
        items: list[dict] = []
        if policy.allow_daily_fact_snapshots:
            for fact in self._db_reader.get_recent_daily_fact_snapshots(limit=min(3, policy.max_items)):
                items.append({"kind": "daily_fact_snapshot", "priority": 0, "payload": fact})
        if policy.allow_daily_summaries:
            for summary in self._db_reader.get_recent_daily_summaries(limit=min(3, policy.max_items)):
                items.append({"kind": "daily_summary", "priority": 1, "payload": summary})
        if policy.allow_active_chat and "active_chat" in policy.allowed_sources:
            for message in self._db_reader.get_chat_messages(channel="all", limit=min(20, policy.max_items)):
                items.append({"kind": "active_chat", "priority": 3, "payload": message.to_dict()})
        if "portfolio_snapshot" in policy.allowed_sources:
            items.append({"kind": "portfolio_snapshot", "priority": 2, "payload": self._db_reader.get_portfolio_snapshot()})
        if "risk_state" in policy.allowed_sources or "risk_state_today" in policy.allowed_sources:
            items.append({"kind": "risk_state", "priority": 2, "payload": self._db_reader.get_latest_risk_record()})
        if "orders_today" in policy.allowed_sources:
            items.append({"kind": "orders_today", "priority": 2, "payload": self._db_reader.get_orders(limit=10)})
        if "positions_today" in policy.allowed_sources:
            items.append({"kind": "positions_today", "priority": 2, "payload": self._db_reader.get_positions()})
        if "query" in policy.allowed_sources:
            items.append({"kind": "query", "priority": 0, "payload": {"query": query}})
        return trim_context_items(items, policy)
