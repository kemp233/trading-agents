from __future__ import annotations

from dashboard.backend.context_policy import get_policy, trim_context_items


def test_strategy_policy_enables_summary_first_budget() -> None:
    policy = get_policy('strategy')

    assert policy.summary_first is True
    assert policy.max_items >= 1
    assert 'daily_fact_snapshots' in policy.allowed_sources


def test_trim_context_items_prefers_summaries() -> None:
    policy = get_policy('strategy')
    items = [
        {'kind': 'active_chat', 'priority': 10, 'payload': {'text': 'x' * 1000}},
        {'kind': 'daily_fact_snapshot', 'priority': 0, 'payload': {'fact': 'keep'}},
        {'kind': 'daily_summary', 'priority': 1, 'payload': {'summary': 'keep'}},
    ]

    trimmed = trim_context_items(items, policy)

    assert trimmed[0]['kind'] == 'daily_fact_snapshot'
    assert any(item['kind'] == 'daily_summary' for item in trimmed)
