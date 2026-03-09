from __future__ import annotations

from dashboard.backend.intent_resolver import parse_command, resolve_intent


def test_parse_command_legacy_format() -> None:
    parsed = parse_command('@Risk_Governor /state')

    assert parsed == {'agent': 'Risk_Governor', 'command': '/state', 'args': ''}


def test_resolve_news_natural_language_as_analysis() -> None:
    intent = resolve_intent('@news summarize overnight policy updates')

    assert intent.target_role == 'news'
    assert intent.intent_type == 'analysis_query'
    assert intent.workflow_class == 'analysis'


def test_resolve_open_position_requires_approval() -> None:
    intent = resolve_intent('@strategy open long rb2510 with 1 lot')

    assert intent.workflow_type == 'open_position'
    assert intent.requires_approval is True
    assert intent.priority == 'HIGH'


def test_execution_rejects_natural_language_direct_mode() -> None:
    intent = resolve_intent('@execution open long rb2510 now')

    assert intent.intent_type == 'structured_input_required'
    assert intent.workflow_class == 'execution'
