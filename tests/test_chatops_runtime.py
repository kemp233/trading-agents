from __future__ import annotations

import sqlite3
from pathlib import Path

from dashboard.backend.chatops_runtime import ChatOpsRuntime
from dashboard.streamlit_mvp.db_reader import DbReader


SCHEMA_PATH = Path(__file__).resolve().parents[1] / 'db' / 'schema.sql'


def init_runtime(tmp_path: Path) -> tuple[ChatOpsRuntime, DbReader]:
    db_path = tmp_path / 'runtime.sqlite'
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA_PATH.read_text(encoding='utf-8'))
    conn.execute(
        'INSERT INTO risk_state_log (current_state, previous_state, state_changed_at, reason) VALUES (?, ?, ?, ?)',
        ('NORMAL', 'NORMAL', '2026-03-09T09:00:00+00:00', 'bootstrap'),
    )
    conn.execute(
        'INSERT INTO connection_log (ts, status, front_addr, session_id, detail) VALUES (?, ?, ?, ?, ?)',
        ('2026-03-09T09:01:00+00:00', 'CONNECTED', 'tcp://front', 'session-1', 'healthy'),
    )
    conn.execute(
        'INSERT INTO account_info (ts, user_id, broker_id, trading_day, available, margin, equity) VALUES (?, ?, ?, ?, ?, ?, ?)',
        ('2026-03-09T09:02:00+00:00', 'u1', '9999', '20260309', 1000.0, 200.0, 1200.0),
    )
    conn.execute(
        'INSERT INTO positions (symbol, venue, side, quantity, entry_price, unrealized_pnl, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)',
        ('rb2510', 'CTP', 'LONG', 2.0, 3500.0, 88.0, '2026-03-09T09:02:00+00:00'),
    )
    conn.commit()
    conn.close()
    return ChatOpsRuntime(str(db_path), repo_root=str(Path(__file__).resolve().parents[1])), DbReader(str(db_path))


def test_runtime_creates_approval_for_trade_proposal(tmp_path: Path) -> None:
    runtime, reader = init_runtime(tmp_path)

    reply = runtime.process_text('@strategy open long rb2510 with 1 lot')

    assert reply.ok is True
    assert reply.executed is False
    approvals = reader.get_pending_approvals(limit=5)
    assert len(approvals) == 1
    assert approvals[0]['requested_action'] == 'open_position'


def test_runtime_request_hash_deduplicates_duplicate_trade_requests(tmp_path: Path) -> None:
    runtime, reader = init_runtime(tmp_path)

    runtime.process_text('@strategy open long rb2510 with 1 lot')
    runtime.process_text('@strategy open long rb2510 with 1 lot')

    approvals = reader.get_pending_approvals(limit=10)
    assert len(approvals) == 1


def test_runtime_blocks_execution_natural_language(tmp_path: Path) -> None:
    runtime, reader = init_runtime(tmp_path)

    reply = runtime.process_text('@execution open long rb2510 now')

    assert reply.ok is False
    assert 'does not accept direct natural-language execution' in reply.content
    assert reader.get_pending_approvals(limit=5) == []


def test_runtime_resolve_approval_updates_status_and_emits_system_message(tmp_path: Path) -> None:
    runtime, reader = init_runtime(tmp_path)

    runtime.process_text('@strategy open long rb2510 with 1 lot')
    approval = reader.get_pending_approvals(limit=5)[0]

    resolved = runtime.resolve_approval(approval['approval_id'], 'approve')

    assert resolved is not None
    assert resolved['status'] == 'APPROVED'
    messages = reader.get_live_chat_messages(channel='group-chat', limit=20)
    assert any('APPROVED' in message.content for message in messages if message.author_kind == 'system')


def test_runtime_persists_reply_context_in_group_chat(tmp_path: Path) -> None:
    runtime, reader = init_runtime(tmp_path)
    runtime._invoke_role_provider = lambda role, text, context_items: 'stubbed live reply'

    runtime.process_text('@news check policy tone', reply_to={'message_id': 'm1', 'author': 'Risk', 'preview': 'Why blocked?'})

    messages = reader.get_live_chat_messages(channel='group-chat', limit=20)
    user_messages = [message for message in messages if message.author_kind == 'user']
    assert user_messages
    assert user_messages[-1].meta.get('reply_to', {}).get('author') == 'Risk'


def test_runtime_strategy_uses_provider_reply_when_available(tmp_path: Path) -> None:
    runtime, _reader = init_runtime(tmp_path)
    runtime._invoke_role_provider = lambda role, text, context_items: 'strategy live reply' if role == 'strategy' else None

    reply = runtime.process_text('@strategy analyze rb2510')

    assert reply.ok is True
    assert reply.executed is True
    assert reply.content == 'strategy live reply'
