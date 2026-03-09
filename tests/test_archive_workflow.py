from __future__ import annotations

import sqlite3
from pathlib import Path

from dashboard.backend.archive_workflow import ArchiveWorkflow


SCHEMA_PATH = Path(__file__).resolve().parents[1] / 'db' / 'schema.sql'


def init_db(tmp_path: Path) -> tuple[str, str]:
    db_path = tmp_path / 'archive.sqlite'
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA_PATH.read_text(encoding='utf-8'))
    conn.execute(
        'INSERT INTO tasks (task_id, source_type, source_message_id, target_role, intent_type, workflow_type, status, priority, preemptible, requires_approval, visibility, system_mode, created_by, arguments_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        ('task-1', 'natural_language', 'msg-1', 'strategy', 'trade_proposal', 'open_position', 'WAITING_APPROVAL', 'HIGH', 1, 1, 'channel', 'NORMAL', 'tester', '{}', '2026-03-09T09:00:00+00:00', '2026-03-09T09:00:00+00:00'),
    )
    conn.execute(
        'INSERT INTO workflow_runs (workflow_run_id, task_id, workflow_type, workflow_class, status, trigger_type, priority, started_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        ('run-1', 'task-1', 'open_position', 'execution', 'WAITING_APPROVAL', 'natural_language', 'HIGH', '2026-03-09T09:00:00+00:00'),
    )
    conn.execute(
        'INSERT INTO approval_requests (approval_id, task_id, workflow_run_id, approval_type, status, requested_action, instrument, position_delta, risk_level, expires_at, market_snapshot_id, risk_snapshot_id, request_hash, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        ('approval-1', 'task-1', 'run-1', 'trade_execution', 'PENDING', 'open_position', 'rb2510', 1.0, 'high', '2026-03-09T09:05:00+00:00', 'market:1', 'risk:1', 'hash-1', '2026-03-09T09:00:00+00:00'),
    )
    conn.execute(
        'INSERT INTO orders (order_id, client_order_id, symbol, venue, side, quantity, price, status, strategy_id, created_at, updated_at, filled_quantity, filled_price) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        ('order-1', 'client-1', 'rb2510', 'CTP', 'BUY', 1.0, 3500.0, 'FILLED', 's1', '2026-03-09T09:00:00+00:00', '2026-03-09T09:01:00+00:00', 1.0, 3500.0),
    )
    conn.execute(
        'INSERT INTO positions (symbol, venue, side, quantity, entry_price, unrealized_pnl, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)',
        ('rb2510', 'CTP', 'LONG', 1.0, 3500.0, 123.0, '2026-03-09T09:01:00+00:00'),
    )
    conn.execute(
        'INSERT INTO risk_state_log (current_state, previous_state, state_changed_at, reason) VALUES (?, ?, ?, ?)',
        ('NORMAL', 'NORMAL', '2026-03-09T09:00:00+00:00', 'bootstrap'),
    )
    conn.commit()
    conn.close()
    return str(db_path), str(Path(__file__).resolve().parents[1])


def test_archive_workflow_writes_facts_and_summary(tmp_path: Path) -> None:
    db_path, repo_root = init_db(tmp_path)
    workflow = ArchiveWorkflow(db_path, repo_root)

    result = workflow.run_for_day('20260309')

    assert result['status'] in {'SUCCESS', 'PARTIAL'}
    runtime_path = Path(result['runtime_path'])
    assert runtime_path.exists()

    conn = sqlite3.connect(db_path)
    facts = conn.execute('SELECT trading_day, schema_version, generator_version FROM daily_fact_snapshots').fetchone()
    summaries = conn.execute('SELECT trading_day, status FROM daily_summaries').fetchone()
    conn.close()

    assert facts == ('20260309', '1.0', 'issue9-chatops-v1')
    assert summaries[0] == '20260309'
