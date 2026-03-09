from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any
from uuid import uuid4

from dashboard.backend.chatops_models import utc_now_iso


FACT_SCHEMA_VERSION = '1.0'
FACT_GENERATOR_VERSION = 'issue9-chatops-v1'


class ArchiveWorkflow:
    def __init__(self, db_path: str, repo_root: str) -> None:
        self._db_path = db_path
        self._repo_root = Path(repo_root)

    def run_for_day(self, trading_day: str) -> dict[str, Any]:
        facts = self._build_fact_snapshot(trading_day)
        runtime_path = self._repo_root / 'output' / 'daily_reports' / trading_day / 'chatops-summary.md'
        knowledge_path = self._repo_root / 'docs' / 'records' / trading_day[:4] / f'{trading_day}-trading-journal.md'
        runtime_path.parent.mkdir(parents=True, exist_ok=True)
        knowledge_path.parent.mkdir(parents=True, exist_ok=True)

        status = 'SUCCESS'
        runtime_ok = self._safe_write(runtime_path, self._build_markdown(trading_day, facts))
        knowledge_ok = self._safe_write(knowledge_path, self._build_markdown(trading_day, facts, detailed=True))
        if runtime_ok and not knowledge_ok:
            status = 'PARTIAL'
        elif not runtime_ok and not knowledge_ok:
            status = 'FAILED'

        self._persist_fact_snapshot(trading_day, facts)
        self._persist_summary(trading_day, facts, status, runtime_path if runtime_ok else None, knowledge_path if knowledge_ok else None)
        return {
            'status': status,
            'runtime_path': str(runtime_path) if runtime_ok else '',
            'knowledge_path': str(knowledge_path) if knowledge_ok else '',
            'facts': facts,
        }

    def _safe_write(self, path: Path, content: str) -> bool:
        try:
            path.write_text(content, encoding='utf-8')
            return True
        except OSError:
            return False

    def _build_fact_snapshot(self, trading_day: str) -> dict[str, Any]:
        with sqlite3.connect(self._db_path) as conn:
            conn.row_factory = sqlite3.Row
            orders = [dict(row) for row in conn.execute('SELECT * FROM orders ORDER BY updated_at DESC').fetchall()]
            approvals = [dict(row) for row in conn.execute('SELECT * FROM approval_requests').fetchall()]
            tasks = [dict(row) for row in conn.execute('SELECT * FROM tasks').fetchall()]
            positions = [dict(row) for row in conn.execute('SELECT * FROM positions').fetchall()]
            risk_states = [dict(row) for row in conn.execute('SELECT * FROM risk_state_log ORDER BY id DESC').fetchall()]
            workflow_runs = [dict(row) for row in conn.execute('SELECT * FROM workflow_runs').fetchall()]
        symbols = sorted({row.get('symbol') for row in orders if row.get('symbol')})
        facts = {
            'instrument_stats_json': {
                'symbols': symbols,
                'order_count': len(orders),
            },
            'decision_counts_json': {
                'task_count': len(tasks),
                'analysis_tasks': sum(1 for row in tasks if row.get('workflow_type') == 'analysis_query'),
                'execution_tasks': sum(1 for row in tasks if row.get('workflow_type') in {'open_position', 'close_position', 'incident_response'}),
            },
            'risk_event_counts_json': {
                'states': {state: sum(1 for item in risk_states if item.get('current_state') == state) for state in {row.get('current_state', 'UNKNOWN') for row in risk_states}},
            },
            'approval_stats_json': {
                'pending': sum(1 for row in approvals if row.get('status') == 'PENDING'),
                'approved': sum(1 for row in approvals if row.get('status') == 'APPROVED'),
            },
            'execution_stats_json': {
                'filled': sum(1 for row in orders if row.get('status') == 'FILLED'),
                'rejected': sum(1 for row in orders if row.get('status') == 'REJECTED'),
            },
            'reconciliation_stats_json': {
                'position_count': len(positions),
            },
            'portfolio_exposure_json': {
                'position_count': len(positions),
                'symbols': [row.get('symbol') for row in positions if row.get('symbol')],
            },
            'incident_flags_json': {
                'has_incident': any(row.get('priority') == 'CRITICAL' for row in tasks),
            },
            'workflow_stats_json': {
                'run_count': len(workflow_runs),
                'workflow_types': {wf: sum(1 for item in workflow_runs if item.get('workflow_type') == wf) for wf in {row.get('workflow_type', 'unknown') for row in workflow_runs}},
            },
            'fallback_stats_json': {
                'partial_runs': sum(1 for row in workflow_runs if row.get('status') == 'PARTIAL'),
                'failed_runs': sum(1 for row in workflow_runs if row.get('status') == 'FAILED'),
            },
        }
        raw = json.dumps(facts, ensure_ascii=False, sort_keys=True)
        facts['schema_version'] = FACT_SCHEMA_VERSION
        facts['generator_version'] = FACT_GENERATOR_VERSION
        facts['checksum'] = hashlib.sha256(raw.encode('utf-8')).hexdigest()
        facts['headline'] = f'{trading_day} post-close summary: tasks={len(tasks)}, approvals={len(approvals)}, orders={len(orders)}'
        return facts

    def _persist_fact_snapshot(self, trading_day: str, facts: dict[str, Any]) -> None:
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO daily_fact_snapshots (
                    snapshot_id, trading_day, instrument_stats_json, decision_counts_json,
                    risk_event_counts_json, approval_stats_json, execution_stats_json,
                    reconciliation_stats_json, portfolio_exposure_json, incident_flags_json,
                    workflow_stats_json, fallback_stats_json, schema_version,
                    generator_version, checksum, generated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f'fact-{uuid4().hex}',
                    trading_day,
                    json.dumps(facts['instrument_stats_json'], ensure_ascii=False),
                    json.dumps(facts['decision_counts_json'], ensure_ascii=False),
                    json.dumps(facts['risk_event_counts_json'], ensure_ascii=False),
                    json.dumps(facts['approval_stats_json'], ensure_ascii=False),
                    json.dumps(facts['execution_stats_json'], ensure_ascii=False),
                    json.dumps(facts['reconciliation_stats_json'], ensure_ascii=False),
                    json.dumps(facts['portfolio_exposure_json'], ensure_ascii=False),
                    json.dumps(facts['incident_flags_json'], ensure_ascii=False),
                    json.dumps(facts['workflow_stats_json'], ensure_ascii=False),
                    json.dumps(facts['fallback_stats_json'], ensure_ascii=False),
                    facts['schema_version'],
                    facts['generator_version'],
                    facts['checksum'],
                    utc_now_iso(),
                ),
            )
            conn.commit()

    def _persist_summary(self, trading_day: str, facts: dict[str, Any], status: str, runtime_path: Path | None, knowledge_path: Path | None) -> None:
        summary_json = {
            'headline': facts['headline'],
            'next_focus': facts['portfolio_exposure_json'].get('symbols', []),
            'approval': facts['approval_stats_json'],
            'execution': facts['execution_stats_json'],
        }
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO daily_summaries (
                    summary_id, trading_day, runtime_path, knowledge_path, status,
                    headline, summary_json, generated_at, source_window_start, source_window_end
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f'summary-{uuid4().hex}',
                    trading_day,
                    str(runtime_path) if runtime_path else '',
                    str(knowledge_path) if knowledge_path else '',
                    status,
                    facts['headline'],
                    json.dumps(summary_json, ensure_ascii=False),
                    utc_now_iso(),
                    f'{trading_day}T00:00:00+00:00',
                    f'{trading_day}T23:59:59+00:00',
                ),
            )
            conn.commit()

    def _build_markdown(self, trading_day: str, facts: dict[str, Any], detailed: bool = False) -> str:
        symbols = facts['portfolio_exposure_json'].get('symbols', [])
        sections = [
            f'# {trading_day} post-close record',
            '',
            '## Session Overview',
            facts['headline'],
            '',
            '## Execution Stats',
            json.dumps(facts['execution_stats_json'], ensure_ascii=False),
            '',
            '## Risk And Approval Stats',
            json.dumps(facts['approval_stats_json'], ensure_ascii=False),
            '',
            '## Portfolio Exposure',
            json.dumps(facts['portfolio_exposure_json'], ensure_ascii=False),
            '',
            '## Next Focus',
            '- ' + '\n- '.join(symbols) if symbols else '- none',
            '',
            '## Lessons Learned',
            '- Use structured facts as the primary cross-day context input.',
        ]
        if detailed:
            sections.extend([
                '',
                '## Market Stats',
                json.dumps(facts['instrument_stats_json'], ensure_ascii=False),
                '',
                '## Workflow Stats',
                json.dumps(facts['workflow_stats_json'], ensure_ascii=False),
            ])
        return '\n'.join(sections).strip() + '\n'
