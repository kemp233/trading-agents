from __future__ import annotations

import json
import sqlite3
from typing import Any
from uuid import uuid4

from dashboard.backend.chatops_models import ResolvedIntent, utc_now_iso


class WorkflowEngine:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path

    def create_task_and_run(
        self,
        *,
        intent: ResolvedIntent,
        source_message_id: str,
        created_by: str,
        system_mode: str,
    ) -> dict[str, Any]:
        task_id = f"task-{uuid4().hex}"
        workflow_run_id = f"run-{uuid4().hex}"
        now = utc_now_iso()
        status = "WAITING_APPROVAL" if intent.requires_approval else "RUNNING"
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                """
                INSERT INTO tasks (
                    task_id, source_type, source_message_id, target_role, intent_type,
                    workflow_type, status, priority, preemptible, requires_approval,
                    visibility, system_mode, created_by, arguments_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    intent.source_type,
                    source_message_id,
                    intent.target_role,
                    intent.intent_type,
                    intent.workflow_type,
                    status,
                    intent.priority,
                    int(intent.preemptible),
                    int(intent.requires_approval),
                    intent.visibility,
                    system_mode,
                    created_by,
                    json.dumps(intent.arguments, ensure_ascii=False),
                    now,
                    now,
                ),
            )
            conn.execute(
                """
                INSERT INTO workflow_runs (
                    workflow_run_id, task_id, workflow_type, workflow_class, status,
                    trigger_type, priority, started_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    workflow_run_id,
                    task_id,
                    intent.workflow_type,
                    intent.workflow_class,
                    status,
                    intent.source_type,
                    intent.priority,
                    now,
                ),
            )
            for idx, step in enumerate(intent.suggested_steps, start=1):
                conn.execute(
                    """
                    INSERT INTO workflow_steps (
                        step_id, workflow_run_id, step_order, step_role, step_type,
                        status, input_json, started_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"step-{uuid4().hex}",
                        workflow_run_id,
                        idx,
                        step.get("role", intent.target_role),
                        step.get("step_type", intent.workflow_type),
                        "QUEUED",
                        json.dumps(intent.arguments, ensure_ascii=False),
                        now,
                    ),
                )
            conn.commit()
        return {
            "task_id": task_id,
            "workflow_run_id": workflow_run_id,
            "status": status,
            "priority": intent.priority,
            "requires_approval": intent.requires_approval,
        }

    def mark_preempted(self, workflow_run_id: str, preempted_by_run_id: str, resumable: bool = True) -> None:
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                """
                UPDATE workflow_runs
                SET status = ?, preempted_by_run_id = ?, resume_token = ?, finished_at = ?
                WHERE workflow_run_id = ?
                """,
                (
                    "CANCELED" if not resumable else "WAITING_DATA",
                    preempted_by_run_id,
                    f"resume-{workflow_run_id}" if resumable else None,
                    utc_now_iso() if not resumable else None,
                    workflow_run_id,
                ),
            )
            conn.commit()

    def complete_run(self, workflow_run_id: str, status: str, summary: dict[str, Any] | None = None, error_text: str | None = None) -> None:
        task_status = "COMPLETED" if status in {"COMPLETED", "APPROVED"} else status
        with sqlite3.connect(self._db_path) as conn:
            row = conn.execute("SELECT task_id FROM workflow_runs WHERE workflow_run_id = ?", (workflow_run_id,)).fetchone()
            conn.execute(
                "UPDATE workflow_runs SET status = ?, summary_json = ?, error_text = ?, finished_at = ? WHERE workflow_run_id = ?",
                (status, json.dumps(summary or {}, ensure_ascii=False), error_text, utc_now_iso(), workflow_run_id),
            )
            if row:
                conn.execute("UPDATE tasks SET status = ?, updated_at = ? WHERE task_id = ?", (task_status, utc_now_iso(), row[0]))
            conn.commit()
