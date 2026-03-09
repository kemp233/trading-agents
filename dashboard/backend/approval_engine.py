from __future__ import annotations

import hashlib
import sqlite3
from datetime import timedelta
from typing import Any
from uuid import uuid4

from dashboard.backend.chatops_models import utc_now, utc_now_iso


class ApprovalEngine:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path

    def create_request(
        self,
        *,
        task_id: str,
        workflow_run_id: str,
        approval_type: str,
        requested_action: str,
        instrument: str | None,
        position_delta: float,
        risk_level: str,
        market_snapshot_id: str | None,
        risk_snapshot_id: str | None,
        system_mode: str,
        expires_in_minutes: int = 5,
    ) -> dict[str, Any]:
        expires_at = (utc_now() + timedelta(minutes=expires_in_minutes)).isoformat()
        request_hash = self.build_request_hash(
            requested_action=requested_action,
            instrument=instrument,
            position_delta=position_delta,
            market_snapshot_id=market_snapshot_id,
            risk_snapshot_id=risk_snapshot_id,
            system_mode=system_mode,
        )
        with sqlite3.connect(self._db_path) as conn:
            conn.row_factory = sqlite3.Row
            existing = conn.execute(
                "SELECT * FROM approval_requests WHERE request_hash = ?",
                (request_hash,),
            ).fetchone()
            if existing:
                return dict(existing)
            approval_id = f"approval-{uuid4().hex}"
            conn.execute(
                """
                INSERT INTO approval_requests (
                    approval_id, task_id, workflow_run_id, approval_type, status,
                    requested_action, instrument, position_delta, risk_level,
                    expires_at, market_snapshot_id, risk_snapshot_id,
                    request_hash, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    approval_id,
                    task_id,
                    workflow_run_id,
                    approval_type,
                    "PENDING",
                    requested_action,
                    instrument,
                    position_delta,
                    risk_level,
                    expires_at,
                    market_snapshot_id,
                    risk_snapshot_id,
                    request_hash,
                    utc_now_iso(),
                ),
            )
            conn.commit()
            return dict(conn.execute("SELECT * FROM approval_requests WHERE approval_id = ?", (approval_id,)).fetchone())

    def expire_for_system_mode(self, system_mode: str) -> int:
        if system_mode not in {"PROTECT_ONLY", "HALTED", "RECONCILING"}:
            return 0
        with sqlite3.connect(self._db_path) as conn:
            cursor = conn.execute(
                """
                UPDATE approval_requests
                SET status = 'AUTO_CANCELED', resolved_at = ?, resolution_note = ?
                WHERE status = 'PENDING'
                """,
                (utc_now_iso(), f"system_mode={system_mode}"),
            )
            conn.commit()
            return int(cursor.rowcount or 0)

    def expire_stale_requests(self, current_market_snapshot_id: str | None, current_risk_snapshot_id: str | None) -> int:
        with sqlite3.connect(self._db_path) as conn:
            cursor = conn.execute(
                """
                UPDATE approval_requests
                SET status = 'EXPIRED', resolved_at = ?, resolution_note = 'snapshot_changed_or_timeout'
                WHERE status = 'PENDING' AND (
                    (expires_at IS NOT NULL AND expires_at < ?)
                    OR (? IS NOT NULL AND market_snapshot_id IS NOT NULL AND market_snapshot_id != ?)
                    OR (? IS NOT NULL AND risk_snapshot_id IS NOT NULL AND risk_snapshot_id != ?)
                )
                """,
                (
                    utc_now_iso(),
                    utc_now_iso(),
                    current_market_snapshot_id,
                    current_market_snapshot_id,
                    current_risk_snapshot_id,
                    current_risk_snapshot_id,
                ),
            )
            conn.commit()
            return int(cursor.rowcount or 0)


    def resolve_request(self, approval_id: str, *, status: str, resolved_by: str = 'operator', resolution_note: str | None = None) -> dict[str, Any] | None:
        allowed = {'APPROVED', 'REJECTED', 'ESCALATED'}
        if status not in allowed:
            raise ValueError(f'unsupported approval status: {status}')
        with sqlite3.connect(self._db_path) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute(
                """
                UPDATE approval_requests
                SET status = ?, resolved_by = ?, resolved_at = ?, resolution_note = ?
                WHERE approval_id = ? AND status = 'PENDING'
                """,
                (status, resolved_by, utc_now_iso(), resolution_note or status.lower(), approval_id),
            )
            conn.commit()
            row = conn.execute("SELECT * FROM approval_requests WHERE approval_id = ?", (approval_id,)).fetchone()
            return dict(row) if row else None

    @staticmethod
    def build_request_hash(
        *,
        requested_action: str,
        instrument: str | None,
        position_delta: float,
        market_snapshot_id: str | None,
        risk_snapshot_id: str | None,
        system_mode: str,
    ) -> str:
        payload = "|".join([
            requested_action,
            instrument or "",
            f"{position_delta:.8f}",
            market_snapshot_id or "",
            risk_snapshot_id or "",
            system_mode,
        ])
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
