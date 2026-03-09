from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any

from dashboard.backend.chat_types import ChatMessage


AGENT_LABELS = {
    "Market_Data": "行情感知",
    "Risk_Governor": "风控治理",
    "Order_Manager": "执行管理",
    "Strategy": "策略分析",
    "News": "资讯研究",
    "Reconciler": "对账复核",
    "System": "系统",
    "Futures_Monitor": "监控器",
}

STATUS_LABELS = {
    "CONNECTED": "已连接",
    "DISCONNECTED": "未连接",
    "RECONNECTING": "重连中",
    "NORMAL": "正常",
    "DEGRADED": "降级",
    "CIRCUIT": "熔断",
    "VENUE_HALT": "冻结",
    "RECONCILING": "对账中",
    "OFFLINE": "离线",
    "ACTIVE": "运行中",
    "IDLE": "空闲",
    "READY": "就绪",
    "NO_SNAPSHOT": "无快照",
    "MVP_ADAPTER": "MVP适配",
    "PLACEHOLDER": "预留位",
}


class DbReader:
    """Lightweight SQLite reader for Streamlit views and chat UI."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path

    def _today_utc_start(self) -> str:
        now = datetime.now(timezone.utc)
        return now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def get_today_monitor_counts(self) -> dict[str, Any]:
        today_start = self._today_utc_start()
        defaults: dict[str, Any] = {
            "order_count": 0,
            "cancel_count": 0,
            "fill_count": 0,
            "duplicate_count": 0,
        }
        try:
            with self._connect() as conn:
                field_map = {
                    "order_count": "order_count",
                    "cancel_count": "cancel_count",
                    "fill_count": "fill_count",
                    "duplicate_count": "duplicate_count",
                }
                for db_field, result_key in field_map.items():
                    row = conn.execute(
                        "SELECT MAX(current_value) AS value FROM monitor_log WHERE field = ? AND ts >= ?",
                        (db_field, today_start),
                    ).fetchone()
                    if row and row["value"] is not None:
                        defaults[result_key] = int(row["value"])
        except sqlite3.Error:
            pass
        return defaults

    def get_monitor_log(self, limit: int = 100) -> list[dict[str, Any]]:
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT * FROM monitor_log ORDER BY ts DESC LIMIT ?",
                    (limit,),
                ).fetchall()
                return [dict(row) for row in rows]
        except sqlite3.Error:
            return []

    def get_system_log(self, limit: int = 100) -> list[dict[str, Any]]:
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT * FROM system_log ORDER BY ts DESC LIMIT ?",
                    (limit,),
                ).fetchall()
                return [dict(row) for row in rows]
        except sqlite3.Error:
            return []

    def get_error_log(self, limit: int = 100) -> list[dict[str, Any]]:
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT * FROM error_log ORDER BY ts DESC LIMIT ?",
                    (limit,),
                ).fetchall()
                return [dict(row) for row in rows]
        except sqlite3.Error:
            return []

    def get_orders(self, limit: int = 100) -> list[dict[str, Any]]:
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT * FROM orders ORDER BY updated_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
                return [dict(row) for row in rows]
        except sqlite3.Error:
            return []

    def get_orders_by_status(self, status: str, limit: int = 100) -> list[dict[str, Any]]:
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT * FROM orders WHERE status = ? ORDER BY updated_at DESC LIMIT ?",
                    (status, limit),
                ).fetchall()
                return [dict(row) for row in rows]
        except sqlite3.Error:
            return []

    def get_latest_risk_state(self) -> str:
        return str(self.get_latest_risk_record().get("current_state") or "NORMAL")

    def get_latest_risk_record(self) -> dict[str, Any]:
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM risk_state_log ORDER BY id DESC LIMIT 1"
                ).fetchone()
                return dict(row) if row else {"current_state": "NORMAL", "reason": "bootstrap"}
        except sqlite3.Error:
            return {"current_state": "NORMAL", "reason": "bootstrap"}

    def get_risk_state_history(self, limit: int = 50) -> list[dict[str, Any]]:
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT * FROM risk_state_log ORDER BY id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
                return [dict(row) for row in rows]
        except sqlite3.Error:
            return []

    def get_today_monitor_alerts(self) -> list[dict[str, Any]]:
        today_start = self._today_utc_start()
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT * FROM monitor_log WHERE ts >= ? AND level IN ('WARNING', 'BREACH') ORDER BY ts DESC LIMIT 100",
                    (today_start,),
                ).fetchall()
                return [dict(row) for row in rows]
        except sqlite3.Error:
            return []

    def get_latest_connection_status(self) -> dict[str, Any]:
        defaults = {
            "status": "DISCONNECTED",
            "front_addr": "",
            "session_id": "",
            "ts": "",
            "detail": "",
        }
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM connection_log ORDER BY id DESC LIMIT 1"
                ).fetchone()
                return dict(row) if row else defaults
        except sqlite3.Error:
            return defaults

    def get_latest_account_info(self) -> dict[str, Any]:
        defaults = {
            "user_id": "",
            "broker_id": "",
            "trading_day": "",
            "available": 0.0,
            "margin": 0.0,
            "equity": 0.0,
        }
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM account_info ORDER BY id DESC LIMIT 1"
                ).fetchone()
                return dict(row) if row else defaults
        except sqlite3.Error:
            return defaults

    def get_positions(self) -> list[dict[str, Any]]:
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT * FROM positions ORDER BY symbol ASC"
                ).fetchall()
                return [dict(row) for row in rows]
        except sqlite3.Error:
            return []

    def get_connection_log(self, limit: int = 50) -> list[dict[str, Any]]:
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT * FROM connection_log ORDER BY ts DESC LIMIT ?",
                    (limit,),
                ).fetchall()
                return [dict(row) for row in rows]
        except sqlite3.Error:
            return []

    def get_audit_log(self, limit: int = 100) -> list[dict[str, Any]]:
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT * FROM audit_log ORDER BY timestamp DESC LIMIT ?",
                    (limit,),
                ).fetchall()
                return [dict(row) for row in rows]
        except sqlite3.Error:
            return []

    def get_portfolio_snapshot(self) -> dict[str, Any]:
        account = self.get_latest_account_info()
        positions = self.get_positions()
        unrealized_pnl = sum(float(pos.get("unrealized_pnl") or 0.0) for pos in positions)
        total_quantity = sum(abs(float(pos.get("quantity") or 0.0)) for pos in positions)
        symbols = sorted({str(pos.get("symbol")) for pos in positions if pos.get("symbol")})
        return {
            "account": account,
            "positions": positions,
            "unrealized_pnl": unrealized_pnl,
            "position_count": len(positions),
            "total_quantity": total_quantity,
            "symbols": symbols,
        }

    def get_reconciler_snapshot(self) -> dict[str, Any]:
        portfolio = self.get_portfolio_snapshot()
        risk = self.get_latest_risk_record()
        return {
            "last_checked_at": portfolio["account"].get("ts") or "",
            "position_count": portfolio["position_count"],
            "symbols": portfolio["symbols"],
            "risk_state": risk.get("current_state", "NORMAL"),
            "has_snapshot": bool(portfolio["positions"]),
        }

    def get_agent_statuses(self) -> list[dict[str, Any]]:
        connection = self.get_latest_connection_status()
        risk = self.get_latest_risk_record()
        account = self.get_latest_account_info()
        orders = self.get_orders(limit=25)
        positions = self.get_positions()
        pending_orders = sum(1 for order in orders if order.get("status") not in {"FILLED", "CANCELED", "REJECTED", "FAILED"})
        return [
            {
                "agent": AGENT_LABELS["Market_Data"],
                "status": STATUS_LABELS.get(connection.get("status", "DISCONNECTED"), connection.get("status", "DISCONNECTED")),
                "detail": connection.get("detail") or connection.get("front_addr") or "暂无行情连接快照",
            },
            {
                "agent": AGENT_LABELS["Risk_Governor"],
                "status": STATUS_LABELS.get(risk.get("current_state", "NORMAL"), risk.get("current_state", "NORMAL")),
                "detail": risk.get("reason") or "暂无风控原因",
            },
            {
                "agent": AGENT_LABELS["Order_Manager"],
                "status": STATUS_LABELS["ACTIVE"] if orders else STATUS_LABELS["IDLE"],
                "detail": f"待处理 {pending_orders} 笔 / 最近订单 {len(orders)} 笔",
            },
            {
                "agent": AGENT_LABELS["Strategy"],
                "status": STATUS_LABELS["MVP_ADAPTER"],
                "detail": "暂停/恢复指令当前以聊天回执方式落地。",
            },
            {
                "agent": AGENT_LABELS["News"],
                "status": STATUS_LABELS["PLACEHOLDER"],
                "detail": "资讯 Provider 已预留，实时新闻尚未接入。",
            },
            {
                "agent": AGENT_LABELS["Reconciler"],
                "status": STATUS_LABELS["READY"] if positions else STATUS_LABELS["NO_SNAPSHOT"],
                "detail": f"本地持仓 {len(positions)} 条，账户快照时间={account.get('ts', '') or '暂无'}",
            },
        ]

    def get_chat_messages(self, channel: str = "all", limit: int = 80) -> list[ChatMessage]:
        messages: list[ChatMessage] = []
        messages.extend(self._system_messages(limit))
        messages.extend(self._risk_messages(limit))
        messages.extend(self._monitor_messages(limit))
        messages.extend(self._error_messages(limit))
        messages.extend(self._order_messages(limit))
        messages.extend(self._audit_messages(limit))

        if channel != "all":
            messages = [message for message in messages if message.channel == channel]

        messages.sort(key=lambda item: (item.ts, item.id))
        if len(messages) > limit:
            messages = messages[-limit:]
        return messages

    def _system_messages(self, limit: int) -> list[ChatMessage]:
        messages: list[ChatMessage] = []
        for row in self.get_system_log(limit):
            event_type = str(row.get("event_type") or "SYSTEM")
            severity = "warning" if event_type in {"HALT", "SHUTDOWN"} else "info"
            messages.append(
                ChatMessage(
                    id=f"system-{row.get('id', row.get('ts', '0'))}",
                    channel="system",
                    agent_name=AGENT_LABELS["System"],
                    content=f"系统事件 {event_type}：{row.get('detail') or '无附加说明'}",
                    severity=severity,
                    ts=str(row.get("ts") or ""),
                    meta=row,
                    workflow_role="system",
                )
            )
        return messages

    def _risk_messages(self, limit: int) -> list[ChatMessage]:
        messages: list[ChatMessage] = []
        for row in self.get_risk_state_history(limit):
            state = str(row.get("current_state") or "NORMAL")
            severity = "critical" if state in {"CIRCUIT", "VENUE_HALT"} else "warning" if state == "DEGRADED" else "info"
            messages.append(
                ChatMessage(
                    id=f"risk-{row.get('id', row.get('state_changed_at', '0'))}",
                    channel="risk-alerts",
                    agent_name=AGENT_LABELS["Risk_Governor"],
                    content=f"风控状态切换为 {STATUS_LABELS.get(state, state)}（原因：{row.get('reason') or '无'}）",
                    severity=severity,
                    ts=str(row.get("state_changed_at") or row.get("ts") or ""),
                    meta=row,
                    workflow_role="risk",
                )
            )
        return messages

    def _monitor_messages(self, limit: int) -> list[ChatMessage]:
        messages: list[ChatMessage] = []
        for row in self.get_monitor_log(limit):
            level = str(row.get("level") or "INFO")
            severity = "critical" if level == "BREACH" else "warning" if level == "WARNING" else "info"
            messages.append(
                ChatMessage(
                    id=f"monitor-{row.get('id', row.get('ts', '0'))}",
                    channel="risk-alerts",
                    agent_name=AGENT_LABELS["Futures_Monitor"],
                    content=(
                        f"监控项 {row.get('field', 'unknown')} 当前值={row.get('current_value')}，"
                        f"阈值={row.get('limit_value')}，级别={level}"
                    ),
                    severity=severity,
                    ts=str(row.get("ts") or ""),
                    meta=row,
                    workflow_role="monitor",
                )
            )
        return messages

    def _error_messages(self, limit: int) -> list[ChatMessage]:
        messages: list[ChatMessage] = []
        for row in self.get_error_log(limit):
            messages.append(
                ChatMessage(
                    id=f"error-{row.get('id', row.get('ts', '0'))}",
                    channel="risk-alerts",
                    agent_name=AGENT_LABELS["Market_Data"],
                    content=f"CTP 错误 {row.get('error_id')}：{row.get('error_msg')}",
                    severity="critical",
                    ts=str(row.get("ts") or ""),
                    meta=row,
                    workflow_role="market-data",
                )
            )
        return messages

    def _order_messages(self, limit: int) -> list[ChatMessage]:
        messages: list[ChatMessage] = []
        severity_map = {
            "FILLED": "success",
            "CANCELED": "warning",
            "REJECTED": "critical",
            "FAILED": "critical",
            "PARTIALLY_FILLED": "warning",
        }
        for row in self.get_orders(limit):
            status = str(row.get("status") or "UNKNOWN")
            messages.append(
                ChatMessage(
                    id=f"order-{row.get('order_id', row.get('client_order_id', '0'))}",
                    channel="orders",
                    agent_name=AGENT_LABELS["Order_Manager"],
                    content=(
                        f"{row.get('symbol', 'UNKNOWN')} {row.get('side', '')} 数量={row.get('quantity')}，"
                        f"状态={status}"
                    ),
                    severity=severity_map.get(status, "info"),
                    ts=str(row.get("updated_at") or row.get("created_at") or ""),
                    meta=row,
                    workflow_role="execution",
                )
            )
        return messages

    def _audit_messages(self, limit: int) -> list[ChatMessage]:
        messages: list[ChatMessage] = []
        for row in self.get_audit_log(limit):
            messages.append(
                ChatMessage(
                    id=f"audit-{row.get('id', row.get('timestamp', '0'))}",
                    channel=str(row.get("channel") or "general"),
                    agent_name=str(row.get("agent_name") or "审计"),
                    content=str(row.get("content") or row.get("event_type") or "审计记录"),
                    severity=str(row.get("severity") or "info"),
                    ts=str(row.get("timestamp") or ""),
                    meta=row,
                    workflow_role="audit",
                )
            )
        return messages


def _dbreader_get_persisted_chat_messages(self, channel: str = "all", limit: int = 80):
    messages = []
    try:
        with self._connect() as conn:
            sql = "SELECT * FROM chat_messages"
            params = []
            if channel != "all":
                sql += " WHERE channel = ?"
                params.append(channel)
            sql += " ORDER BY created_at DESC LIMIT ?"
            params.append(limit)
            rows = conn.execute(sql, tuple(params)).fetchall()
            for row in reversed(rows):
                payload = dict(row)
                meta = payload.get("payload_json") or "{}"
                try:
                    meta_payload = __import__('json').loads(meta)
                except Exception:
                    meta_payload = {}
                messages.append(
                    ChatMessage(
                        id=str(payload.get("message_id")),
                        channel=str(payload.get("channel") or "general"),
                        agent_name=str(payload.get("sender_id") or payload.get("agent_role") or "系统"),
                        content=str(payload.get("content") or ""),
                        severity=str(meta_payload.get("severity") or "info"),
                        ts=str(payload.get("created_at") or ""),
                        meta=meta_payload,
                        workflow_role=payload.get("agent_role"),
                        author_kind=str(payload.get("author_kind") or "system"),
                    )
                )
    except sqlite3.Error:
        return []
    return messages


def _dbreader_get_live_chat_messages(self, channel: str = "all", limit: int = 80):
    persisted = self.get_persisted_chat_messages(channel=channel, limit=limit)
    derived = self.get_chat_messages(channel=channel, limit=limit)
    if persisted:
        combined = derived + persisted
        combined.sort(key=lambda item: (item.ts, item.id))
        return combined[-limit:]
    return derived


def _dbreader_get_open_tasks(self, limit: int = 20):
    try:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM tasks WHERE status NOT IN ('COMPLETED', 'FAILED', 'CANCELED', 'SUPERSEDED') ORDER BY updated_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]
    except sqlite3.Error:
        return []


def _dbreader_get_pending_approvals(self, limit: int = 20):
    try:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM approval_requests WHERE status = 'PENDING' ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]
    except sqlite3.Error:
        return []


def _dbreader_get_recent_daily_summaries(self, limit: int = 3):
    try:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM daily_summaries ORDER BY trading_day DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]
    except sqlite3.Error:
        return []


def _dbreader_get_recent_daily_fact_snapshots(self, limit: int = 3):
    try:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM daily_fact_snapshots ORDER BY trading_day DESC LIMIT ?",
                (limit,),
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                for key in list(item.keys()):
                    if key.endswith('_json') and isinstance(item[key], str):
                        try:
                            item[key] = __import__('json').loads(item[key])
                        except Exception:
                            pass
                result.append(item)
            return result
    except sqlite3.Error:
        return []


def _dbreader_get_system_mode(self):
    risk_state = str(self.get_latest_risk_record().get("current_state") or "NORMAL")
    return {
        "NORMAL": "NORMAL",
        "DEGRADED": "CAUTION",
        "CIRCUIT": "RESTRICTED",
        "VENUE_HALT": "HALTED",
        "OFFLINE": "PROTECT_ONLY",
        "RECONCILING": "RECONCILING",
    }.get(risk_state, "NORMAL")


DbReader.get_persisted_chat_messages = _dbreader_get_persisted_chat_messages
DbReader.get_live_chat_messages = _dbreader_get_live_chat_messages
DbReader.get_open_tasks = _dbreader_get_open_tasks
DbReader.get_pending_approvals = _dbreader_get_pending_approvals
DbReader.get_recent_daily_summaries = _dbreader_get_recent_daily_summaries
DbReader.get_recent_daily_fact_snapshots = _dbreader_get_recent_daily_fact_snapshots
DbReader.get_system_mode = _dbreader_get_system_mode
