from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


class DbReader:
    """轻量 SQLite 只读查询模块，供所有 Streamlit 页面调用（同步，非 async）。"""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path

    def _today_utc_start(self) -> str:
        """返回今日 00:00 UTC 的 ISO8601 字符串。"""
        now = datetime.now(timezone.utc)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return today.isoformat()

    def get_today_monitor_counts(self) -> dict[str, Any]:
        """返回今日最新 order_count/cancel_count 等（从 monitor_log 聚合）。"""
        today_start = self._today_utc_start()
        defaults: dict[str, Any] = {
            "order_count": 0,
            "cancel_count": 0,
            "fill_count": 0,
            "duplicate_count": 0,
        }
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                # 取今日各字段最新（最大）current_value 作为累计值
                field_map = {
                    "order_count": "order_count",
                    "cancel_count": "cancel_count",
                    "fill_count": "fill_count",
                    "duplicate_count": "duplicate_count",
                }
                for db_field, result_key in field_map.items():
                    cur = conn.execute(
                        "SELECT MAX(current_value) as v FROM monitor_log "
                        "WHERE field = ? AND ts >= ?",
                        (db_field, today_start),
                    )
                    row = cur.fetchone()
                    if row and row["v"] is not None:
                        defaults[result_key] = int(row["v"])
        except Exception as exc:
            logger.error("get_today_monitor_counts error: %s", exc)
        return defaults

    def get_monitor_log(self, limit: int = 100) -> list[dict]:
        """读取 monitor_log 表最近 N 条记录。"""
        rows: list[dict] = []
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.execute(
                    "SELECT * FROM monitor_log ORDER BY ts DESC LIMIT ?",
                    (limit,),
                )
                rows = [dict(r) for r in cur.fetchall()]
        except Exception as exc:
            logger.error("get_monitor_log error: %s", exc)
        return rows

    def get_system_log(self, limit: int = 100) -> list[dict]:
        """读取 system_log 表最近 N 条记录。"""
        rows: list[dict] = []
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.execute(
                    "SELECT * FROM system_log ORDER BY ts DESC LIMIT ?",
                    (limit,),
                )
                rows = [dict(r) for r in cur.fetchall()]
        except Exception as exc:
            logger.error("get_system_log error: %s", exc)
        return rows

    def get_error_log(self, limit: int = 100) -> list[dict]:
        """读取 error_log 表最近 N 条记录。"""
        rows: list[dict] = []
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.execute(
                    "SELECT * FROM error_log ORDER BY ts DESC LIMIT ?",
                    (limit,),
                )
                rows = [dict(r) for r in cur.fetchall()]
        except Exception as exc:
            logger.error("get_error_log error: %s", exc)
        return rows

    def get_orders(self, limit: int = 100) -> list[dict]:
        """读取 orders 表最近 N 条记录。"""
        rows: list[dict] = []
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.execute(
                    "SELECT * FROM orders ORDER BY updated_at DESC LIMIT ?",
                    (limit,),
                )
                rows = [dict(r) for r in cur.fetchall()]
        except Exception as exc:
            logger.error("get_orders error: %s", exc)
        return rows

    def get_orders_by_status(self, status: str, limit: int = 100) -> list[dict]:
        """读取指定状态的 orders。"""
        rows: list[dict] = []
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.execute(
                    "SELECT * FROM orders WHERE status = ? ORDER BY updated_at DESC LIMIT ?",
                    (status, limit),
                )
                rows = [dict(r) for r in cur.fetchall()]
        except Exception as exc:
            logger.error("get_orders_by_status error: %s", exc)
        return rows

    def get_latest_risk_state(self) -> str:
        """返回最新风控状态，如 'NORMAL' / 'DEGRADED' / 'VENUE_HALT'。"""
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.execute(
                    "SELECT current_state FROM risk_state_log ORDER BY id DESC LIMIT 1"
                )
                row = cur.fetchone()
                if row:
                    return str(row["current_state"])
        except Exception as exc:
            logger.error("get_latest_risk_state error: %s", exc)
        return "NORMAL"

    def get_today_monitor_alerts(self) -> list[dict]:
        """返回今日 monitor_log 中的 WARNING/BREACH 记录。"""
        today_start = self._today_utc_start()
        rows: list[dict] = []
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.execute(
                    "SELECT * FROM monitor_log WHERE ts >= ? AND level IN ('WARNING', 'BREACH') "
                    "ORDER BY ts DESC LIMIT 100",
                    (today_start,),
                )
                rows = [dict(r) for r in cur.fetchall()]
        except Exception as exc:
            logger.error("get_today_monitor_alerts error: %s", exc)
        return rows

    def get_latest_connection_status(self) -> dict:
        """返回最新一条 connection_log 记录。"""
        defaults: dict = {
            "status": "DISCONNECTED",
            "front_addr": "",
            "session_id": "",
            "ts": "",
        }
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.execute(
                    "SELECT * FROM connection_log ORDER BY id DESC LIMIT 1"
                )
                row = cur.fetchone()
                if row:
                    return dict(row)
        except Exception as exc:
            logger.error("get_latest_connection_status error: %s", exc)
        return defaults

    def get_latest_account_info(self) -> dict:
        """返回最新一条 account_info 记录。"""
        defaults: dict = {
            "user_id": "",
            "broker_id": "",
            "trading_day": "",
            "available": 0.0,
            "margin": 0.0,
            "equity": 0.0,
        }
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.execute(
                    "SELECT * FROM account_info ORDER BY id DESC LIMIT 1"
                )
                row = cur.fetchone()
                if row:
                    return dict(row)
        except Exception as exc:
            logger.error("get_latest_account_info error: %s", exc)
        return defaults

    def get_positions(self) -> list[dict]:
        """返回所有持仓记录，按 symbol ASC 排序。"""
        rows: list[dict] = []
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.execute(
                    "SELECT * FROM positions ORDER BY symbol ASC"
                )
                rows = [dict(r) for r in cur.fetchall()]
        except Exception as exc:
            logger.error("get_positions error: %s", exc)
        return rows

    def get_connection_log(self, limit: int = 50) -> list[dict]:
        """读取 connection_log 最近 N 条记录。"""
        rows: list[dict] = []
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.execute(
                    "SELECT * FROM connection_log ORDER BY ts DESC LIMIT ?",
                    (limit,),
                )
                rows = [dict(r) for r in cur.fetchall()]
        except Exception as exc:
            logger.error("get_connection_log error: %s", exc)
        return rows
