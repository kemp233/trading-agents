from __future__ import annotations

import sqlite3
from pathlib import Path

from dashboard.streamlit_mvp.db_reader import DbReader


SCHEMA_PATH = Path(__file__).resolve().parents[1] / "db" / "schema.sql"


def init_reader(tmp_path: Path) -> DbReader:
    db_path = tmp_path / "reader.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    conn.execute(
        "INSERT INTO system_log (ts, event_type, detail) VALUES (?, ?, ?)",
        ("2026-03-09T10:00:00+00:00", "STARTUP", "boot"),
    )
    conn.execute(
        "INSERT INTO risk_state_log (current_state, previous_state, state_changed_at, reason) VALUES (?, ?, ?, ?)",
        ("CIRCUIT", "DEGRADED", "2026-03-09T10:01:00+00:00", "manual_circuit"),
    )
    conn.execute(
        "INSERT INTO error_log (ts, error_id, error_msg, context) VALUES (?, ?, ?, ?)",
        ("2026-03-09T10:02:00+00:00", 7, "login failed", "ctp"),
    )
    conn.execute(
        "INSERT INTO orders (order_id, client_order_id, symbol, venue, side, quantity, price, status, strategy_id, created_at, updated_at, filled_quantity, filled_price) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "ord-1",
            "client-1",
            "rb2510",
            "CTP",
            "BUY",
            1.0,
            3500.0,
            "FILLED",
            None,
            "2026-03-09T10:03:00+00:00",
            "2026-03-09T10:04:00+00:00",
            1.0,
            3500.0,
        ),
    )
    conn.commit()
    conn.close()
    return DbReader(str(db_path))


def test_get_chat_messages_maps_risk_and_error_channels(tmp_path: Path) -> None:
    reader = init_reader(tmp_path)

    messages = reader.get_chat_messages(channel="risk-alerts", limit=10)

    assert any(message.agent_name == "风控治理" for message in messages)
    assert any(message.agent_name == "行情感知" for message in messages)
    assert any(message.severity == "critical" for message in messages)


def test_get_chat_messages_maps_system_and_orders(tmp_path: Path) -> None:
    reader = init_reader(tmp_path)

    system_messages = reader.get_chat_messages(channel="system", limit=10)
    order_messages = reader.get_chat_messages(channel="orders", limit=10)

    assert any(message.agent_name == "系统" for message in system_messages)
    assert any(message.agent_name == "执行管理" for message in order_messages)
    assert any(message.severity == "success" for message in order_messages)
