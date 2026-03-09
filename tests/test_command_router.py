from __future__ import annotations

import sqlite3
from pathlib import Path

from dashboard.backend.command_router import (
    handle,
    handle_chat_command,
    list_command_suggestions,
)
from dashboard.streamlit_mvp.db_reader import DbReader


class StubOrderManager:
    def __init__(self) -> None:
        self.cancel_all_calls = 0
        self.flatten_calls: list[str | None] = []

    async def cancel_all(self):
        self.cancel_all_calls += 1
        return {"ok": True}

    async def flatten(self, symbol: str | None = None):
        self.flatten_calls.append(symbol)
        return [{"ok": True, "symbol": symbol}]


class StubRiskGovernor:
    def __init__(self) -> None:
        self.transitions: list[tuple[str, str]] = []
        self.resumed = False

    def transition(self, state: str, reason: str, metadata: dict):
        self.transitions.append((state, reason))
        return True

    def resume(self):
        self.resumed = True


SCHEMA_PATH = Path(__file__).resolve().parents[1] / "db" / "schema.sql"


def init_db(tmp_path: Path) -> DbReader:
    db_path = tmp_path / "chat.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    conn.execute(
        "INSERT INTO risk_state_log (current_state, previous_state, state_changed_at, reason) VALUES (?, ?, ?, ?)",
        ("DEGRADED", "NORMAL", "2026-03-09T09:00:00+00:00", "threshold_warning"),
    )
    conn.execute(
        "INSERT INTO connection_log (ts, status, front_addr, session_id, detail) VALUES (?, ?, ?, ?, ?)",
        ("2026-03-09T09:01:00+00:00", "CONNECTED", "tcp://front", "session-1", "healthy"),
    )
    conn.execute(
        "INSERT INTO account_info (ts, user_id, broker_id, trading_day, available, margin, equity) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("2026-03-09T09:02:00+00:00", "u1", "9999", "20260309", 1000.0, 200.0, 1200.0),
    )
    conn.execute(
        "INSERT INTO positions (symbol, venue, side, quantity, entry_price, unrealized_pnl, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("rb2510", "CTP", "LONG", 2.0, 3500.0, 88.0, "2026-03-09T09:02:00+00:00"),
    )
    conn.commit()
    conn.close()
    return DbReader(str(db_path))


def test_handle_cancel_all_prefers_order_manager() -> None:
    order_manager = StubOrderManager()

    result = handle("CANCEL_ALL", order_manager=order_manager)

    assert result == {"ok": True, "command": "CANCEL_ALL"}
    assert order_manager.cancel_all_calls == 1


def test_handle_flatten_forwards_symbol_to_order_manager() -> None:
    order_manager = StubOrderManager()

    result = handle("FLATTEN", reason="BTCUSDT", order_manager=order_manager)

    assert result == {"ok": True, "command": "FLATTEN", "symbol": "BTCUSDT"}
    assert order_manager.flatten_calls == ["BTCUSDT"]


def test_handle_chat_command_returns_latest_risk_state(tmp_path: Path) -> None:
    reader = init_db(tmp_path)

    reply = handle_chat_command("@Risk_Governor /state", db_reader=reader)

    assert reply.ok is True
    assert reply.channel == "risk-alerts"
    assert "DEGRADED" in reply.content
    assert reply.executed is True


def test_handle_chat_command_flattens_symbol(tmp_path: Path) -> None:
    reader = init_db(tmp_path)
    order_manager = StubOrderManager()

    reply = handle_chat_command(
        "@Order_Manager /flatten BTCUSDT",
        db_reader=reader,
        order_manager=order_manager,
    )

    assert reply.ok is True
    assert reply.channel == "orders"
    assert order_manager.flatten_calls == ["BTCUSDT"]


def test_handle_chat_command_news_latest_is_placeholder(tmp_path: Path) -> None:
    reader = init_db(tmp_path)

    reply = handle_chat_command("@News /latest", db_reader=reader)

    assert reply.ok is True
    assert reply.executed is False
    assert reply.channel == "news"
    assert "预留" in reply.content


def test_handle_chat_command_circuit_uses_risk_governor(tmp_path: Path) -> None:
    reader = init_db(tmp_path)
    risk_governor = StubRiskGovernor()

    reply = handle_chat_command(
        "@Risk_Governor /circuit",
        db_reader=reader,
        risk_governor=risk_governor,
    )

    assert reply.ok is True
    assert risk_governor.transitions == [("CIRCUIT", "manual_circuit")]


def test_list_command_suggestions_filters_prefix() -> None:
    results = list_command_suggestions("@Risk_G")

    assert any(item.startswith("@Risk_Governor") for item in results)
