from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiosqlite
import pytest

from agents.strategy import StrategyAgent
from agents.technical_analysis import TechnicalAnalysisAgent
from core.event_bus import EventBus
from core.event_envelope import EventEnvelope, EventType
from core.state_writer import StateWriter


async def _init_sqlite_db(db_path: Path) -> None:
    schema_path = Path(__file__).resolve().parents[1] / "db" / "schema.sql"
    schema_sql = schema_path.read_text(encoding="utf-8")
    async with aiosqlite.connect(str(db_path)) as db:
        await db.executescript(schema_sql)
        await db.commit()


async def _make_runtime(tmp_path: Path) -> tuple[EventBus, StateWriter]:
    db_path = tmp_path / "issue8.sqlite3"
    await _init_sqlite_db(db_path)
    writer = StateWriter(str(db_path))
    await writer.start()
    bus = EventBus(writer)
    await bus.start()
    return bus, writer


def _market_tick(symbol: str, price: float, seq: int, event_ts: datetime) -> EventEnvelope:
    return EventEnvelope.make(
        EventType.MARKET_TICK,
        symbol,
        {
            "symbol": symbol,
            "last_price": price,
            "open_price": price,
            "high_price": price,
            "low_price": price,
        },
        stream_seq=seq,
        event_ts=event_ts,
    )


def _ta_signal(symbol: str, cross: str, seq: int, event_ts: datetime) -> EventEnvelope:
    return EventEnvelope.make(
        EventType.TA_SIGNAL,
        symbol,
        {
            "symbol": symbol,
            "last_price": 100.0,
            "indicators": {
                "rsi": 55.0,
                "macd": 1.2,
                "macd_signal": 1.0,
                "macd_hist": 0.2,
                "bollinger_upper": 110.0,
                "bollinger_mid": 100.0,
                "bollinger_lower": 90.0,
                "atr": 2.0,
                "ema_fast": 101.0,
                "ema_slow": 100.0,
            },
            "derived": {"ema_cross": cross},
            "source_event_id": f"source-{seq}",
        },
        stream_seq=seq,
        event_ts=event_ts,
    )


@pytest.mark.asyncio
async def test_ta_agent_waits_for_warmup_before_publishing(tmp_path: Path) -> None:
    bus, writer = await _make_runtime(tmp_path)
    agent = TechnicalAnalysisAgent(bus)
    ta_signals: list[EventEnvelope] = []
    bus.subscribe(EventType.TA_SIGNAL, ta_signals.append)
    await agent.start()

    start_ts = datetime(2026, 3, 8, tzinfo=timezone.utc)
    try:
        for seq in range(TechnicalAnalysisAgent.WARMUP_TICKS - 1):
            published = await bus.publish(_market_tick("BTCUSDT", 100.0 + seq, seq, start_ts + timedelta(seconds=seq)))
            assert published is True
        assert ta_signals == []
    finally:
        await agent.stop()
        await writer.stop()


@pytest.mark.asyncio
async def test_ta_agent_publishes_signal_with_dual_timestamps_and_monotonic_seq(tmp_path: Path) -> None:
    bus, writer = await _make_runtime(tmp_path)
    agent = TechnicalAnalysisAgent(bus)
    ta_signals: list[EventEnvelope] = []
    bus.subscribe(EventType.TA_SIGNAL, ta_signals.append)
    await agent.start()

    start_ts = datetime(2026, 3, 8, tzinfo=timezone.utc)
    try:
        total_ticks = TechnicalAnalysisAgent.WARMUP_TICKS + 1
        for seq in range(total_ticks):
            price = 100.0 + (seq * 0.5)
            published = await bus.publish(_market_tick("BTCUSDT", price, seq, start_ts + timedelta(seconds=seq)))
            assert published is True

        assert [signal.stream_seq for signal in ta_signals] == [0, 1]
        first_signal = ta_signals[0]
        assert first_signal.event_type == EventType.TA_SIGNAL
        assert first_signal.stream_id == f"{EventType.TA_SIGNAL}:BTCUSDT"
        assert first_signal.event_ts == start_ts + timedelta(seconds=TechnicalAnalysisAgent.WARMUP_TICKS - 1)
        assert first_signal.event_ts.tzinfo is not None
        assert first_signal.recv_ts.tzinfo is not None
        assert first_signal.recv_ts >= first_signal.event_ts
        assert first_signal.payload["symbol"] == "BTCUSDT"
        assert first_signal.payload["source_event_id"]
        indicators = first_signal.payload["indicators"]
        assert set(indicators) == {
            "rsi",
            "macd",
            "macd_signal",
            "macd_hist",
            "bollinger_upper",
            "bollinger_mid",
            "bollinger_lower",
            "atr",
            "ema_fast",
            "ema_slow",
        }
        assert first_signal.payload["derived"]["ema_cross"] in {"golden", "death", "none"}
    finally:
        await agent.stop()
        await writer.stop()


@pytest.mark.asyncio
async def test_strategy_agent_generates_buy_sell_and_deduplicates_same_direction(tmp_path: Path) -> None:
    bus, writer = await _make_runtime(tmp_path)
    strategy = StrategyAgent(bus, config={"venue": "binance", "default_quantity": "0.005"})
    trade_intents: list[EventEnvelope] = []
    bus.subscribe(EventType.TRADE_INTENT, trade_intents.append)
    await strategy.start()

    start_ts = datetime(2026, 3, 8, tzinfo=timezone.utc)
    try:
        assert await bus.publish(_ta_signal("BTCUSDT", "golden", 0, start_ts)) is True
        assert await bus.publish(_ta_signal("BTCUSDT", "golden", 1, start_ts + timedelta(seconds=1))) is True
        assert await bus.publish(_ta_signal("BTCUSDT", "death", 2, start_ts + timedelta(seconds=2))) is True

        assert len(trade_intents) == 2
        assert [intent.payload["side"] for intent in trade_intents] == ["BUY", "SELL"]
        first_intent = trade_intents[0]
        assert first_intent.payload["order_type"] == "MARKET"
        assert first_intent.payload["quantity"] == "0.005"
        assert first_intent.payload["time_in_force"] == "IOC"
        assert first_intent.payload["strategy_id"] == "ema_crossover_v1"
        assert first_intent.payload["client_order_id"].startswith("ema-crossover-v1-BTCUSDT-000000-buy")
    finally:
        await strategy.stop()
        await writer.stop()


@pytest.mark.asyncio
async def test_strategy_pause_resume_updates_state_without_backfilling(tmp_path: Path) -> None:
    bus, writer = await _make_runtime(tmp_path)
    strategy = StrategyAgent(bus)
    trade_intents: list[EventEnvelope] = []
    bus.subscribe(EventType.TRADE_INTENT, trade_intents.append)
    await strategy.start()

    start_ts = datetime(2026, 3, 8, tzinfo=timezone.utc)
    try:
        assert await strategy.handle_command("/pause", "") == {"ok": True, "paused": True}
        assert await bus.publish(_ta_signal("BTCUSDT", "golden", 0, start_ts)) is True
        assert trade_intents == []

        assert await strategy.handle_command("/resume", "") == {"ok": True, "paused": False}
        assert await bus.publish(_ta_signal("BTCUSDT", "golden", 1, start_ts + timedelta(seconds=1))) is True
        assert trade_intents == []

        assert await bus.publish(_ta_signal("BTCUSDT", "death", 2, start_ts + timedelta(seconds=2))) is True
        assert await bus.publish(_ta_signal("BTCUSDT", "golden", 3, start_ts + timedelta(seconds=3))) is True
        assert [intent.payload["side"] for intent in trade_intents] == ["SELL", "BUY"]

        listed = await strategy.handle_command("/list", "")
        assert listed == {
            "strategies": ["ema_crossover_v1"],
            "active": "ema_crossover_v1",
            "paused": False,
        }
    finally:
        await strategy.stop()
        await writer.stop()


@pytest.mark.asyncio
async def test_market_tick_to_ta_signal_to_trade_intent_end_to_end(tmp_path: Path) -> None:
    bus, writer = await _make_runtime(tmp_path)
    ta_agent = TechnicalAnalysisAgent(bus)
    strategy = StrategyAgent(bus)
    ta_signals: list[EventEnvelope] = []
    trade_intents: list[EventEnvelope] = []
    bus.subscribe(EventType.TA_SIGNAL, ta_signals.append)
    bus.subscribe(EventType.TRADE_INTENT, trade_intents.append)
    await ta_agent.start()
    await strategy.start()

    start_ts = datetime(2026, 3, 8, tzinfo=timezone.utc)
    prices = [200.0 - seq for seq in range(50)] + [151.0 + seq for seq in range(50)]
    try:
        for seq, price in enumerate(prices):
            published = await bus.publish(_market_tick("BTCUSDT", price, seq, start_ts + timedelta(seconds=seq)))
            assert published is True

        assert ta_signals
        assert trade_intents
        latest_intent = trade_intents[-1]
        assert latest_intent.event_type == EventType.TRADE_INTENT
        assert latest_intent.payload["symbol"] == "BTCUSDT"
        assert latest_intent.payload["order_type"] == "MARKET"
        assert latest_intent.event_ts.tzinfo is not None
        assert latest_intent.recv_ts.tzinfo is not None
        assert latest_intent.event_ts <= latest_intent.recv_ts
    finally:
        await strategy.stop()
        await ta_agent.stop()
        await writer.stop()
