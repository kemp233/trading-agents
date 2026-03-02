-- Trading Agents v3 — Database Schema
-- Phase 1: SQLite (WAL mode)
-- Phase 2: Migrate to PostgreSQL

PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=5000;

-- ============================================
-- Orders (订单主表)
-- ============================================
CREATE TABLE IF NOT EXISTS orders (
    order_id            TEXT PRIMARY KEY,
    client_order_id     TEXT UNIQUE NOT NULL,
    symbol              TEXT NOT NULL,
    side                TEXT NOT NULL,  -- 'BUY' | 'SELL'
    order_type          TEXT NOT NULL,  -- 'MARKET' | 'LIMIT' | 'STOP'
    quantity            REAL NOT NULL,
    price               REAL,
    status              TEXT NOT NULL DEFAULT 'PENDING_SEND',
    -- PENDING_SEND → SENT → PARTIALLY_FILLED → FILLED | CANCELED | REJECTED | FAILED
    venue               TEXT NOT NULL,  -- 'binance' | 'ctp'
    venue_order_id      TEXT,
    filled_quantity     REAL DEFAULT 0,
    filled_price        REAL DEFAULT 0,
    idempotency_key     TEXT UNIQUE,
    strategy_id         TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Outbox Orders (Outbox 原子下单)
-- ============================================
CREATE TABLE IF NOT EXISTS outbox_orders (
    event_id            TEXT PRIMARY KEY,
    aggregate_id        TEXT NOT NULL,  -- order_id
    event_type          TEXT NOT NULL,  -- 'OrderSubmit' | 'OrderCancel'
    payload             TEXT NOT NULL,  -- JSON
    idempotency_key     TEXT UNIQUE,
    status              TEXT DEFAULT 'NEW',  -- NEW → SENT → CONFIRMED | FAILED | RETRY
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sent_at             TIMESTAMP,
    retry_count         INTEGER DEFAULT 0,
    max_retries         INTEGER DEFAULT 3,
    error_message       TEXT
);

CREATE INDEX IF NOT EXISTS idx_outbox_status 
    ON outbox_orders(status, created_at);

-- ============================================
-- Positions (持仓)
-- ============================================
CREATE TABLE IF NOT EXISTS positions (
    symbol              TEXT NOT NULL,
    venue               TEXT NOT NULL,
    side                TEXT NOT NULL,  -- 'LONG' | 'SHORT'
    quantity            REAL NOT NULL DEFAULT 0,
    entry_price         REAL NOT NULL DEFAULT 0,
    unrealized_pnl      REAL DEFAULT 0,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, venue, side)
);

-- ============================================
-- Event Dedup (事件去重表)
-- ============================================
CREATE TABLE IF NOT EXISTS processed_events (
    stream_id           TEXT NOT NULL,
    stream_seq          INTEGER NOT NULL,
    idempotency_key     TEXT NOT NULL,
    processed_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (stream_id, stream_seq)
);

CREATE INDEX IF NOT EXISTS idx_dedup_key 
    ON processed_events(idempotency_key);

-- ============================================
-- Stream Checkpoints (流检查点)
-- ============================================
CREATE TABLE IF NOT EXISTS stream_checkpoints (
    stream_id           TEXT PRIMARY KEY,
    last_seq            INTEGER NOT NULL DEFAULT 0,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Risk State Log (风控状态变更日志)
-- ============================================
CREATE TABLE IF NOT EXISTS risk_state_log (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    from_state          TEXT NOT NULL,
    to_state            TEXT NOT NULL,
    reason              TEXT,
    timestamp           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Audit Log (审计日志)
-- ============================================
CREATE TABLE IF NOT EXISTS audit_log (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_name          TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    content             TEXT,
    severity            TEXT DEFAULT 'info',  -- info | warning | critical | success
    channel             TEXT DEFAULT 'general',
    timestamp           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_audit_channel_ts 
    ON audit_log(channel, timestamp);

-- ============================================
-- Risk Windows (风险窗口)
-- ============================================
CREATE TABLE IF NOT EXISTS risk_windows (
    window_id           TEXT PRIMARY KEY,
    start_ts            TIMESTAMP NOT NULL,
    end_ts              TIMESTAMP NOT NULL,
    severity            TEXT NOT NULL,  -- LOW | MEDIUM | HIGH | CRITICAL
    scope_markets       TEXT,  -- JSON array
    scope_venues        TEXT,  -- JSON array
    scope_symbols       TEXT,  -- JSON array
    source              TEXT,
    action              TEXT,  -- REDUCE_SIZE | PAUSE_NEW | FLATTEN_AND_HALT
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
