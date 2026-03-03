-- Trading Agents v3 — Database Schema
PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=5000;

-- ============================================
-- Orders
-- ============================================
CREATE TABLE IF NOT EXISTS orders (
    order_id            TEXT PRIMARY KEY,
    client_order_id     TEXT UNIQUE NOT NULL,
    symbol              TEXT NOT NULL,
    venue               TEXT NOT NULL,
    side                TEXT NOT NULL,
    quantity            REAL NOT NULL,
    price               REAL,
    status              TEXT NOT NULL DEFAULT 'PENDING_SEND',
    strategy_id         TEXT,
    created_at          TIMESTAMP NOT NULL,
    updated_at          TIMESTAMP NOT NULL,
    filled_quantity     REAL DEFAULT 0,
    filled_price        REAL DEFAULT 0
);

-- ============================================
-- Outbox Orders
-- ============================================
CREATE TABLE IF NOT EXISTS outbox_orders (
    event_id            TEXT PRIMARY KEY,
    aggregate_id        TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    payload             TEXT NOT NULL,
    idempotency_key     TEXT UNIQUE,
    status              TEXT DEFAULT 'NEW',
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sent_at             TIMESTAMP,
    retry_count         INTEGER DEFAULT 0,
    max_retries         INTEGER DEFAULT 3,
    error_message       TEXT
);

CREATE INDEX IF NOT EXISTS idx_outbox_status
    ON outbox_orders(status, created_at);

-- ============================================
-- Positions
-- ============================================
CREATE TABLE IF NOT EXISTS positions (
    symbol              TEXT NOT NULL,
    venue               TEXT NOT NULL,
    side                TEXT NOT NULL,
    quantity            REAL NOT NULL DEFAULT 0,
    entry_price         REAL NOT NULL DEFAULT 0,
    unrealized_pnl      REAL DEFAULT 0,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, venue, side)
);

-- ============================================
-- Event Dedup
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
-- Stream Checkpoints
-- ============================================
CREATE TABLE IF NOT EXISTS stream_checkpoints (
    stream_id           TEXT PRIMARY KEY,
    last_seq            INTEGER NOT NULL DEFAULT 0,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Risk State Log
-- ============================================
CREATE TABLE IF NOT EXISTS risk_state_log (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    current_state       TEXT NOT NULL,
    previous_state      TEXT,
    state_changed_at    TIMESTAMP NOT NULL,
    reason              TEXT,
    metadata            TEXT
);

-- ============================================
-- Audit Log
-- ============================================
CREATE TABLE IF NOT EXISTS audit_log (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_name          TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    content             TEXT,
    severity            TEXT DEFAULT 'info',
    channel             TEXT DEFAULT 'general',
    timestamp           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_audit_channel_ts
    ON audit_log(channel, timestamp);

-- ============================================
-- Risk Windows
-- ============================================
CREATE TABLE IF NOT EXISTS risk_windows (
    window_id           TEXT PRIMARY KEY,
    start_ts            TIMESTAMP NOT NULL,
    end_ts              TIMESTAMP NOT NULL,
    severity            TEXT NOT NULL,
    scope_markets       TEXT,
    scope_venues        TEXT,
    scope_symbols       TEXT,
    source              TEXT,
    action              TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
