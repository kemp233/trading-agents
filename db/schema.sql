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

-- ============================================
-- Issue #14: Monitor Log (阈值预警记录)
-- ============================================
CREATE TABLE IF NOT EXISTS monitor_log (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ts                  TEXT NOT NULL,              -- ISO8601 UTC
    field               TEXT NOT NULL,              -- 触发阈值的字段名，如 "order_count"
    current_value       INTEGER NOT NULL,
    limit_value         INTEGER NOT NULL,
    level               TEXT NOT NULL               -- "WARNING" 或 "BREACH"
);

CREATE INDEX IF NOT EXISTS idx_monitor_log_ts
    ON monitor_log(ts);

-- ============================================
-- Issue #14: System Log (启停/异常事件)
-- ============================================
CREATE TABLE IF NOT EXISTS system_log (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ts                  TEXT NOT NULL,              -- ISO8601 UTC
    event_type          TEXT NOT NULL,              -- 如 "STARTUP", "SHUTDOWN", "HALT", "RESUME"
    detail              TEXT                        -- 可选附加说明
);

CREATE INDEX IF NOT EXISTS idx_system_log_ts
    ON system_log(ts);

-- ============================================
-- Issue #14: Error Log (CTP 错误回调)
-- ============================================
CREATE TABLE IF NOT EXISTS error_log (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ts                  TEXT NOT NULL,              -- ISO8601 UTC
    error_id            INTEGER NOT NULL,           -- CTP ErrorID
    error_msg           TEXT NOT NULL,              -- 格式化后的错误描述
    context             TEXT                        -- 发生错误时的上下文，如 "submit_order:rb2510"
);

CREATE INDEX IF NOT EXISTS idx_error_log_ts
    ON error_log(ts);

-- ============================================
-- Issue #15: Connection Log (CTP 连接状态)
-- ============================================
CREATE TABLE IF NOT EXISTS connection_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              TEXT NOT NULL,          -- ISO8601 UTC
    status          TEXT NOT NULL,          -- "CONNECTED" / "DISCONNECTED" / "RECONNECTING"
    front_addr      TEXT,
    session_id      TEXT,
    detail          TEXT
);

CREATE INDEX IF NOT EXISTS idx_connection_log_ts
    ON connection_log(ts);

-- ============================================
-- Issue #15: Account Info (CTP 账户资金快照)
-- ============================================
CREATE TABLE IF NOT EXISTS account_info (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              TEXT NOT NULL,          -- ISO8601 UTC
    user_id         TEXT,
    broker_id       TEXT,
    trading_day     TEXT,
    available       REAL DEFAULT 0,
    margin          REAL DEFAULT 0,
    equity          REAL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_account_info_ts
    ON account_info(ts);

-- ============================================
-- Issue #9 Upgrade: ChatOps Raw Messages
-- ============================================
CREATE TABLE IF NOT EXISTS chat_messages (
    message_id           TEXT PRIMARY KEY,
    channel              TEXT NOT NULL,
    thread_id            TEXT,
    author_kind          TEXT NOT NULL DEFAULT 'system',
    sender_id            TEXT,
    agent_role           TEXT,
    message_type         TEXT NOT NULL DEFAULT 'plain',
    visibility           TEXT NOT NULL DEFAULT 'channel',
    task_id              TEXT,
    workflow_run_id      TEXT,
    content              TEXT NOT NULL,
    payload_json         TEXT,
    created_at           TEXT NOT NULL,
    trading_day          TEXT
);

CREATE INDEX IF NOT EXISTS idx_chat_messages_channel_created
    ON chat_messages(channel, created_at);

CREATE INDEX IF NOT EXISTS idx_chat_messages_trading_day
    ON chat_messages(trading_day, created_at);

-- ============================================
-- Issue #9 Upgrade: Tasks
-- ============================================
CREATE TABLE IF NOT EXISTS tasks (
    task_id                  TEXT PRIMARY KEY,
    source_type              TEXT NOT NULL,
    source_message_id        TEXT,
    target_role              TEXT NOT NULL,
    intent_type              TEXT NOT NULL,
    workflow_type            TEXT NOT NULL,
    status                   TEXT NOT NULL,
    priority                 TEXT NOT NULL DEFAULT 'NORMAL',
    preemptible              INTEGER NOT NULL DEFAULT 1,
    requires_approval        INTEGER NOT NULL DEFAULT 0,
    visibility               TEXT NOT NULL DEFAULT 'channel',
    system_mode              TEXT NOT NULL DEFAULT 'NORMAL',
    parent_task_id           TEXT,
    superseded_by_task_id    TEXT,
    created_by               TEXT,
    arguments_json           TEXT,
    created_at               TEXT NOT NULL,
    updated_at               TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_status_priority
    ON tasks(status, priority, updated_at);

-- ============================================
-- Issue #9 Upgrade: Workflow Runs
-- ============================================
CREATE TABLE IF NOT EXISTS workflow_runs (
    workflow_run_id          TEXT PRIMARY KEY,
    task_id                  TEXT NOT NULL,
    workflow_type            TEXT NOT NULL,
    workflow_class           TEXT NOT NULL,
    status                   TEXT NOT NULL,
    trigger_type             TEXT NOT NULL,
    priority                 TEXT NOT NULL DEFAULT 'NORMAL',
    preempted_by_run_id      TEXT,
    resume_token             TEXT,
    deadline_at              TEXT,
    summary_json             TEXT,
    error_text               TEXT,
    started_at               TEXT NOT NULL,
    finished_at              TEXT
);

CREATE INDEX IF NOT EXISTS idx_workflow_runs_task
    ON workflow_runs(task_id, started_at);

CREATE INDEX IF NOT EXISTS idx_workflow_runs_status_priority
    ON workflow_runs(status, priority, started_at);

-- ============================================
-- Issue #9 Upgrade: Workflow Steps
-- ============================================
CREATE TABLE IF NOT EXISTS workflow_steps (
    step_id                  TEXT PRIMARY KEY,
    workflow_run_id          TEXT NOT NULL,
    step_order               INTEGER NOT NULL,
    step_role                TEXT NOT NULL,
    step_type                TEXT NOT NULL,
    status                   TEXT NOT NULL,
    input_json               TEXT,
    output_json              TEXT,
    started_at               TEXT NOT NULL,
    finished_at              TEXT,
    error_text               TEXT
);

CREATE INDEX IF NOT EXISTS idx_workflow_steps_run
    ON workflow_steps(workflow_run_id, step_order);

-- ============================================
-- Issue #9 Upgrade: Approval Requests
-- ============================================
CREATE TABLE IF NOT EXISTS approval_requests (
    approval_id              TEXT PRIMARY KEY,
    task_id                  TEXT NOT NULL,
    workflow_run_id          TEXT NOT NULL,
    approval_type            TEXT NOT NULL,
    status                   TEXT NOT NULL,
    requested_action         TEXT NOT NULL,
    instrument               TEXT,
    position_delta           REAL DEFAULT 0,
    risk_level               TEXT NOT NULL DEFAULT 'medium',
    expires_at               TEXT,
    superseded_by            TEXT,
    market_snapshot_id       TEXT,
    risk_snapshot_id         TEXT,
    request_hash             TEXT NOT NULL,
    resolved_by              TEXT,
    resolved_at              TEXT,
    resolution_note          TEXT,
    created_at               TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_approval_request_hash
    ON approval_requests(request_hash);

CREATE INDEX IF NOT EXISTS idx_approval_status_expires
    ON approval_requests(status, expires_at);

-- ============================================
-- Issue #9 Upgrade: Scheduled Jobs
-- ============================================
CREATE TABLE IF NOT EXISTS scheduled_jobs (
    job_id                   TEXT PRIMARY KEY,
    job_type                 TEXT NOT NULL,
    schedule_kind            TEXT NOT NULL,
    schedule_expr            TEXT NOT NULL,
    enabled                  INTEGER NOT NULL DEFAULT 1,
    last_run_at              TEXT,
    next_run_at              TEXT,
    payload_json             TEXT
);

-- ============================================
-- Issue #9 Upgrade: Agent Capabilities
-- ============================================
CREATE TABLE IF NOT EXISTS agent_capabilities (
    agent_name                   TEXT PRIMARY KEY,
    provider                     TEXT,
    allowed_workflows            TEXT NOT NULL,
    can_read_market_data         INTEGER NOT NULL DEFAULT 0,
    can_read_positions           INTEGER NOT NULL DEFAULT 0,
    can_read_logs                INTEGER NOT NULL DEFAULT 0,
    can_generate_trade_advice    INTEGER NOT NULL DEFAULT 0,
    can_request_approval         INTEGER NOT NULL DEFAULT 0,
    can_trigger_execution        INTEGER NOT NULL DEFAULT 0,
    can_force_protective_action  INTEGER NOT NULL DEFAULT 0,
    requires_structured_input    INTEGER NOT NULL DEFAULT 0,
    supports_natural_language    INTEGER NOT NULL DEFAULT 1,
    fallback_provider            TEXT,
    enabled                      INTEGER NOT NULL DEFAULT 1
);

-- ============================================
-- Issue #9 Upgrade: Daily Fact Snapshots
-- ============================================
CREATE TABLE IF NOT EXISTS daily_fact_snapshots (
    snapshot_id                  TEXT PRIMARY KEY,
    trading_day                  TEXT NOT NULL UNIQUE,
    instrument_stats_json        TEXT NOT NULL,
    decision_counts_json         TEXT NOT NULL,
    risk_event_counts_json       TEXT NOT NULL,
    approval_stats_json          TEXT NOT NULL,
    execution_stats_json         TEXT NOT NULL,
    reconciliation_stats_json    TEXT NOT NULL,
    portfolio_exposure_json      TEXT NOT NULL,
    incident_flags_json          TEXT NOT NULL,
    workflow_stats_json          TEXT NOT NULL,
    fallback_stats_json          TEXT NOT NULL,
    schema_version               TEXT NOT NULL,
    generator_version            TEXT NOT NULL,
    checksum                     TEXT,
    generated_at                 TEXT NOT NULL
);

-- ============================================
-- Issue #9 Upgrade: Daily Summaries
-- ============================================
CREATE TABLE IF NOT EXISTS daily_summaries (
    summary_id                   TEXT PRIMARY KEY,
    trading_day                  TEXT NOT NULL UNIQUE,
    runtime_path                 TEXT,
    knowledge_path               TEXT,
    status                       TEXT NOT NULL,
    headline                     TEXT NOT NULL,
    summary_json                 TEXT NOT NULL,
    generated_at                 TEXT NOT NULL,
    source_window_start          TEXT,
    source_window_end            TEXT
);
