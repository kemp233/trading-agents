# Trading Agents 🤖📈

> 多资产 Multi-Agent 量化交易系统 v3 (Production-Ready)

覆盖加密货币（Binance 合约）与中国商品期货（CTP），事件驱动五层架构，Outbox 原子下单，六状态风控状态机，Agent 群组聊天 UI。

## 架构概览

```
A 层 · 感知层  — Market Data / News & Macro / 经济日历
B 层 · 决策层  — TA / Strategy / Risk Governor
C 层 · 执行层  — Order Manager (Outbox) / Reconciler / Venue Adapter
D 层 · 状态层  — State Store / Outbox 表 / 审计日志 / Data QA
E 层 · 基础设施 — 事件总线 / 模型路由 R0-R6 / Dashboard UI
```

## Agent 清单

| Agent | 层级 | 在线调 LLM |
|-------|------|----------|
| Orchestrator | 跨层 | ❌ |
| Market Data | A 感知 | ❌ |
| News & Macro | A 感知 | ❌ (离线用LLM) |
| Technical Analysis | B 决策 | ❌ |
| Strategy | B 决策 | ❌ |
| Risk Governor | B 决策 | ❌ |
| Order Manager | C 执行 | ❌ |
| Reconciler | C 执行 | ❌ |
| Portfolio Tracker | D 状态 | ❌ |
| Venue Adapter | C 执行 | ❌ |

## 关键设计

- **Outbox Pattern**: 原子下单，防止崩溃窗口重复下单
- **Per-stream 序列号**: `(stream_id, stream_seq)` 替代全局单调递增
- **双时间戳**: `event_ts` + `recv_ts`，防止 look-ahead bias
- **六状态风控**: NORMAL → DEGRADED → CIRCUIT_BREAKER → RECONCILING → VENUE_HALT
- **Agent 群组聊天 UI**: @指令系统，类 Slack/Discord 频道交互

## 开发路线

- **Phase 1** (4-6周): 最小可交易系统 + Streamlit MVP + 故障注入测试
- **Phase 2** (3-4周): 多品种 + React 群组聊天 UI + Redis
- **Phase 3** (3-4周): 模拟盘 + Grafana 监控
- **Phase 4** (2-3周): 实盘

## 技术栈

Python 3.11+ / asyncio / SQLite(WAL) → PostgreSQL / Redis Streams / ccxt / vn.py / DeepSeek V3&R1 (离线) / Streamlit → React / FastAPI + WebSocket
