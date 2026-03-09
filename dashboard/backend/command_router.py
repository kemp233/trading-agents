"""Command routing utilities for agent and Streamlit chat entrypoints."""

from __future__ import annotations

import logging
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from dashboard.backend.chat_types import ChatReply

logger = logging.getLogger(__name__)

COMMAND_MAP = {
    "all": {
        "/status": "cmd_all_status",
        "/report": "cmd_all_report",
    },
    "Risk_Governor": {
        "/state": "cmd_risk_state",
        "/recover": "cmd_risk_recover",
        "/circuit": "cmd_risk_circuit",
    },
    "Strategy": {
        "/pause": "cmd_strategy_pause",
        "/resume": "cmd_strategy_resume",
        "/list": "cmd_strategy_list",
    },
    "Order_Manager": {
        "/cancel_all": "cmd_order_cancel_all",
        "/flatten": "cmd_order_flatten",
    },
    "Market_Data": {
        "/subscribe": "cmd_market_subscribe",
        "/health": "cmd_market_health",
    },
    "News": {
        "/latest": "cmd_news_latest",
        "/windows": "cmd_news_windows",
    },
    "Reconciler": {
        "/check": "cmd_reconciler_check",
    },
    "Portfolio": {
        "/pnl": "cmd_portfolio_pnl",
        "/exposure": "cmd_portfolio_exposure",
    },
}

WORKFLOW_ROLE_MAP = {
    "all": "orchestration",
    "Risk_Governor": "risk",
    "Strategy": "strategy",
    "Order_Manager": "execution",
    "Market_Data": "market-data",
    "News": "news",
    "Reconciler": "reconciliation",
    "Portfolio": "portfolio",
}

CHANNEL_MAP = {
    "all": "general",
    "Risk_Governor": "risk-alerts",
    "Strategy": "signals",
    "Order_Manager": "orders",
    "Market_Data": "system",
    "News": "news",
    "Reconciler": "system",
    "Portfolio": "general",
}


def parse_command(text: str) -> Optional[dict[str, str]]:
    pattern = r"@(\w+)\s+(/\w+)\s*(.*)?"
    match = re.match(pattern, text.strip())
    if not match:
        return None
    return {
        "agent": match.group(1),
        "command": match.group(2),
        "args": match.group(3).strip() if match.group(3) else "",
    }


def list_command_suggestions(prefix: str = "") -> list[str]:
    suggestions = [
        f"@{agent} {command}"
        for agent, commands in COMMAND_MAP.items()
        for command in commands
    ]
    if not prefix.strip():
        return suggestions
    lowered = prefix.strip().lower()
    return [item for item in suggestions if item.lower().startswith(lowered)]


class CommandRouter:
    def __init__(self, agents: dict[str, Any]):
        self._agents = agents

    async def execute(self, text: str) -> dict[str, Any]:
        parsed = parse_command(text)
        if not parsed:
            return {"error": f"无法解析指令：{text}"}
        agent_name = parsed["agent"]
        command = parsed["command"]
        args = parsed["args"]
        if agent_name == "all":
            results = {}
            for name, agent in self._agents.items():
                try:
                    result = await agent.handle_command(command, args)
                    results[name] = result
                except Exception as exc:
                    results[name] = f"错误：{exc}"
            return {"agent": "all", "command": command, "results": results}

        agent = self._agents.get(agent_name)
        if not agent:
            return {"error": f"未知 Agent：{agent_name}"}
        try:
            result = await agent.handle_command(command, args)
            return {"agent": agent_name, "command": command, "result": result}
        except Exception as exc:
            return {"error": f"{agent_name} 执行失败：{exc}"}


def handle(
    command: str,
    reason: str = "",
    adapter=None,
    state_writer=None,
    risk_governor=None,
    order_manager=None,
) -> dict[str, Any]:
    import asyncio

    logger.info("handle command: %s reason=%s", command, reason)

    default_db = str(Path(__file__).resolve().parent.parent.parent / "data" / "trading.db")
    db_path = os.environ.get("AIAGENTTS_DB", default_db)

    def run_async(coro):
        try:
            return asyncio.run(coro)
        except RuntimeError:
            loop = asyncio.get_event_loop()
            return loop.create_task(coro)

    def write_risk_state_fallback(current: str, previous: str | None, rsn: str) -> None:
        try:
            with sqlite3.connect(db_path) as conn:
                prior_state = previous
                if prior_state is None:
                    row = conn.execute(
                        "SELECT current_state FROM risk_state_log ORDER BY id DESC LIMIT 1"
                    ).fetchone()
                    prior_state = row[0] if row else "NORMAL"
                conn.execute(
                    "INSERT INTO risk_state_log (current_state, previous_state, state_changed_at, reason) VALUES (?, ?, ?, ?)",
                    (current, prior_state, datetime.now(timezone.utc).isoformat(), rsn),
                )
                conn.commit()
        except Exception as exc:
            logger.warning("write_risk_state_fallback failed: %s", exc)

    def write_system_log_fallback(event_type: str, detail: str | None) -> None:
        try:
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "INSERT INTO system_log (ts, event_type, detail) VALUES (?, ?, ?)",
                    (datetime.now(timezone.utc).isoformat(), event_type, detail),
                )
                conn.commit()
        except Exception as exc:
            logger.warning("write_system_log_fallback failed: %s", exc)

    def emit_system_log(event_type: str, detail: str | None) -> None:
        if state_writer is not None:
            from core.state_schema import SystemLogEntry

            entry = SystemLogEntry(
                ts=datetime.now(timezone.utc),
                event_type=event_type,
                detail=detail,
            )
            run_async(state_writer.write_system_log(entry))
        else:
            write_system_log_fallback(event_type, detail)

    if command == "HALT":
        if risk_governor is not None:
            risk_governor.halt(reason or "manual")
        else:
            write_risk_state_fallback("VENUE_HALT", None, reason or "manual")
        emit_system_log("HALT", reason or "manual")
        return {"ok": True, "command": "HALT", "reason": reason}

    if command == "RESUME":
        if risk_governor is not None:
            risk_governor.resume()
        else:
            write_risk_state_fallback("NORMAL", "VENUE_HALT", "manual_resume")
        emit_system_log("RESUME", None)
        return {"ok": True, "command": "RESUME"}

    if command == "CIRCUIT":
        if risk_governor is not None:
            risk_governor.transition("CIRCUIT", reason or "manual_circuit", {"trigger": "chat_command"})
        else:
            write_risk_state_fallback("CIRCUIT", None, reason or "manual_circuit")
        emit_system_log("CIRCUIT", reason or "manual_circuit")
        return {"ok": True, "command": "CIRCUIT", "reason": reason or "manual_circuit"}

    if command == "CANCEL_ALL":
        if order_manager is not None:
            run_async(order_manager.cancel_all())
            emit_system_log("CANCEL_ALL", reason or None)
            return {"ok": True, "command": "CANCEL_ALL"}
        if risk_governor is not None and adapter is not None:
            run_async(risk_governor.cancel_all_orders(adapter))
        emit_system_log("CANCEL_ALL", reason or None)
        return {"ok": True, "command": "CANCEL_ALL"}

    if command in {"FLATTEN", "FLATTEN_ALL"}:
        symbol = reason or None
        if order_manager is not None:
            run_async(order_manager.flatten(symbol))
            emit_system_log("FLATTEN", symbol)
            return {"ok": True, "command": "FLATTEN", "symbol": symbol}
        if risk_governor is not None and adapter is not None:
            run_async(risk_governor.attempt_flatten(adapter))
            emit_system_log("FLATTEN", symbol)
            return {"ok": True, "command": "FLATTEN", "symbol": symbol}
        return {"ok": False, "error": "FLATTEN 需要 order_manager 或 risk_governor+adapter"}

    logger.warning("handle: unknown command %s", command)
    return {"ok": False, "error": f"unknown command: {command}"}


def handle_chat_command(
    text: str,
    *,
    db_reader=None,
    adapter=None,
    state_writer=None,
    risk_governor=None,
    order_manager=None,
) -> ChatReply:
    parsed = parse_command(text)
    if not parsed:
        return ChatReply(
            ok=False,
            agent_name="系统",
            command_text=text,
            channel="general",
            content="指令格式错误，请使用 @Agent /command [args]。",
            severity="critical",
            executed=False,
            meta={"suggestions": list_command_suggestions()},
            workflow_role="orchestration",
        )

    agent = parsed["agent"]
    command = parsed["command"]
    args = parsed["args"]
    commands = COMMAND_MAP.get(agent)
    if commands is None or command not in commands:
        return ChatReply(
            ok=False,
            agent_name=agent,
            command_text=text,
            channel=CHANNEL_MAP.get(agent, "general"),
            content=f"{agent} 暂不支持该命令：{command}",
            severity="critical",
            executed=False,
            meta={"suggestions": list_command_suggestions(f"@{agent}")},
            workflow_role=WORKFLOW_ROLE_MAP.get(agent),
        )

    if db_reader is None:
        from dashboard.streamlit_mvp.db_reader import DbReader

        default_db = str(Path(__file__).resolve().parent.parent.parent / "data" / "trading.db")
        db_reader = DbReader(os.environ.get("AIAGENTTS_DB", default_db))

    if agent == "all":
        statuses = db_reader.get_agent_statuses()
        summary = "\n".join(f"- {item['agent']}: {item['status']}（{item['detail']}）" for item in statuses)
        label = "状态汇总" if command == "/status" else "工作报告"
        return ChatReply(
            ok=True,
            agent_name="all",
            command_text=text,
            channel="general",
            content=f"智能体{label}：\n{summary}",
            severity="info",
            executed=True,
            meta={"statuses": statuses},
            workflow_role=WORKFLOW_ROLE_MAP.get(agent),
        )

    if agent == "Risk_Governor":
        if command == "/state":
            record = db_reader.get_latest_risk_record()
            content = (
                f"当前风控状态：{record.get('current_state', 'NORMAL')}\n"
                f"触发原因：{record.get('reason') or '暂无'}"
            )
            return ChatReply(True, agent, text, "risk-alerts", content, "info", True, meta=record, workflow_role="risk")
        if command == "/circuit":
            result = handle("CIRCUIT", reason=args or "manual_circuit", state_writer=state_writer, risk_governor=risk_governor)
            return ChatReply(True, agent, text, "risk-alerts", f"已受理熔断指令：{result}", "critical", True, meta=result, workflow_role="risk")
        if command == "/recover":
            result = handle("RESUME", state_writer=state_writer, risk_governor=risk_governor)
            return ChatReply(True, agent, text, "risk-alerts", f"已受理恢复指令：{result}", "success", True, meta=result, workflow_role="risk")

    if agent == "Order_Manager":
        if command == "/cancel_all":
            result = handle("CANCEL_ALL", adapter=adapter, state_writer=state_writer, risk_governor=risk_governor, order_manager=order_manager)
            return ChatReply(True, agent, text, "orders", f"已受理全部撤单：{result}", "warning", True, meta=result, workflow_role="execution")
        if command == "/flatten":
            result = handle("FLATTEN", reason=args, adapter=adapter, state_writer=state_writer, risk_governor=risk_governor, order_manager=order_manager)
            severity = "success" if result.get("ok") else "critical"
            return ChatReply(bool(result.get("ok")), agent, text, "orders", f"平仓结果：{result}", severity, True, meta=result, workflow_role="execution")

    if agent == "Market_Data":
        if command == "/health":
            status = db_reader.get_latest_connection_status()
            return ChatReply(
                ok=True,
                agent_name=agent,
                command_text=text,
                channel="system",
                content=f"行情连接状态：{status.get('status')}（{status.get('detail') or status.get('front_addr') or '暂无详情'}）",
                severity="info",
                executed=True,
                meta=status,
                workflow_role="market-data",
            )
        if command == "/subscribe":
            return ChatReply(
                ok=True,
                agent_name=agent,
                command_text=text,
                channel="system",
                content=f"已记录订阅请求：{args or '默认品种'}。实时订阅联动会在后续工作流中接入。",
                severity="info",
                executed=False,
                meta={"symbol": args},
                workflow_role="market-data",
            )

    if agent == "Portfolio":
        snapshot = db_reader.get_portfolio_snapshot()
        if command == "/pnl":
            account = snapshot["account"]
            content = (
                f"账户权益={float(account.get('equity') or 0):,.2f}，"
                f"可用资金={float(account.get('available') or 0):,.2f}，"
                f"浮动盈亏={snapshot['unrealized_pnl']:,.2f}"
            )
            return ChatReply(True, agent, text, "general", content, "info", True, meta=snapshot, workflow_role="portfolio")
        if command == "/exposure":
            content = (
                f"持仓数={snapshot['position_count']}，总数量={snapshot['total_quantity']:,.2f}，"
                f"品种={', '.join(snapshot['symbols']) if snapshot['symbols'] else '无'}"
            )
            return ChatReply(True, agent, text, "general", content, "info", True, meta=snapshot, workflow_role="portfolio")

    if agent == "Reconciler" and command == "/check":
        snapshot = db_reader.get_reconciler_snapshot()
        message = (
            f"对账快照：持仓={snapshot['position_count']}，"
            f"风控状态={snapshot['risk_state']}，"
            f"品种={', '.join(snapshot['symbols']) if snapshot['symbols'] else '无'}"
        )
        return ChatReply(True, agent, text, "system", message, "info", True, meta=snapshot, workflow_role="reconciliation")

    if agent == "Strategy":
        if command in {"/pause", "/resume"}:
            action = "暂停" if command == "/pause" else "恢复"
            return ChatReply(
                ok=True,
                agent_name=agent,
                command_text=text,
                channel="signals",
                content=f"已记录策略{action}指令。真实策略生命周期绑定会在后续工作流接入。",
                severity="warning" if command == "/pause" else "success",
                executed=False,
                meta={"action": action},
                workflow_role="strategy",
            )
        if command == "/list":
            return ChatReply(True, agent, text, "signals", "当前策略槽位：ema_crossover_v1（MVP 适配模式）。", "info", False, meta={"strategies": ["ema_crossover_v1"]}, workflow_role="strategy")

    if agent == "News":
        if command == "/latest":
            return ChatReply(True, agent, text, "news", "资讯 Provider 插槽已预留，issue9 暂未接入实时新闻源。", "info", False, meta={"provider_slot": True}, workflow_role="news")
        if command == "/windows":
            return ChatReply(True, agent, text, "news", "宏观/资讯窗口尚未接线，后续可通过本地 Provider 映射接入。", "info", False, meta={"provider_slot": True}, workflow_role="news")

    return ChatReply(
        ok=False,
        agent_name=agent,
        command_text=text,
        channel=CHANNEL_MAP.get(agent, "general"),
        content=f"{agent} {command} 的处理逻辑尚未补齐。",
        severity="critical",
        executed=False,
        workflow_role=WORKFLOW_ROLE_MAP.get(agent),
    )


def handle_chat_input(
    text: str,
    *,
    db_reader=None,
    sender_id: str = "operator",
    channel_hint: str | None = None,
) -> ChatReply:
    default_db = str(Path(__file__).resolve().parent.parent.parent / "data" / "trading.db")
    db_path = getattr(db_reader, "_db_path", None) or os.environ.get("AIAGENTTS_DB", default_db)
    from dashboard.backend.chatops_runtime import ChatOpsRuntime

    runtime = ChatOpsRuntime(db_path=db_path)
    return runtime.process_text(text, sender_id=sender_id, channel_hint=channel_hint)
