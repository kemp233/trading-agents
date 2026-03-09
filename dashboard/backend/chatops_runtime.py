from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any
from uuid import uuid4

from dashboard.backend.agent_registry import ensure_seeded, get_agent_capability
from dashboard.backend.approval_engine import ApprovalEngine
from dashboard.backend.archive_workflow import ArchiveWorkflow
from dashboard.backend.chat_types import ChatReply
from dashboard.backend.chatops_models import infer_trading_day, utc_now_iso
from dashboard.backend.conversation_context import ConversationContextStore
from dashboard.backend.intent_resolver import resolve_intent
from dashboard.backend.workflow_engine import WorkflowEngine
from dashboard.streamlit_mvp.db_reader import DbReader
from core.model_router import ModelRouter


class ChatOpsRuntime:
    def __init__(self, db_path: str, repo_root: str | None = None) -> None:
        self._db_path = db_path
        self._repo_root = Path(repo_root or Path(__file__).resolve().parents[2])
        self._ensure_schema()
        ensure_seeded(db_path)
        self._reader = DbReader(db_path)
        self._model_router = ModelRouter(config={})
        self._workflow_engine = WorkflowEngine(db_path)
        self._approval_engine = ApprovalEngine(db_path)
        self._context_store = ConversationContextStore(self._reader)
        self._archive_workflow = ArchiveWorkflow(db_path, str(self._repo_root))

    def _ensure_schema(self) -> None:
        schema_path = self._repo_root / 'db' / 'schema.sql'
        if not schema_path.exists():
            return
        with sqlite3.connect(self._db_path) as conn:
            conn.executescript(schema_path.read_text(encoding='utf-8'))
            conn.commit()

    def process_text(self, text: str, *, sender_id: str = 'operator', channel_hint: str | None = None, reply_to: dict[str, Any] | None = None) -> ChatReply:
        intent = resolve_intent(text)
        channel = channel_hint or intent.channel
        user_message_id = self._persist_message(
            channel=channel,
            author_kind='user',
            sender_id=sender_id,
            agent_role=intent.target_role,
            message_type='plain',
            visibility=intent.visibility,
            content=text,
            payload={'intent_type': intent.intent_type, 'source_type': intent.source_type, 'reply_to': reply_to or {}},
        )

        if intent.source_type == 'slash_command':
            return self._process_group_command(text, intent, user_message_id, channel)

        system_mode = self._current_system_mode()
        self._approval_engine.expire_for_system_mode(system_mode)
        self._approval_engine.expire_stale_requests(self._latest_market_snapshot_id(), self._latest_risk_snapshot_id())

        capability = get_agent_capability(intent.target_role)
        if capability and not capability.get('supports_natural_language', True):
            reply = ChatReply(
                ok=False,
                agent_name=intent.target_role,
                command_text=text,
                channel=channel,
                content='Execution agent does not accept direct natural-language execution. Ask for guidance first, then send an explicit @agent -command... message.',
                severity='critical',
                executed=False,
                meta={'system_mode': system_mode, 'requires_structured_input': True},
                workflow_role=intent.target_role,
            )
            self._persist_reply(reply, workflow_run_id=None, task_id=None)
            return reply

        task = self._workflow_engine.create_task_and_run(
            intent=intent,
            source_message_id=user_message_id,
            created_by=sender_id,
            system_mode=system_mode,
        )
        context_items = self._context_store.build(intent.target_role, intent.arguments.get('query', text))

        if intent.workflow_type == 'incident_response':
            self._preempt_analysis_runs(task['workflow_run_id'])

        meta: dict[str, Any] = {
            'task_id': task['task_id'],
            'workflow_run_id': task['workflow_run_id'],
            'system_mode': system_mode,
            'context_items': context_items,
        }
        if reply_to:
            meta['in_reply_to'] = reply_to

        if intent.intent_type == 'log_request':
            reply = ChatReply(
                ok=True,
                agent_name=intent.target_role,
                command_text=text,
                channel=channel,
                content=(
                    'I can help with logs. Please confirm the scope first, then send an explicit command like '                    f'`@{self._role_to_group_handle(intent.target_role)} -commandlog latest` or '                    f'`@{self._role_to_group_handle(intent.target_role)} -commandlog risk`.'
                ),
                severity='info',
                executed=False,
                meta=meta,
                workflow_role=intent.target_role,
            )
            self._workflow_engine.complete_run(task['workflow_run_id'], 'COMPLETED', summary={'summary': reply.content})
            self._persist_reply(reply, task_id=task['task_id'], workflow_run_id=task['workflow_run_id'], message_type='summary_card')
            return reply

        if intent.requires_approval:
            approval = self._approval_engine.create_request(
                task_id=task['task_id'],
                workflow_run_id=task['workflow_run_id'],
                approval_type='trade_execution',
                requested_action=intent.workflow_type,
                instrument=self._extract_instrument(text),
                position_delta=1.0 if intent.workflow_type == 'open_position' else -1.0,
                risk_level='high',
                market_snapshot_id=self._latest_market_snapshot_id(),
                risk_snapshot_id=self._latest_risk_snapshot_id(),
                system_mode=system_mode,
            )
            meta['approval'] = approval
            content = (
                f'Created `{intent.workflow_type}` task and queued it for approval. '                f'task={task["task_id"]}, approval={approval["approval_id"]}, system_mode={system_mode}'
            )
            reply = ChatReply(
                ok=True,
                agent_name=intent.target_role,
                command_text=text,
                channel=channel,
                content=content,
                severity='warning',
                executed=False,
                meta=meta,
                workflow_role=intent.target_role,
            )
            self._persist_reply(reply, task_id=task['task_id'], workflow_run_id=task['workflow_run_id'], message_type='approval_card')
            return reply

        summary = self._render_analysis_summary(intent.target_role, text, context_items)
        self._workflow_engine.complete_run(task['workflow_run_id'], 'COMPLETED', summary={'summary': summary})
        reply = ChatReply(
            ok=True,
            agent_name=intent.target_role,
            command_text=text,
            channel=channel,
            content=summary,
            severity='info',
            executed=True,
            meta=meta,
            workflow_role=intent.target_role,
        )
        self._persist_reply(reply, task_id=task['task_id'], workflow_run_id=task['workflow_run_id'], message_type='summary_card')
        return reply

    def run_archive_for_day(self, trading_day: str | None = None) -> dict[str, Any]:
        day = self._reader.get_latest_account_info().get('trading_day') or infer_trading_day()
        return self._archive_workflow.run_for_day(str(trading_day or day))

    def resolve_approval(self, approval_id: str, action: str, *, operator_id: str = 'operator') -> dict[str, Any] | None:
        status_map = {
            'approve': 'APPROVED',
            'reject': 'REJECTED',
            'review': 'ESCALATED',
            'handoff_risk': 'ESCALATED',
        }
        status = status_map.get(action)
        if status is None:
            raise ValueError(f'unsupported approval action: {action}')
        approval = self._approval_engine.resolve_request(approval_id, status=status, resolved_by=operator_id, resolution_note=action)
        if approval is None:
            return None
        severity = 'success' if status == 'APPROVED' else 'warning' if status == 'ESCALATED' else 'critical'
        self._persist_message(
            channel='group-chat',
            author_kind='system',
            sender_id='system',
            agent_role='system',
            message_type='approval_card',
            visibility='channel',
            content=f'Approval {approval_id} -> {status}',
            payload={'approval_id': approval_id, 'approval': approval, 'action': action, 'severity': severity},
            task_id=approval.get('task_id'),
            workflow_run_id=approval.get('workflow_run_id'),
        )
        return approval

    def _process_group_command(self, text: str, intent, user_message_id: str, channel: str) -> ChatReply:
        command = str(intent.arguments.get('command') or '')
        args = str(intent.arguments.get('args') or '')
        if command.startswith('-command'):
            payload = self._execute_group_shorthand(intent.target_role, command, args)
            reply = ChatReply(
                ok=bool(payload.get('ok', True)),
                agent_name=intent.target_role,
                command_text=text,
                channel=channel,
                content=str(payload.get('content') or payload.get('message') or 'Command handled.'),
                severity=str(payload.get('severity') or ('info' if payload.get('ok', True) else 'critical')),
                executed=bool(payload.get('executed', False)),
                meta=payload,
                workflow_role=intent.target_role,
            )
            self._persist_reply(reply, message_type='summary_card')
            return reply

        from dashboard.backend.command_router import handle_chat_command

        reply = handle_chat_command(text, db_reader=self._reader)
        self._persist_reply(reply)
        return reply

    def _execute_group_shorthand(self, role: str, command: str, args: str) -> dict[str, Any]:
        normalized = command[len('-command'):].strip().lower()
        if normalized == 'log':
            logs = self._reader.get_system_log(limit=5)
            if role == 'risk':
                logs = self._reader.get_risk_state_history(limit=5)
            if role == 'reconciliation':
                logs = self._reader.get_audit_log(limit=5)
            lines = []
            for item in logs[:5]:
                stamp = item.get('ts') or item.get('timestamp') or item.get('state_changed_at') or ''
                detail = item.get('detail') or item.get('reason') or item.get('content') or item.get('event_type') or 'log'
                lines.append(f'- {stamp}: {detail}')
            return {
                'ok': True,
                'executed': True,
                'severity': 'info',
                'content': 'Log command completed.\n' + ('\n'.join(lines) if lines else 'No log entries found.'),
            }
        if normalized == 'status':
            return {
                'ok': True,
                'executed': True,
                'severity': 'info',
                'content': f'{role} command status acknowledged. args={args or "none"}',
            }
        return {
            'ok': False,
            'executed': False,
            'severity': 'critical',
            'content': f'Unknown shorthand command `{command}`. Try `-commandlog` or `-commandstatus`.',
        }

    def _persist_message(
        self,
        *,
        channel: str,
        author_kind: str,
        sender_id: str,
        agent_role: str | None,
        message_type: str,
        visibility: str,
        content: str,
        payload: dict[str, Any] | None = None,
        task_id: str | None = None,
        workflow_run_id: str | None = None,
    ) -> str:
        message_id = f'msg-{uuid4().hex}'
        trading_day = self._reader.get_latest_account_info().get('trading_day') or infer_trading_day()
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                """
                INSERT INTO chat_messages (
                    message_id, channel, author_kind, sender_id, agent_role,
                    message_type, visibility, task_id, workflow_run_id,
                    content, payload_json, created_at, trading_day
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message_id,
                    channel,
                    author_kind,
                    sender_id,
                    agent_role,
                    message_type,
                    visibility,
                    task_id,
                    workflow_run_id,
                    content,
                    json.dumps(payload or {}, ensure_ascii=False),
                    utc_now_iso(),
                    str(trading_day),
                ),
            )
            conn.commit()
        return message_id

    def _persist_reply(self, reply: ChatReply, task_id: str | None = None, workflow_run_id: str | None = None, message_type: str = 'plain') -> None:
        self._persist_message(
            channel=reply.channel,
            author_kind='assistant',
            sender_id=reply.agent_name,
            agent_role=reply.workflow_role,
            message_type=message_type,
            visibility='channel',
            content=reply.content,
            payload=reply.meta,
            task_id=task_id,
            workflow_run_id=workflow_run_id,
        )

    def _current_system_mode(self) -> str:
        latest_risk = str(self._reader.get_latest_risk_record().get('current_state') or 'NORMAL')
        return {
            'NORMAL': 'NORMAL',
            'DEGRADED': 'CAUTION',
            'CIRCUIT': 'RESTRICTED',
            'VENUE_HALT': 'HALTED',
            'OFFLINE': 'PROTECT_ONLY',
            'RECONCILING': 'RECONCILING',
        }.get(latest_risk, 'NORMAL')

    def _latest_market_snapshot_id(self) -> str | None:
        status = self._reader.get_latest_connection_status()
        snapshot = status.get('id') or status.get('ts')
        return f'market:{snapshot}' if snapshot else None

    def _latest_risk_snapshot_id(self) -> str | None:
        record = self._reader.get_latest_risk_record()
        snapshot = record.get('id') or record.get('state_changed_at')
        return f'risk:{snapshot}' if snapshot else None

    def _extract_instrument(self, text: str) -> str | None:
        for token in text.split():
            if any(char.isdigit() for char in token) or token.isupper():
                return token.strip('@,.;')
        return None

    def _preempt_analysis_runs(self, new_run_id: str) -> None:
        with sqlite3.connect(self._db_path) as conn:
            rows = conn.execute("SELECT workflow_run_id FROM workflow_runs WHERE workflow_class = 'analysis' AND status IN ('RUNNING', 'QUEUED', 'WAITING_DATA')").fetchall()
        for row in rows:
            self._workflow_engine.mark_preempted(row[0], new_run_id, resumable=True)

    def _render_analysis_summary(self, role: str, text: str, context_items: list[dict[str, Any]]) -> str:
        live = self._invoke_role_provider(role, text, context_items)
        if live:
            return live

        if role == 'news':
            return f'News agent received: {text} | context_items={len(context_items)} | I can discuss events naturally, then guide you to an explicit command if needed.'
        if role == 'strategy':
            return f'Strategy agent received: {text} | context_items={len(context_items)} | I can discuss setups in natural language, but execution still needs explicit approval or command.'
        if role == 'risk':
            return f'Risk agent received: {text} | context_items={len(context_items)} | I can explain the current constraints and tell you the exact command to send next.'
        if role == 'orchestration':
            portfolio = self._reader.get_portfolio_snapshot()
            risk = self._reader.get_latest_risk_record()
            return f"Group summary: positions={portfolio['position_count']}, unrealized_pnl={portfolio['unrealized_pnl']:,.2f}, risk_state={risk.get('current_state', 'NORMAL')} | message={text}"
        return f'{role} agent received: {text} | context_items={len(context_items)}'

    def _role_to_group_handle(self, role: str) -> str:
        mapping = {
            'orchestration': 'orchestration',
            'strategy': 'strategy',
            'risk': 'risk',
            'execution': 'execution',
            'market-data': 'market_data',
            'news': 'news',
            'reconciliation': 'reconciler',
            'portfolio': 'portfolio',
        }
        return mapping.get(role, role)

    def _invoke_role_provider(self, role: str, text: str, context_items: list[dict[str, Any]]) -> str | None:
        assignment = self._model_router.workflow_assignments.get_assignment(role)
        provider_ids = assignment.provider_ids or []
        if not provider_ids:
            return None
        metadata = assignment.metadata or {}
        provider_models = dict(metadata.get('provider_models') or {})
        task_bindings = dict(metadata.get('task_bindings') or {})

        prompt = self._build_role_prompt(role, text, context_items)

        ordered_provider_ids = list(provider_ids)
        if role == 'news':
            preferred = []
            for binding_key in ('deep_zh', 'deep_zh_fallback', 'classification', 'retrieval'):
                provider_id = task_bindings.get(binding_key)
                if provider_id and provider_id not in preferred:
                    preferred.append(provider_id)
            ordered_provider_ids = preferred + [item for item in provider_ids if item not in preferred]

        errors: list[str] = []
        for provider_id in ordered_provider_ids:
            provider = self._provider_with_model_override(provider_id, provider_models.get(provider_id))
            if provider is None:
                continue
            response = provider.invoke(
                provider.build_request(
                    prompt=prompt,
                    context={'role': role, 'context_items': context_items},
                    metadata={'channel_role': role},
                )
            )
            if response.ok and response.content.strip():
                return response.content.strip()
            errors.append(f'{provider.display_name}: {response.content}')
        if errors:
            return ' | '.join(errors)
        return None

    def _build_role_prompt(self, role: str, text: str, context_items: list[dict[str, Any]]) -> str:
        context_preview = []
        for item in context_items[:4]:
            source = item.get('source', 'context')
            content = str(item.get('content', ''))[:240]
            context_preview.append(f'[{source}] {content}')
        context_block = '\n'.join(context_preview) if context_preview else 'None'

        base = (
            'You are working inside a self-built China futures trading system. Reply in Chinese, lead with the conclusion, and stay concise.\n'
            'If the request implies trading, provide analysis and risk notes, but do not claim any real execution happened.\n\n'
            f'Role: {role}\n'
            f'User request: {text}\n\n'
            f'Available context:\n{context_block}\n\n'
        )

        prompts = {
            'news': 'Focus on news, policy, industry chain and market sentiment. Output: event summary, impact path, affected instruments, bullish/bearish/neutral view, time window, and action notes.',
            'strategy': 'Focus on trade thesis and setup quality. Output: current judgement, key signals, supporting/opposing reasons, risks, and what should be confirmed before action.',
            'risk': 'Focus on risk explanation. Output: current risk state, restriction reason, possible consequence, and the safest next command or confirmation step.',
            'orchestration': 'Act as coordinator. Output: overall conclusion, which roles are involved, what should happen next, and whether approval is needed.',
            'reconciliation': 'Focus on reconciliation and audit. Output: what should be checked, likely inconsistencies, severity, and the next log or status command to run.',
            'portfolio': 'Focus on portfolio exposure. Output: concentration or correlation risk, remaining risk budget, and the next recommended action.',
        }
        return base + prompts.get(role, 'Provide a concise Chinese answer aligned with the role responsibility.')

    def _provider_with_model_override(self, provider_id: str, model_name: str | None):
        provider = self._model_router.provider_registry.get_provider(provider_id)
        if provider is None:
            return None
        if not model_name:
            return provider
        return provider.__class__(
            config={
                'api_key': provider.api_key,
                'base_url': provider.base_url,
                'timeout': min(float(provider.timeout), 12.0),
                'model_name': model_name,
            }
        )
