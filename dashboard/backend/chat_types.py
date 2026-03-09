from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True)
class ChatMessage:
    id: str
    channel: str
    agent_name: str
    content: str
    severity: str
    ts: str
    meta: dict[str, Any] = field(default_factory=dict)
    source_provider: str | None = None
    workflow_role: str | None = None
    trace_id: str | None = None
    step_id: str | None = None
    author_kind: str = "system"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ChatReply:
    ok: bool
    agent_name: str
    command_text: str
    channel: str
    content: str
    severity: str
    executed: bool
    meta: dict[str, Any] = field(default_factory=dict)
    source_provider: str | None = None
    workflow_role: str | None = None
    trace_id: str | None = None
    step_id: str | None = None

    def to_message(self, *, message_id: str, ts: str) -> ChatMessage:
        return ChatMessage(
            id=message_id,
            channel=self.channel,
            agent_name=self.agent_name,
            content=self.content,
            severity=self.severity,
            ts=ts,
            meta=dict(self.meta),
            source_provider=self.source_provider,
            workflow_role=self.workflow_role,
            trace_id=self.trace_id,
            step_id=self.step_id,
            author_kind="assistant",
        )
