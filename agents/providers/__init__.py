from __future__ import annotations

from .base import BaseProviderAgent, ProviderHealth, ProviderRequest, ProviderResponse
from .codex_agent import CodexAgent
from .deepseek_chat_agent import DeepSeekChatAgent
from .deepseek_reasoner_agent import DeepSeekReasonerAgent
from .doubao_agent import DoubaoAgent
from .perplexity_agent import PerplexityAgent
from .qwen_agent import QwenAgent


PROVIDER_CLASS_MAP = {
    "codex": CodexAgent,
    "perplexity-pro": PerplexityAgent,
    "deepseek-chat": DeepSeekChatAgent,
    "deepseek-reasoner": DeepSeekReasonerAgent,
    "doubao": DoubaoAgent,
    "qwen": QwenAgent,
}


def create_default_provider_agents(
    overrides: dict[str, dict] | None = None,
) -> list[BaseProviderAgent]:
    override_map = overrides or {}
    return [
        provider_cls(config=override_map.get(provider_id))
        for provider_id, provider_cls in PROVIDER_CLASS_MAP.items()
    ]


__all__ = [
    "BaseProviderAgent",
    "ProviderHealth",
    "ProviderRequest",
    "ProviderResponse",
    "CodexAgent",
    "PerplexityAgent",
    "DeepSeekChatAgent",
    "DeepSeekReasonerAgent",
    "DoubaoAgent",
    "QwenAgent",
    "PROVIDER_CLASS_MAP",
    "create_default_provider_agents",
]
