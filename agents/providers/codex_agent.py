from __future__ import annotations

from .base import BaseProviderAgent


class CodexAgent(BaseProviderAgent):
    provider_id = "codex"
    display_name = "Codex"
    api_key_env = "CODEX_API_KEY"
    default_base_url = "https://api.openai.com/v1"
    default_model_name = "codex"
