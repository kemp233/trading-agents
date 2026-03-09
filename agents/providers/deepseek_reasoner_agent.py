from __future__ import annotations

from .openai_compatible_chat_agent import OpenAICompatibleChatAgent


class DeepSeekReasonerAgent(OpenAICompatibleChatAgent):
    provider_id = 'deepseek-reasoner'
    display_name = 'DeepSeek Reasoner'
    api_key_env = 'DEEPSEEK_API_KEY'
    default_base_url = 'https://api.deepseek.com'
    default_model_name = 'deepseek-reasoner'
    system_prompt = 'You are DeepSeek Reasoner, a careful strategy and portfolio reasoning assistant for the China futures market.'
