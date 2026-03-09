from __future__ import annotations

from .openai_compatible_chat_agent import OpenAICompatibleChatAgent


class DeepSeekChatAgent(OpenAICompatibleChatAgent):
    provider_id = 'deepseek-chat'
    display_name = 'DeepSeek Chat'
    api_key_env = 'DEEPSEEK_API_KEY'
    default_base_url = 'https://api.deepseek.com'
    default_model_name = 'deepseek-chat'
    system_prompt = 'You are DeepSeek Chat, a financial trading assistant focused on Chinese-language analysis for the China futures market.'
