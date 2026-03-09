from __future__ import annotations

from .openai_compatible_chat_agent import OpenAICompatibleChatAgent


class QwenAgent(OpenAICompatibleChatAgent):
    provider_id = 'qwen'
    display_name = 'Qwen'
    api_key_env = 'QWEN_API_KEY'
    default_base_url = 'https://dashscope.aliyuncs.com/compatible-mode/v1'
    default_model_name = 'qwen-max'
    system_prompt = 'You are Qwen, a concise financial trading assistant focused on the China futures market.'
