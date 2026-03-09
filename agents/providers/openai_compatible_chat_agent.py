from __future__ import annotations

import json
from typing import Any
from urllib import request as urllib_request
from urllib.error import HTTPError, URLError

from .base import BaseProviderAgent, ProviderRequest, ProviderResponse


class OpenAICompatibleChatAgent(BaseProviderAgent):
    """Simple OpenAI-compatible chat/completions provider wrapper."""

    system_prompt = 'You are a helpful financial trading assistant.'

    def invoke(self, request: ProviderRequest) -> ProviderResponse:
        payload = {
            'model': self.model_name,
            'messages': [
                {'role': 'system', 'content': self.system_prompt},
                {'role': 'user', 'content': request.prompt},
            ],
        }
        body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        req = urllib_request.Request(
            f"{self.base_url.rstrip('/')}/chat/completions",
            data=body,
            headers={
                'Authorization': f'Bearer {self.api_key}',
                'Content-Type': 'application/json',
            },
            method='POST',
        )
        try:
            with urllib_request.urlopen(req, timeout=self.timeout) as resp:
                raw = json.loads(resp.read().decode('utf-8'))
        except HTTPError as exc:
            detail = exc.read().decode('utf-8', errors='ignore')
            return ProviderResponse(
                provider_id=self.provider_id,
                display_name=self.display_name,
                ok=False,
                content=f'{self.display_name} ?????HTTP {exc.code}',
                raw=detail,
                metadata={'reason': 'http_error', 'status_code': exc.code},
                trace_id=request.trace_id,
                step_id=request.step_id,
            )
        except URLError as exc:
            return ProviderResponse(
                provider_id=self.provider_id,
                display_name=self.display_name,
                ok=False,
                content=f'{self.display_name} ?????{exc.reason}',
                raw=str(exc),
                metadata={'reason': 'network_error'},
                trace_id=request.trace_id,
                step_id=request.step_id,
            )
        except Exception as exc:
            return ProviderResponse(
                provider_id=self.provider_id,
                display_name=self.display_name,
                ok=False,
                content=f'{self.display_name} ?????{exc}',
                raw=str(exc),
                metadata={'reason': 'unexpected_error'},
                trace_id=request.trace_id,
                step_id=request.step_id,
            )

        return ProviderResponse(
            provider_id=self.provider_id,
            display_name=self.display_name,
            ok=True,
            content=self._extract_text(raw),
            raw=raw,
            metadata={'reason': 'live_response'},
            trace_id=request.trace_id,
            step_id=request.step_id,
        )

    def _extract_text(self, raw: dict[str, Any]) -> str:
        choices = raw.get('choices')
        if isinstance(choices, list) and choices:
            first = choices[0]
            if isinstance(first, dict):
                message = first.get('message')
                if isinstance(message, dict):
                    content = message.get('content')
                    if isinstance(content, str) and content.strip():
                        return content.strip()
        return f'{self.display_name} ????????????????'
