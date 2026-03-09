from __future__ import annotations

import socket
from typing import Any

import requests

from .base import BaseProviderAgent, ProviderRequest, ProviderResponse


class DoubaoAgent(BaseProviderAgent):
    provider_id = "doubao"
    display_name = "Doubao"
    api_key_env = "DOUBAO_API_KEY"
    default_base_url = "https://ark.cn-beijing.volces.com/api/v3"
    default_model_name = "doubao-pro"

    def invoke(self, request: ProviderRequest) -> ProviderResponse:
        payload = {
            "model": self.model_name,
            "input": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": request.prompt,
                        }
                    ],
                }
            ],
        }

        diagnostics = self._connection_diagnostics()
        if diagnostics.get("dns_suspicious"):
            return ProviderResponse(
                provider_id=self.provider_id,
                display_name=self.display_name,
                ok=False,
                content=self._network_error_message(RuntimeError('suspicious_dns'), diagnostics),
                raw='suspicious_dns',
                metadata={"reason": "network_error", **diagnostics},
                trace_id=request.trace_id,
                step_id=request.step_id,
            )
        try:
            resp = requests.post(
                f"{self.base_url.rstrip('/')}/responses",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=min(float(self.timeout), 12.0),
            )
        except requests.exceptions.RequestException as exc:
            return ProviderResponse(
                provider_id=self.provider_id,
                display_name=self.display_name,
                ok=False,
                content=self._network_error_message(exc, diagnostics),
                raw=str(exc),
                metadata={"reason": "network_error", **diagnostics},
                trace_id=request.trace_id,
                step_id=request.step_id,
            )

        if not resp.ok:
            return ProviderResponse(
                provider_id=self.provider_id,
                display_name=self.display_name,
                ok=False,
                content=f"{self.display_name} call failed: HTTP {resp.status_code}",
                raw=resp.text,
                metadata={"reason": "http_error", "status_code": resp.status_code, **diagnostics},
                trace_id=request.trace_id,
                step_id=request.step_id,
            )

        raw = resp.json()
        return ProviderResponse(
            provider_id=self.provider_id,
            display_name=self.display_name,
            ok=True,
            content=self._extract_text(raw),
            raw=raw,
            metadata={"reason": "live_response", **diagnostics},
            trace_id=request.trace_id,
            step_id=request.step_id,
        )

    def _extract_text(self, raw: dict[str, Any]) -> str:
        output_text = raw.get("output_text")
        if isinstance(output_text, str) and output_text.strip():
            return output_text.strip()
        output = raw.get("output")
        if isinstance(output, list):
            chunks: list[str] = []
            for item in output:
                if not isinstance(item, dict):
                    continue
                content = item.get("content")
                if not isinstance(content, list):
                    continue
                for part in content:
                    if not isinstance(part, dict):
                        continue
                    text = part.get("text") or part.get("output_text")
                    if isinstance(text, str) and text.strip():
                        chunks.append(text.strip())
            if chunks:
                return "\n".join(chunks)
        return "Doubao returned a response, but no text content was extracted."

    def _connection_diagnostics(self) -> dict[str, Any]:
        host = self.base_url.split("//", 1)[-1].split("/", 1)[0]
        try:
            infos = socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)
            resolved_ips: list[str] = []
            for info in infos:
                ip = info[4][0]
                if ip not in resolved_ips:
                    resolved_ips.append(ip)
        except OSError:
            resolved_ips = []
        return {
            "resolved_ips": resolved_ips,
            "dns_suspicious": any(ip.startswith("198.18.") for ip in resolved_ips),
        }

    def _network_error_message(self, exc: Exception, diagnostics: dict[str, Any]) -> str:
        if diagnostics.get("dns_suspicious"):
            resolved = ", ".join(diagnostics.get("resolved_ips") or []) or "unknown"
            return f"{self.display_name} call failed: suspicious DNS resolution ({resolved}). Current machine cannot reach Ark over a valid network path."
        return f"{self.display_name} call failed: {exc}"
