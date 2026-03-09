from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

from .local_config import LOCAL_PROVIDER_CONFIG_PATH, get_local_provider_settings


@dataclass(slots=True)
class ProviderRequest:
    prompt: str
    context: dict[str, Any] = field(default_factory=dict)
    tools: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    trace_id: str | None = None
    step_id: str | None = None


@dataclass(slots=True)
class ProviderResponse:
    provider_id: str
    display_name: str
    ok: bool
    content: str
    raw: Any = None
    metadata: dict[str, Any] = field(default_factory=dict)
    trace_id: str | None = None
    step_id: str | None = None


@dataclass(slots=True)
class ProviderHealth:
    provider_id: str
    display_name: str
    enabled: bool
    configured: bool
    model_name: str
    base_url: str
    message: str


class BaseProviderAgent:
    provider_id = "base"
    display_name = "基础 Provider"
    api_key_env = ""
    default_base_url = ""
    default_model_name = ""
    default_timeout = 30.0

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self._config = dict(config or {})

    def _local_settings(self) -> dict[str, Any]:
        return get_local_provider_settings(self.provider_id)

    @property
    def enabled(self) -> bool:
        return bool(self._config.get("enabled", True))

    @property
    def base_url(self) -> str:
        local_settings = self._local_settings()
        return str(
            self._config.get("base_url")
            or local_settings.get("base_url")
            or self.default_base_url
        )

    @property
    def model_name(self) -> str:
        local_settings = self._local_settings()
        return str(
            self._config.get("model_name")
            or local_settings.get("model_name")
            or self.default_model_name
        )

    @property
    def timeout(self) -> float:
        local_settings = self._local_settings()
        return float(
            self._config.get("timeout")
            or local_settings.get("timeout")
            or self.default_timeout
        )

    @property
    def api_key(self) -> str:
        if self._config.get("api_key"):
            return str(self._config["api_key"])
        local_settings = self._local_settings()
        if local_settings.get("api_key"):
            candidate = str(local_settings["api_key"]).strip()
            if candidate and not candidate.lower().startswith("paste-"):
                return candidate
        if not self.api_key_env:
            return ""
        return os.environ.get(self.api_key_env, "")

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def build_request(
        self,
        prompt: str,
        context: dict[str, Any] | None = None,
        tools: list[dict[str, Any]] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ProviderRequest:
        meta = dict(metadata or {})
        return ProviderRequest(
            prompt=prompt,
            context=dict(context or {}),
            tools=list(tools or []),
            metadata=meta,
            trace_id=meta.get("trace_id"),
            step_id=meta.get("step_id"),
        )

    def invoke(self, request: ProviderRequest) -> ProviderResponse:
        if not self.enabled:
            return ProviderResponse(
                provider_id=self.provider_id,
                display_name=self.display_name,
                ok=False,
                content=f"{self.display_name} 已被禁用。",
                metadata={"reason": "disabled"},
                trace_id=request.trace_id,
                step_id=request.step_id,
            )
        if not self.is_configured():
            return ProviderResponse(
                provider_id=self.provider_id,
                display_name=self.display_name,
                ok=False,
                content=f"{self.display_name} 尚未配置 API Key。",
                metadata={
                    "reason": "missing_api_key",
                    "api_key_env": self.api_key_env,
                    "local_config_path": str(LOCAL_PROVIDER_CONFIG_PATH),
                },
                trace_id=request.trace_id,
                step_id=request.step_id,
            )
        return ProviderResponse(
            provider_id=self.provider_id,
            display_name=self.display_name,
            ok=False,
            content=f"{self.display_name} 已预留调用位，但 issue9 暂未接入真实在线调用。",
            metadata={"reason": "not_implemented"},
            trace_id=request.trace_id,
            step_id=request.step_id,
        )

    def normalize_response(self, raw: Any) -> ProviderResponse:
        if isinstance(raw, ProviderResponse):
            return raw
        if isinstance(raw, dict):
            return ProviderResponse(
                provider_id=self.provider_id,
                display_name=self.display_name,
                ok=bool(raw.get("ok", True)),
                content=str(raw.get("content", "")),
                raw=raw,
                metadata=dict(raw.get("metadata", {})),
                trace_id=raw.get("trace_id"),
                step_id=raw.get("step_id"),
            )
        return ProviderResponse(
            provider_id=self.provider_id,
            display_name=self.display_name,
            ok=True,
            content=str(raw),
            raw=raw,
        )

    def health_check(self) -> ProviderHealth:
        configured = self.is_configured()
        if not self.enabled:
            message = "已禁用"
        elif configured:
            message = f"已配置（来源：环境变量或 {LOCAL_PROVIDER_CONFIG_PATH.name}）"
        else:
            message = f"未配置：请填写 {self.api_key_env} 或 {LOCAL_PROVIDER_CONFIG_PATH.name}"
        return ProviderHealth(
            provider_id=self.provider_id,
            display_name=self.display_name,
            enabled=self.enabled,
            configured=configured,
            model_name=self.model_name,
            base_url=self.base_url,
            message=message,
        )

