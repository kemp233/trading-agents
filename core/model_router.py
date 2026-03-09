"""Model router and provider registry for workflow-capable agent orchestration."""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from typing import Any

from agents.providers import BaseProviderAgent, ProviderHealth, create_default_provider_agents
from agents.providers.local_config import load_local_workflow_assignments

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class WorkflowAssignment:
    workflow_role: str
    provider_ids: list[str] | None = None
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["provider_ids"] = list(self.provider_ids or [])
        payload["metadata"] = dict(self.metadata or {})
        return payload


class ProviderRegistry:
    def __init__(
        self,
        providers: list[BaseProviderAgent] | None = None,
        overrides: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        default_providers = providers or create_default_provider_agents(overrides)
        self._providers = {provider.provider_id: provider for provider in default_providers}

    def get_provider(self, provider_id: str) -> BaseProviderAgent | None:
        return self._providers.get(provider_id)

    def list_providers(self) -> list[BaseProviderAgent]:
        return list(self._providers.values())

    def list_provider_health(self) -> list[ProviderHealth]:
        return [provider.health_check() for provider in self.list_providers()]


class WorkflowAssignmentRegistry:
    def __init__(self, assignments: dict[str, Any] | None = None) -> None:
        self._assignments: dict[str, WorkflowAssignment] = {}
        for role, config in dict(assignments or {}).items():
            self._assignments[role] = self._normalize_assignment(role, config)

    def _normalize_assignment(self, workflow_role: str, config: Any) -> WorkflowAssignment:
        if isinstance(config, WorkflowAssignment):
            return config
        if isinstance(config, str):
            return WorkflowAssignment(workflow_role=workflow_role, provider_ids=[config], metadata={})
        if isinstance(config, dict):
            metadata = dict(config)
            provider_ids_raw = metadata.pop("provider_ids", None)
            provider_id_raw = metadata.pop("provider_id", None)
            provider_ids: list[str] = []
            if isinstance(provider_ids_raw, list):
                provider_ids = [str(item) for item in provider_ids_raw if str(item).strip()]
            elif provider_ids_raw is not None:
                provider_ids = [str(provider_ids_raw)]
            elif provider_id_raw is not None:
                provider_ids = [str(provider_id_raw)]
            return WorkflowAssignment(workflow_role=workflow_role, provider_ids=provider_ids, metadata=metadata)
        if isinstance(config, list):
            return WorkflowAssignment(
                workflow_role=workflow_role,
                provider_ids=[str(item) for item in config if str(item).strip()],
                metadata={},
            )
        return WorkflowAssignment(workflow_role=workflow_role, provider_ids=[], metadata={})

    def assign(self, workflow_role: str, provider_ids: list[str] | None, metadata: dict[str, Any] | None = None) -> None:
        normalized_ids = [str(item) for item in (provider_ids or []) if str(item).strip()]
        if not normalized_ids and not metadata:
            self._assignments.pop(workflow_role, None)
            return
        self._assignments[workflow_role] = WorkflowAssignment(
            workflow_role=workflow_role,
            provider_ids=normalized_ids,
            metadata=dict(metadata or {}),
        )

    def get_assignment(self, workflow_role: str) -> WorkflowAssignment:
        return self._assignments.get(
            workflow_role,
            WorkflowAssignment(workflow_role=workflow_role, provider_ids=[], metadata={}),
        )

    def list_assignments(self) -> list[WorkflowAssignment]:
        return [self._assignments[key] for key in sorted(self._assignments)]


class ModelRouter:
    """Keep route hooks while exposing provider/workflow registries for issue9."""

    def __init__(self, config: dict[str, Any] | None):
        self._config = dict(config or {})
        self._llm_enabled = bool(self._config.get("llm_enabled", False))
        provider_overrides = self._config.get("provider_overrides", {})
        workflow_assignments = dict(self._config.get("workflow_assignments", {}))
        workflow_assignments.update(load_local_workflow_assignments())
        self.provider_registry = ProviderRegistry(overrides=provider_overrides)
        self.workflow_assignments = WorkflowAssignmentRegistry(workflow_assignments)

    def route(self, route_id: str, **kwargs):
        router_map = {
            "R0": self._r0_data_validation,
            "R1": self._r1_technical_signal,
            "R2": self._r2_news_classification,
            "R3": self._r3_semantic_validation,
            "R4": self._r4_strategy_approval,
            "R5": self._r5_post_review,
            "R6": self._r6_fault_handling,
        }
        handler = router_map.get(route_id)
        if not handler:
            raise ValueError(f"Unknown route: {route_id}")
        return handler(**kwargs)

    def list_provider_health(self) -> list[dict[str, Any]]:
        return [asdict(item) for item in self.provider_registry.list_provider_health()]

    def list_workflow_assignments(self) -> list[dict[str, Any]]:
        return [assignment.to_dict() for assignment in self.workflow_assignments.list_assignments()]

    def assign_provider(
        self,
        workflow_role: str,
        provider_id: str | None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        provider_ids = [provider_id] if provider_id is not None else []
        self.assign_providers(workflow_role, provider_ids, metadata)

    def assign_providers(
        self,
        workflow_role: str,
        provider_ids: list[str] | None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        normalized_ids = [str(item) for item in (provider_ids or []) if str(item).strip()]
        for provider_id in normalized_ids:
            if self.provider_registry.get_provider(provider_id) is None:
                raise ValueError(f"Unknown provider: {provider_id}")
        self.workflow_assignments.assign(workflow_role, normalized_ids, metadata)

    def _r0_data_validation(self, **kwargs):
        return {"route": "R0", "mode": "rule-only", "metadata": kwargs}

    def _r1_technical_signal(self, **kwargs):
        return {"route": "R1", "mode": "rule-only", "metadata": kwargs}

    def _r2_news_classification(self, **kwargs):
        if self._llm_enabled:
            raise RuntimeError("R2 cannot run during trading hours")
        return {"route": "R2", "mode": "offline-provider-slot", "metadata": kwargs}

    def _r3_semantic_validation(self, **kwargs):
        return {"route": "R3", "mode": "rule-only", "metadata": kwargs}

    def _r4_strategy_approval(self, **kwargs):
        if self._llm_enabled:
            raise RuntimeError("R4 is offline-only")
        return {"route": "R4", "mode": "offline-provider-slot", "metadata": kwargs}

    def _r5_post_review(self, **kwargs):
        return {"route": "R5", "mode": "offline-provider-slot", "metadata": kwargs}

    def _r6_fault_handling(self, fault_type: str, **kwargs):
        return {
            "route": "R6",
            "fault_type": fault_type,
            "mode": "degradation-policy",
            "metadata": kwargs,
        }
