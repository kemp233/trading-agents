from __future__ import annotations

from core.model_router import ModelRouter


def test_model_router_discovers_six_provider_slots() -> None:
    router = ModelRouter(config={})

    health = router.list_provider_health()

    assert {item["provider_id"] for item in health} == {
        "codex",
        "perplexity-pro",
        "deepseek-chat",
        "deepseek-reasoner",
        "doubao",
        "qwen",
    }


def test_model_router_loads_multi_provider_assignments_and_allows_override() -> None:
    router = ModelRouter(config={})

    assignments = router.list_workflow_assignments()
    orchestration = next(item for item in assignments if item["workflow_role"] == "orchestration")
    assert orchestration["provider_ids"] == ["qwen", "deepseek-chat"]

    router.assign_providers("research", ["deepseek-chat", "qwen"], {"api_interfaces": ["chat_completion", "chat_completion"]})

    updated = router.workflow_assignments.get_assignment("research").to_dict()
    assert updated == {
        "workflow_role": "research",
        "provider_ids": ["deepseek-chat", "qwen"],
        "metadata": {"api_interfaces": ["chat_completion", "chat_completion"]},
    }
