from __future__ import annotations

import json
from pathlib import Path
from typing import Any


CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"
LOCAL_PROVIDER_CONFIG_PATH = CONFIG_DIR / "provider_secrets.local.json"
LOCAL_WORKFLOW_ASSIGNMENTS_PATH = CONFIG_DIR / "workflow_assignments.local.json"


def _load_json_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def load_local_provider_config() -> dict[str, dict[str, Any]]:
    payload = _load_json_dict(LOCAL_PROVIDER_CONFIG_PATH)
    normalized: dict[str, dict[str, Any]] = {}
    for provider_id, config in payload.items():
        if isinstance(provider_id, str) and isinstance(config, dict):
            normalized[provider_id] = dict(config)
    return normalized


def get_local_provider_settings(provider_id: str) -> dict[str, Any]:
    return dict(load_local_provider_config().get(provider_id, {}))


def load_local_workflow_assignments() -> dict[str, dict[str, Any]]:
    payload = _load_json_dict(LOCAL_WORKFLOW_ASSIGNMENTS_PATH)
    normalized: dict[str, dict[str, Any]] = {}
    for role, config in payload.items():
        if isinstance(role, str) and isinstance(config, dict):
            normalized[role] = dict(config)
    return normalized

