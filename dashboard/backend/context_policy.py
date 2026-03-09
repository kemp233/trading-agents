from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "context_policy.yaml"


@dataclass(slots=True)
class ContextPolicy:
    role: str
    allowed_sources: list[str]
    denied_sources: list[str]
    lookback_days: int
    allow_active_chat: bool
    allow_daily_summaries: bool
    allow_daily_fact_snapshots: bool
    max_items: int
    max_chars: int
    max_tokens_estimate: int
    summary_first: bool


def _load_config() -> dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {}
    payload = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
    return payload if isinstance(payload, dict) else {}


def get_policy(role: str) -> ContextPolicy:
    config = _load_config()
    defaults = dict(config.get("defaults", {}))
    role_cfg = dict(config.get(role, {}))
    merged = {**defaults, **role_cfg}
    return ContextPolicy(
        role=role,
        allowed_sources=list(merged.get("allowed_sources", [])),
        denied_sources=list(merged.get("denied_sources", [])),
        lookback_days=int(merged.get("lookback_days", 1)),
        allow_active_chat=bool(merged.get("allow_active_chat", False)),
        allow_daily_summaries=bool(merged.get("allow_daily_summaries", True)),
        allow_daily_fact_snapshots=bool(merged.get("allow_daily_fact_snapshots", True)),
        max_items=max(1, int(merged.get("max_items", 6))),
        max_chars=max(256, int(merged.get("max_chars", 2048))),
        max_tokens_estimate=max(128, int(merged.get("max_tokens_estimate", 768))),
        summary_first=bool(merged.get("summary_first", True)),
    )


def trim_context_items(items: list[dict[str, Any]], policy: ContextPolicy) -> list[dict[str, Any]]:
    budget_chars = policy.max_chars
    trimmed: list[dict[str, Any]] = []
    ordered = list(items)
    if policy.summary_first:
        ordered.sort(key=lambda item: (0 if item.get("kind") in {"daily_fact_snapshot", "daily_summary"} else 1, item.get("priority", 0)))
    for item in ordered:
        content = json.dumps(item, ensure_ascii=False)
        if len(trimmed) >= policy.max_items:
            break
        if budget_chars - len(content) < 0:
            continue
        trimmed.append(item)
        budget_chars -= len(content)
    return trimmed
