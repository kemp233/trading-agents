from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Callable, Dict, List, Set

from .event_envelope import EventEnvelope
from .state_writer import StateWriter

logger = logging.getLogger(__name__)


class EventBus:
    def __init__(self, state_writer: StateWriter, checkpoint_interval: int = 100, dedup_cache_size: int = 10000) -> None:
        self._state_writer = state_writer
        self._checkpoint_interval = checkpoint_interval
        self._dedup_cache_size = dedup_cache_size
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._processed_keys: Set[str] = set()
        self._stream_sequences: Dict[str, int] = {}
        self._event_count: int = 0
        self._lock: asyncio.Lock = asyncio.Lock()

    async def start(self) -> None:
        await self._load_checkpoint()

    def subscribe(self, event_type: str, handler: Callable) -> None:
        self._subscribers[event_type].append(handler)

    async def publish(self, envelope: EventEnvelope) -> bool:
        async with self._lock:
            try:
                envelope.validate()
            except Exception:
                return False

            if self._is_duplicate(envelope):
                return False

            known_seq = self._stream_sequences.get(envelope.stream_id, -1)
            if envelope.stream_seq < known_seq:
                return False

            self._processed_keys.add(envelope.idempotency_key)
            self._stream_sequences[envelope.stream_id] = envelope.stream_seq
            self._event_count += 1

            if self._event_count % self._checkpoint_interval == 0:
                asyncio.create_task(self._save_checkpoint())

        await self._notify_subscribers(envelope)
        return True

    def _is_duplicate(self, envelope: EventEnvelope) -> bool:
        if envelope.idempotency_key in self._processed_keys:
            return True
        known_seq = self._stream_sequences.get(envelope.stream_id, -1)
        if envelope.stream_seq <= known_seq:
            return True
        return False

    async def _notify_subscribers(self, envelope: EventEnvelope) -> None:
        handlers = self._subscribers.get(envelope.event_type, [])
        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(envelope)
                else:
                    handler(envelope)
            except Exception as e:
                logger.exception(f"Handler {handler.__name__} failed for event {envelope.event_type}: {e}")

    async def _save_checkpoint(self) -> None:
        await self._state_writer.save_checkpoint(self._stream_sequences, self._processed_keys)

    async def _load_checkpoint(self) -> None:
        self._stream_sequences = await self._state_writer.load_checkpoints()
        self._processed_keys = await self._state_writer.load_processed_events(self._dedup_cache_size)

    async def _cleanup_dedup_cache(self) -> None:
        if len(self._processed_keys) > self._dedup_cache_size:
            self._processed_keys = await self._state_writer.load_processed_events(self._dedup_cache_size)
