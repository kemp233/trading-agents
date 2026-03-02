"""Event Bus — per-stream 序列号 + 去重 (v3)"""
import asyncio
import logging
from collections import defaultdict
from typing import Callable, Awaitable
from core.event_envelope import EventEnvelope

logger = logging.getLogger(__name__)


class EventBus:
    """进程内事件总线 (Phase 1)
    
    Phase 2 迁移到 Redis Streams 时,
    保持相同接口, 替换底层实现即可。
    """

    def __init__(self):
        self._subscribers: dict[str, list[Callable]] = defaultdict(list)
        self._stream_seqs: dict[str, int] = defaultdict(int)  # per-stream 计数器
        self._processed: set[str] = set()  # 去重表
        self._lock = asyncio.Lock()

    def subscribe(self, event_type: str, handler: Callable[[EventEnvelope], Awaitable]):
        """订阅事件类型, '*' 订阅所有"""
        self._subscribers[event_type].append(handler)

    async def publish(self, envelope: EventEnvelope):
        """发布事件, 自动分配 stream_seq"""
        async with self._lock:
            # 去重检查
            if envelope.idempotency_key in self._processed:
                logger.debug(f"Duplicate event skipped: {envelope.idempotency_key}")
                return

            # 分配 per-stream 序列号
            self._stream_seqs[envelope.stream_id] += 1
            envelope.stream_seq = self._stream_seqs[envelope.stream_id]

            # 记录已处理
            self._processed.add(envelope.idempotency_key)

        # 分发给订阅者
        handlers = (
            self._subscribers.get(envelope.event_type, []) +
            self._subscribers.get('*', [])
        )
        for handler in handlers:
            try:
                await handler(envelope)
            except Exception as e:
                logger.error(
                    f"Handler error for {envelope.event_type}: {e}",
                    exc_info=True
                )

    def get_stream_seq(self, stream_id: str) -> int:
        return self._stream_seqs.get(stream_id, 0)

    def clear_dedup_table(self):
        """定期清理去重表 (由 Orchestrator 调度)"""
        self._processed.clear()
