"""EventEnvelope — 事件信封 (v3: per-stream 序列号 + 双时间戳)"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
import uuid


@dataclass
class EventEnvelope:
    """所有事件的统一信封
    
    v3 关键改进:
    - stream_id + stream_seq 替代全局 sequence_number
    - event_ts + recv_ts 双时间戳防止 look-ahead bias
    """
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = ''           # 'MarketEvent' | 'Signal' | 'TradeIntent' | ...
    stream_id: str = ''            # 分区键: "{event_type}:{symbol}"
    stream_seq: int = 0            # 流内单调递增
    event_ts: datetime = field(default_factory=datetime.utcnow)  # 源头生成时间
    recv_ts: datetime = field(default_factory=datetime.utcnow)   # 本地接收时间
    producer_id: str = ''          # 生产者标识
    idempotency_key: str = ''      # 业务去重键
    payload: dict = field(default_factory=dict)
    schema_version: str = '1.0'

    def __post_init__(self):
        if not self.idempotency_key:
            self.idempotency_key = self.event_id
