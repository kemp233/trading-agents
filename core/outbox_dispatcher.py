"""Outbox Dispatcher — 原子下单的发送协程 (v3 核心组件)

设计:
- 轮询 outbox_orders 表中 status='NEW' 的记录
- 通过 VenueAdapter 发送订单
- 用交易所回执更新状态
- 崩溃安全: 重启后自动补发未确认订单
- 幂等: 交易所侧用 clientOrderId 去重
"""
import asyncio
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class OutboxDispatcher:
    def __init__(self, state_writer, venue_adapter, config):
        self._state_writer = state_writer
        self._venue_adapter = venue_adapter
        self._poll_interval = config.get('poll_interval_sec', 1)
        self._max_retries = config.get('max_retries', 3)
        self._backoff_base = config.get('retry_backoff_base_sec', 5)
        self._running = False

    async def start(self):
        self._running = True
        asyncio.create_task(self._dispatch_loop())
        logger.info("OutboxDispatcher started")

    async def stop(self):
        self._running = False

    async def _dispatch_loop(self):
        while self._running:
            try:
                pending = await self._fetch_pending()
                for record in pending:
                    await self._process_record(record)
            except Exception as e:
                logger.error(f"OutboxDispatcher error: {e}")
            await asyncio.sleep(self._poll_interval)

    async def _fetch_pending(self) -> list:
        """获取待发送的 outbox 记录"""
        reader = self._state_writer.get_reader()
        try:
            cursor = reader.execute(
                "SELECT * FROM outbox_orders "
                "WHERE status IN ('NEW', 'RETRY') "
                "AND retry_count < ? "
                "ORDER BY created_at ASC LIMIT 10",
                (self._max_retries,)
            )
            return cursor.fetchall()
        finally:
            reader.close()

    async def _process_record(self, record):
        """处理单条 outbox 记录"""
        event_id = record['event_id']
        payload = json.loads(record['payload'])
        event_type = record['event_type']

        try:
            if event_type == 'OrderSubmit':
                receipt = await self._venue_adapter.submit_order(payload)
            elif event_type == 'OrderCancel':
                receipt = await self._venue_adapter.cancel_order(
                    payload['client_order_id']
                )
            else:
                logger.warning(f"Unknown event_type: {event_type}")
                return

            # 成功: 更新 outbox 状态
            await self._mark_confirmed(event_id, receipt)
            logger.info(f"Outbox {event_id} confirmed: {receipt}")

        except Exception as e:
            # 失败: 增加重试计数
            await self._mark_retry(event_id, str(e))
            backoff = self._backoff_base * (2 ** record['retry_count'])
            logger.warning(
                f"Outbox {event_id} failed (retry {record['retry_count']+1}), "
                f"backoff {backoff}s: {e}"
            )
            await asyncio.sleep(min(backoff, 300))

    async def _mark_confirmed(self, event_id: str, receipt):
        def _update(db):
            db.execute(
                "UPDATE outbox_orders SET status='CONFIRMED', "
                "sent_at=? WHERE event_id=?",
                (datetime.utcnow().isoformat(), event_id)
            )
        await self._state_writer.write(_update)

    async def _mark_retry(self, event_id: str, error_msg: str):
        def _update(db):
            db.execute(
                "UPDATE outbox_orders SET status='RETRY', "
                "retry_count=retry_count+1 WHERE event_id=?",
                (event_id,)
            )
        await self._state_writer.write(_update)
