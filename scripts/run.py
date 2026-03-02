"""Trading Agents — 启动入口"""
import asyncio
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger('trading-agents')


async def main():
    logger.info("🚀 Trading Agents v3 starting...")

    # TODO Phase 1: 初始化各组件
    # 1. StateWriter (SQLite + WAL)
    # 2. EventBus
    # 3. OutboxDispatcher
    # 4. VenueAdapter (Binance)
    # 5. Agents (Market Data, TA, Strategy, Risk, Order, Reconciler, Portfolio)
    # 6. Orchestrator
    # 7. Dashboard (Streamlit MVP)
    # 8. Telegram Bot

    logger.info("✅ All agents registered")
    logger.info("📡 System running... Press Ctrl+C to stop")

    try:
        await asyncio.Event().wait()  # 永久等待
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down...")


if __name__ == '__main__':
    asyncio.run(main())
