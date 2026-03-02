"""Orchestrator — 编排心跳、调度、状态机驱动"""
import asyncio
import logging

logger = logging.getLogger(__name__)


class Orchestrator:
    """系统编排器 — 所有 Agent 的生命周期管理"""

    def __init__(self, event_bus, state_writer, config):
        self._event_bus = event_bus
        self._state_writer = state_writer
        self._config = config
        self._agents = {}
        self._running = False

    def register_agent(self, name: str, agent):
        self._agents[name] = agent

    async def start(self):
        self._running = True
        logger.info(f"Orchestrator starting with {len(self._agents)} agents")
        for name, agent in self._agents.items():
            await agent.start()
            logger.info(f"  ✅ {name} started")
        asyncio.create_task(self._heartbeat_loop())

    async def stop(self):
        self._running = False
        for name, agent in self._agents.items():
            await agent.stop()
            logger.info(f"  🛑 {name} stopped")

    async def _heartbeat_loop(self):
        while self._running:
            for name, agent in self._agents.items():
                try:
                    health = await agent.health_check()
                    if not health.ok:
                        logger.warning(f"{name} unhealthy: {health.reason}")
                except Exception as e:
                    logger.error(f"{name} health_check failed: {e}")
            await asyncio.sleep(10)
