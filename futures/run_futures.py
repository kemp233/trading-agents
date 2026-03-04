"""
FUTURES ONLY - DO NOT IMPORT CRYPTO MODULES

期货独立启动入口
AppID: client_aiagentts_1.0.0
开发人: 方馒涵

启动顺序：StateWriter → EventBus → OutboxDispatcher → CtpAdapter → Streamlit Dashboard

使用方式：
    cd <repo_root>
    python -m futures.run_futures
"""

import asyncio
import subprocess
import sys
from pathlib import Path

import yaml
from loguru import logger

# ── 共用核心层（core/ 与 venue/ 是共享模块，允许导入）────────────────────────
from core.event_bus import EventBus
from core.state_writer import StateWriter
from core.outbox_dispatcher import OutboxDispatcher
from venue.ctp_adapter import CtpAdapter  # stub，#13 实现

# ── 配置加载 ─────────────────────────────────────────────────────────────────
CONFIG_PATH = Path(__file__).parent / "config" / "risk_params_futures.yaml"


def load_config() -> dict:
    """加载期货专用配置文件"""
    if not CONFIG_PATH.exists():
        logger.error(f"配置文件不存在: {CONFIG_PATH}")
        sys.exit(1)
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ── 主启动逻辑 ────────────────────────────────────────────────────────────────
async def main() -> None:
    logger.info("[Futures] 启动期货交易系统...")
    logger.info(f"[Futures] AppID: client_aiagentts_1.0.0")

    config = load_config()
    db_path = config.get("db_path", "./data/futures.db")

    # 1. StateWriter
    logger.info("[Futures] 初始化 StateWriter...")
    state_writer = StateWriter(db_path=db_path)
    await state_writer.initialize()

    # 2. EventBus
    logger.info("[Futures] 初始化 EventBus...")
    event_bus = EventBus(state_writer=state_writer)

    # 3. OutboxDispatcher
    logger.info("[Futures] 初始化 OutboxDispatcher...")
    dispatcher = OutboxDispatcher(
        event_bus=event_bus,
        state_writer=state_writer,
        max_retries=config.get("reconnect_max_retries", 5),
    )

    # 4. CtpAdapter
    # Bug1 修复：原代码用关键字参数调用，与 CTPAdapter.__init__(config: dict) 签名不符
    # 改为将配置项封装为 dict 传入；同时补上原本缺失的 await connect()
    logger.info("[Futures] 初始化 CtpAdapter...")
    ctp_config = {
        "broker_id":  config.get("broker_id", ""),
        "user_id":    config.get("user_id", ""),
        "app_id":     config.get("app_id", "client_aiagentts_1.0.0"),
        "front_addr": config.get("ctp_front_addr", ""),
        # password / auth_code 优先从 yaml 读取，若为空则 fallback 到环境变量
        # CTP_PASSWORD / CTP_AUTH_CODE
        "password":   config.get("password", ""),
        "auth_code":  config.get("auth_code", ""),
    }
    ctp_adapter = CtpAdapter(config=ctp_config, state_writer=state_writer)
    logger.info("[Futures] 连接 CTP 前置机...")
    await ctp_adapter.connect()
    logger.info("[Futures] CTP 连接成功。")

    # 5. 启动 Streamlit Dashboard（#15 实现）
    dashboard_path = Path(__file__).parent.parent / "dashboard" / "app_futures.py"
    if dashboard_path.exists():
        logger.info("[Futures] 启动 Streamlit Dashboard...")
        subprocess.Popen(
            [sys.executable, "-m", "streamlit", "run", str(dashboard_path),
             "--server.port", "8501"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    else:
        logger.warning(f"[Futures] Dashboard 未找到: {dashboard_path}（#15 完成后生效）")

    # 6. 启动事件循环
    logger.info("[Futures] 系统启动完成，进入事件循环...")
    try:
        await dispatcher.run()
    except KeyboardInterrupt:
        logger.info("[Futures] 收到停止信号，正在关闭...")
    finally:
        await ctp_adapter.disconnect()
        await state_writer.close()
        logger.info("[Futures] 已安全关闭。")


if __name__ == "__main__":
    asyncio.run(main())
