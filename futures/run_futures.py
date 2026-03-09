"""
FUTURES ONLY - DO NOT IMPORT CRYPTO MODULES

Standalone startup for the CTP futures workflow.
"""

import asyncio
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite
import yaml
from loguru import logger

from core.state_schema import SystemLogEntry
from core.state_writer import StateWriter
from core.outbox_dispatcher import OutboxDispatcher
from venue.ctp_adapter import CtpAdapter
from venue.ctp_md_gateway import CtpMdGateway
from venue.ctp_utils import build_ctp_runtime_config

REPO_ROOT = Path(__file__).parent.parent
CONFIG_PATH = Path(__file__).parent / "config" / "risk_params_futures.yaml"
SCHEMA_PATH = REPO_ROOT / "db" / "schema.sql"
DASHBOARD_PATH = REPO_ROOT / "dashboard" / "streamlit_mvp" / "app.py"


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        logger.error(f"Config file not found: {CONFIG_PATH}")
        sys.exit(1)
    with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh) or {}

    config["broker_id"] = os.getenv("CTP_BROKER_ID", config.get("broker_id", ""))
    config["user_id"] = os.getenv("CTP_USER_ID", config.get("user_id", ""))
    config["password"] = os.getenv("CTP_PASSWORD", config.get("password", ""))
    config["ctp_td_front_addr"] = os.getenv(
        "CTP_TD_FRONT",
        config.get("ctp_td_front_addr") or config.get("ctp_front_addr", ""),
    )
    config["ctp_md_front_addr"] = os.getenv(
        "CTP_MD_FRONT",
        config.get("ctp_md_front_addr") or config.get("ctp_md_front_addr", ""),
    )
    config["ctp_counter_env"] = os.getenv(
        "CTP_COUNTER_ENV",
        config.get("ctp_counter_env", "实盘"),
    )
    config["db_path"] = config.get("db_path", "./data/trading.db")
    return config


async def init_database(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    if not SCHEMA_PATH.exists():
        logger.error(f"Schema file not found: {SCHEMA_PATH}")
        sys.exit(1)
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    async with aiosqlite.connect(db_path) as db:
        await db.executescript(schema_sql)
        await db.commit()
    logger.info("[Futures] database initialized: %s", db_path)


async def main() -> None:
    logger.info("[Futures] starting futures trading stack")

    config = load_config()
    db_path = config["db_path"]
    default_symbols = list(config.get("default_subscribe_symbols", []))

    runtime_config = build_ctp_runtime_config(config)
    logger.info(
        "[Futures] broker=%s user=%s td=%s md=%s env=%s",
        runtime_config["broker_id"],
        runtime_config["user_id"],
        runtime_config["ctp_td_front_addr"],
        runtime_config["ctp_md_front_addr"],
        runtime_config["ctp_counter_env"],
    )

    await init_database(db_path)

    state_writer = StateWriter(db_path=db_path)
    await state_writer.start()
    await state_writer.write_system_log(
        SystemLogEntry(
            ts=datetime.now(timezone.utc),
            event_type="STARTUP",
            detail="futures.run_futures",
        )
    )

    ctp_adapter = CtpAdapter(config=runtime_config, state_writer=state_writer)
    md_gateway = CtpMdGateway(config=runtime_config, gateway_wrapper=ctp_adapter.gateway_wrapper)

    dispatcher: OutboxDispatcher | None = None
    try:
        logger.info("[Futures] connecting TD")
        await ctp_adapter.connect()

        logger.info("[Futures] querying account snapshot")
        await ctp_adapter.query_account()

        logger.info("[Futures] querying positions snapshot")
        await ctp_adapter.query_positions()

        logger.info("[Futures] connecting MD")
        await md_gateway.connect(default_symbols or None)

        logger.info("[Futures] starting outbox dispatcher")
        dispatcher = OutboxDispatcher(
            state_writer=state_writer,
            venue_adapter=ctp_adapter,
            poll_interval=0.5,
            max_retries=config.get("reconnect_max_retries", 5),
            instrument_config_path=str(REPO_ROOT / "futures" / "config" / "instruments_cn.yaml"),
            semantic_config={
                **config,
                "require_account_snapshot": True,
            },
        )
        await dispatcher.start()

        if DASHBOARD_PATH.exists():
            logger.info("[Futures] starting Streamlit dashboard")
            env = os.environ.copy()
            env["AIAGENTTS_DB"] = str(Path(db_path).resolve())
            subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "streamlit",
                    "run",
                    str(DASHBOARD_PATH),
                    "--server.port",
                    "8501",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=env,
            )
        else:
            logger.warning("[Futures] dashboard not found: %s", DASHBOARD_PATH)

        logger.info("[Futures] startup complete")
        await asyncio.Event().wait()
    except Exception:
        logger.exception("[Futures] startup failed")
        raise
    finally:
        if dispatcher is not None:
            await dispatcher.stop()
        await md_gateway.disconnect()
        await ctp_adapter.disconnect()
        await state_writer.write_system_log(
            SystemLogEntry(
                ts=datetime.now(timezone.utc),
                event_type="SHUTDOWN",
                detail="futures.run_futures",
            )
        )
        await state_writer.stop()


if __name__ == "__main__":
    asyncio.run(main())

