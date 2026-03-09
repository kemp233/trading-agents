from __future__ import annotations

import os
from pathlib import Path

import yaml
from vnpy.trader.constant import Direction, Exchange, Offset, OrderType, Status
from vnpy.trader.object import AccountData, PositionData


_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_INSTRUMENTS_PATH = _REPO_ROOT / "futures" / "config" / "instruments_cn.yaml"
_COUNTER_ENV_ALLOWED = {"实盘", "测试"}


def build_ctp_runtime_config(config: dict) -> dict:
    counter_env = (
        config.get("ctp_counter_env")
        or os.getenv("CTP_COUNTER_ENV")
        or "实盘"
    )
    if counter_env not in _COUNTER_ENV_ALLOWED:
        allowed = ", ".join(sorted(_COUNTER_ENV_ALLOWED))
        raise ValueError(f"ctp_counter_env must be one of {allowed}, got {counter_env!r}")

    td_front = (
        config.get("ctp_td_front_addr")
        or config.get("front_addr")
        or os.getenv("CTP_TD_FRONT")
        or os.getenv("CTP_FRONT_ADDR")
        or ""
    )
    md_front = (
        config.get("ctp_md_front_addr")
        or config.get("md_front_addr")
        or os.getenv("CTP_MD_FRONT")
        or td_front
    )

    normalized = {
        "broker_id": config.get("broker_id") or os.getenv("CTP_BROKER_ID", ""),
        "user_id": config.get("user_id") or os.getenv("CTP_USER_ID", ""),
        "password": config.get("password") or os.getenv("CTP_PASSWORD", ""),
        "app_id": config.get("app_id", "simnow_client_test"),
        "auth_code": config.get("auth_code") or os.getenv("CTP_AUTH_CODE", ""),
        "ctp_td_front_addr": td_front,
        "ctp_md_front_addr": md_front,
        "ctp_counter_env": counter_env,
    }

    required = (
        "broker_id",
        "user_id",
        "app_id",
        "auth_code",
        "ctp_td_front_addr",
        "ctp_md_front_addr",
        "ctp_counter_env",
    )
    missing = [key for key in required if not normalized[key]]
    if missing:
        raise ValueError(f"Missing required CTP config fields: {', '.join(missing)}")

    return normalized


def build_vnpy_setting(config: dict) -> dict:
    runtime = build_ctp_runtime_config(config)
    return {
        "用户名": runtime["user_id"],
        "密码": runtime["password"],
        "经纪商代码": runtime["broker_id"],
        "交易服务器": runtime["ctp_td_front_addr"],
        "行情服务器": runtime["ctp_md_front_addr"],
        "产品名称": runtime["app_id"],
        "授权编码": runtime["auth_code"],
        "柜台环境": runtime["ctp_counter_env"],
    }


def load_instrument_exchange_map(config_path: Path | None = None) -> dict[str, Exchange]:
    config_path = config_path or _DEFAULT_INSTRUMENTS_PATH
    with config_path.open("r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh) or {}

    mapping: dict[str, Exchange] = {}
    for instrument in payload.get("instruments", []):
        symbol = instrument.get("symbol")
        exchange = instrument.get("exchange")
        if symbol and exchange:
            mapping[str(symbol)] = Exchange(str(exchange))
    return mapping


def status_to_receipt(status: Status) -> str:
    if status in {Status.SUBMITTING, Status.NOTTRADED}:
        return "SENT"
    if status == Status.PARTTRADED:
        return "PARTIALLY_FILLED"
    if status == Status.ALLTRADED:
        return "FILLED"
    if status == Status.CANCELLED:
        return "CANCELED"
    if status == Status.REJECTED:
        return "REJECTED"
    return "FAILED"


def side_to_direction(side: str) -> Direction:
    if side == "BUY":
        return Direction.LONG
    if side == "SELL":
        return Direction.SHORT
    raise ValueError(f"Unsupported side: {side}")


def order_type_to_vnpy(order_type: str, time_in_force: str) -> OrderType:
    if order_type == "MARKET":
        return OrderType.MARKET
    if order_type == "STOP":
        return OrderType.STOP
    if time_in_force == "FOK":
        return OrderType.FOK
    if time_in_force == "IOC":
        return OrderType.FAK
    return OrderType.LIMIT


def reduce_only_to_offset(reduce_only: bool) -> Offset:
    return Offset.CLOSE if reduce_only else Offset.OPEN


def account_to_snapshot(account: AccountData, user_id: str, broker_id: str) -> dict:
    balance = float(account.balance or 0)
    available = float(getattr(account, "available", balance - float(account.frozen or 0)))
    frozen = float(account.frozen or 0)
    return {
        "user_id": user_id,
        "broker_id": broker_id,
        "trading_day": "",
        "available": available,
        "margin": frozen,
        "equity": balance,
    }


def position_to_side(position: PositionData) -> str:
    return "LONG" if position.direction == Direction.LONG else "SHORT"

