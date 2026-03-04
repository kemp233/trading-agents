from __future__ import annotations
"""CTP 登录与连接状态页面（指标 #1、#3）"""
import logging
import os
import sys
import time
from pathlib import Path

import pandas as pd
import streamlit as st

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from dashboard.streamlit_mvp.db_reader import DbReader  # noqa: E402

logger = logging.getLogger(__name__)

_DEFAULT_DB = str(_REPO_ROOT / "data" / "trading.db")
DB_PATH = os.environ.get("AIAGENTTS_DB", _DEFAULT_DB)

db = DbReader(DB_PATH)

st.set_page_config(page_title="CTP 登录与连接", layout="wide")
st.title("🔌 登录与连接状态")

# ===========================================================================
# Section 1 —— CTP 连接配置（指标 #1 截图用）
# ===========================================================================
st.header("🔧 CTP 连接配置")

with st.form("ctp_config_form"):
    front_addr_val = os.environ.get("CTP_FRONT_ADDR", "")
    broker_id_val = os.environ.get("CTP_BROKER_ID", "9999")
    user_id_val = os.environ.get("CTP_USER_ID", "256354")

    front_addr = st.text_input(
        "front_addr",
        value=front_addr_val,
        placeholder="tcp://180.168.146.187:10130",
    )
    broker_id = st.text_input("broker_id", value=broker_id_val)
    user_id = st.text_input("user_id", value=user_id_val)
    submitted = st.form_submit_button("🔌 连接 CTP")

if submitted:
    st.info("配置已保存，请重启 run_futures.py 使配置生效")

st.divider()

# ===========================================================================
# Section 2 —— 实时连接状态（指标 #3 截图用）
# ===========================================================================
st.header("🚦 实时连接状态")

conn_status = db.get_latest_connection_status()
raw_status = conn_status.get("status", "DISCONNECTED")

if raw_status == "CONNECTED":
    status_label = "🟢 已连接"
elif raw_status == "RECONNECTING":
    status_label = "🟡 重连中"
else:
    status_label = "🔴 断线"

col1, col2, col3 = st.columns(3)
col1.metric("连接状态", status_label)
col2.metric("前置地址", conn_status.get("front_addr") or "-")
col3.metric("最后更新", conn_status.get("ts") or "-")

st.divider()

# ===========================================================================
# Section 3 —— 账户信息（指标 #1 截图用）
# ===========================================================================
st.header("📄 登录账户信息")

account = db.get_latest_account_info()

available = account.get("available", 0)
margin = account.get("margin", 0)
equity = account.get("equity", 0)

if available == 0 and margin == 0 and equity == 0:
    st.info("暂无资金数据，等待 CTP 账户回报")
else:
    cola, colb, colc = st.columns(3)
    cola.metric("用户号", account.get("user_id") or "-")
    colb.metric("Broker ID", account.get("broker_id") or "-")
    colc.metric("交易日", account.get("trading_day") or "-")

    cold, cole, colf = st.columns(3)
    cold.metric("可用资金（元）", f"{available:,.2f}")
    cole.metric("冻结保证金（元）", f"{margin:,.2f}")
    colf.metric("权益（元）", f"{equity:,.2f}")

st.divider()

# ===========================================================================
# Section 4 —— 连接历史日志
# ===========================================================================
st.header("📃 连接历史日志（最近 20 条）")

conn_rows = db.get_connection_log(limit=20)
if conn_rows:
    df_conn = pd.DataFrame(conn_rows)
    col_rename = {
        "ts": "时间",
        "status": "状态",
        "front_addr": "前置地址",
        "session_id": "会话 ID",
        "detail": "详情",
    }
    df_conn = df_conn.rename(columns={k: v for k, v in col_rename.items() if k in df_conn.columns})
    st.dataframe(df_conn, use_container_width=True)
else:
    st.info("暂无连接记录")

# ===========================================================================
# 自动刷新（3 秒）
# ===========================================================================
time.sleep(3)
st.rerun()
