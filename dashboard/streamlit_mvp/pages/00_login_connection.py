from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pandas as pd
import streamlit as st

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from dashboard.streamlit_mvp.chat_widget import render_chat_widget  # noqa: E402
from dashboard.streamlit_mvp.db_reader import DbReader  # noqa: E402

_DEFAULT_DB = str(_REPO_ROOT / "data" / "trading.db")
DB_PATH = os.environ.get("AIAGENTTS_DB", _DEFAULT_DB)

db = DbReader(DB_PATH)
st.set_page_config(page_title="CTP Login Connection", layout="wide")

main_col, chat_col = st.columns([4.8, 2.0], gap="large")
with main_col:
    st.title("CTP Login and Connection")
    st.header("Connection Status")
    conn_status = db.get_latest_connection_status()
    raw_status = conn_status.get("status", "DISCONNECTED")
    col1, col2, col3 = st.columns(3)
    col1.metric("Status", raw_status)
    col2.metric("Trade Front", conn_status.get("front_addr") or "-")
    col3.metric("Updated At", conn_status.get("ts") or "-")
    if conn_status.get("detail"):
        st.caption(conn_status["detail"])

    st.divider()
    st.header("Account Snapshot")
    account = db.get_latest_account_info()
    row1 = st.columns(3)
    row1[0].metric("User", account.get("user_id") or "-")
    row1[1].metric("Broker", account.get("broker_id") or "-")
    row1[2].metric("Trading Day", account.get("trading_day") or "-")
    row2 = st.columns(3)
    row2[0].metric("Available", f"{float(account.get('available', 0) or 0):,.2f}")
    row2[1].metric("Margin", f"{float(account.get('margin', 0) or 0):,.2f}")
    row2[2].metric("Equity", f"{float(account.get('equity', 0) or 0):,.2f}")

    st.divider()
    st.header("Connection Log")
    conn_rows = db.get_connection_log(limit=20)
    if conn_rows:
        st.dataframe(pd.DataFrame(conn_rows), use_container_width=True)
    else:
        st.info("No connection records yet.")

with chat_col:
    render_chat_widget(db_path=DB_PATH, repo_root=str(_REPO_ROOT), key_prefix='page-login', expanded=False)

time.sleep(3)
st.rerun()
