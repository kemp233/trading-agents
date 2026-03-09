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
st.set_page_config(page_title="Positions and Capital", layout="wide")

main_col, chat_col = st.columns([4.8, 2.0], gap="large")
with main_col:
    st.title("Positions and Capital")
    account = db.get_latest_account_info()
    col1, col2, col3 = st.columns(3)
    col1.metric("Available", f"{float(account.get('available', 0) or 0):,.2f}")
    col2.metric("Margin", f"{float(account.get('margin', 0) or 0):,.2f}")
    col3.metric("Equity", f"{float(account.get('equity', 0) or 0):,.2f}")

    st.divider()
    st.header("Current Positions")
    positions = db.get_positions()
    if positions:
        st.dataframe(pd.DataFrame(positions), use_container_width=True)
    else:
        st.info("No positions right now.")

    st.divider()
    st.header("Recent Orders")
    orders = db.get_orders(limit=100)
    if orders:
        st.dataframe(pd.DataFrame(orders), use_container_width=True)
    else:
        st.info("No recent orders.")

with chat_col:
    render_chat_widget(db_path=DB_PATH, repo_root=str(_REPO_ROOT), key_prefix='page-positions', expanded=False)

time.sleep(3)
st.rerun()
