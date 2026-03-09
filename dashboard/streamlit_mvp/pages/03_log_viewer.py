from __future__ import annotations

import os
import sys
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
st.set_page_config(page_title="Log Viewer", layout="wide")

main_col, chat_col = st.columns([4.8, 2.0], gap="large")
with main_col:
    st.title("Log Viewer")
    limit = st.select_slider("Rows", options=[50, 100, 200, 500], value=100)
    tab_orders, tab_system, tab_monitor, tab_error = st.tabs(["Orders", "System", "Monitor", "Errors"])
    with tab_orders:
        rows = db.get_orders(limit=limit)
        st.dataframe(pd.DataFrame(rows), use_container_width=True) if rows else st.info("No order logs.")
    with tab_system:
        rows = db.get_system_log(limit=limit)
        st.dataframe(pd.DataFrame(rows), use_container_width=True) if rows else st.info("No system logs.")
    with tab_monitor:
        rows = db.get_monitor_log(limit=limit)
        st.dataframe(pd.DataFrame(rows), use_container_width=True) if rows else st.info("No monitor logs.")
    with tab_error:
        rows = db.get_error_log(limit=limit)
        st.dataframe(pd.DataFrame(rows), use_container_width=True) if rows else st.info("No error logs.")

with chat_col:
    render_chat_widget(db_path=DB_PATH, repo_root=str(_REPO_ROOT), key_prefix='page-logs', expanded=False)
