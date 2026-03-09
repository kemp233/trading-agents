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
st.set_page_config(page_title="Monitor", layout="wide")

main_col, chat_col = st.columns([4.8, 2.0], gap="large")
with main_col:
    st.title("Trading Monitor")
    counts = db.get_today_monitor_counts()
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Orders", counts["order_count"])
    col2.metric("Cancels", counts["cancel_count"])
    col3.metric("Fills", counts["fill_count"])
    col4.metric("Duplicates", counts["duplicate_count"])

    st.divider()
    st.header("Monitor Alerts")
    alerts = db.get_today_monitor_alerts()
    if alerts:
        st.dataframe(pd.DataFrame(alerts), use_container_width=True)
    else:
        st.success("No monitor alerts right now.")

with chat_col:
    render_chat_widget(db_path=DB_PATH, repo_root=str(_REPO_ROOT), key_prefix='page-monitor', expanded=False)

time.sleep(5)
st.rerun()
