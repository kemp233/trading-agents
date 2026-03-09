from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from dashboard.backend.command_router import handle as _cmd_handle  # noqa: E402
from dashboard.streamlit_mvp.chat_widget import render_chat_widget  # noqa: E402
from dashboard.streamlit_mvp.db_reader import DbReader  # noqa: E402

_DEFAULT_DB = str(_REPO_ROOT / "data" / "trading.db")
DB_PATH = os.environ.get("AIAGENTTS_DB", _DEFAULT_DB)

db = DbReader(DB_PATH)
st.set_page_config(page_title="Risk Control", layout="wide")

main_col, chat_col = st.columns([4.8, 2.0], gap="large")
with main_col:
    st.title("Risk Control")
    latest = db.get_latest_risk_record()
    col1, col2 = st.columns(2)
    col1.metric("Current State", latest.get("current_state") or "NORMAL")
    col2.metric("Changed At", latest.get("state_changed_at") or "-")
    if latest.get("reason"):
        st.caption(latest["reason"])

    st.divider()
    st.header("Risk State History")
    history = db.get_risk_state_history(limit=30)
    if history:
        st.dataframe(pd.DataFrame(history), use_container_width=True)
    else:
        st.info("No risk history yet.")

    st.divider()
    st.header("Emergency Actions")
    action_col1, action_col2 = st.columns(2)
    with action_col1:
        if st.button("Enter Circuit Mode", use_container_width=True, type="primary"):
            result = _cmd_handle("CIRCUIT")
            st.write(result)
    with action_col2:
        if st.button("Cancel All Orders", use_container_width=True):
            result = _cmd_handle("CANCEL_ALL")
            st.write(result)

with chat_col:
    render_chat_widget(db_path=DB_PATH, repo_root=str(_REPO_ROOT), key_prefix='page-risk', expanded=False)
