from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import streamlit as st

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.model_router import ModelRouter  # noqa: E402
from dashboard.streamlit_mvp.chat_widget import render_chat_widget  # noqa: E402
from dashboard.streamlit_mvp.db_reader import DbReader  # noqa: E402

st.set_page_config(page_title='Trading Agents Console', layout='wide')

DEFAULT_DB = str(_REPO_ROOT / 'data' / 'trading.db')
DB_PATH = os.environ.get('AIAGENTTS_DB', DEFAULT_DB)
STATUS_STYLE = {
    'NORMAL': ('#0f5132', '#d1fae5'),
    'DEGRADED': ('#7c2d12', '#ffedd5'),
    'CIRCUIT': ('#7f1d1d', '#fee2e2'),
    'VENUE_HALT': ('#7f1d1d', '#fee2e2'),
    'RECONCILING': ('#5b21b6', '#ede9fe'),
    'OFFLINE': ('#475569', '#e2e8f0'),
    'CONNECTED': ('#0f5132', '#d1fae5'),
    'DISCONNECTED': ('#7f1d1d', '#fee2e2'),
    'RECONNECTING': ('#7c2d12', '#ffedd5'),
    'CAUTION': ('#9a3412', '#ffedd5'),
    'RESTRICTED': ('#991b1b', '#fee2e2'),
    'PROTECT_ONLY': ('#7c3aed', '#ede9fe'),
    'HALTED': ('#3f3f46', '#e4e4e7'),
}
STATUS_LABELS = {
    'NORMAL': 'Normal',
    'DEGRADED': 'Degraded',
    'CIRCUIT': 'Circuit',
    'VENUE_HALT': 'Venue Halt',
    'RECONCILING': 'Reconciling',
    'OFFLINE': 'Offline',
    'CONNECTED': 'Connected',
    'DISCONNECTED': 'Disconnected',
    'RECONNECTING': 'Reconnecting',
    'CAUTION': 'Caution',
    'RESTRICTED': 'Restricted',
    'PROTECT_ONLY': 'Protect Only',
    'HALTED': 'Halted',
}


def ui_status(value: str | None) -> str:
    raw = str(value or '')
    return STATUS_LABELS.get(raw, raw or 'Unknown')


def metric_card(label: str, value: str, tone: str) -> None:
    fg, bg = STATUS_STYLE.get(tone, ('#124559', '#edf6f9'))
    st.markdown(
        f"""
        <div style="background:{bg}; color:{fg}; border-radius:18px; padding:1rem 1.1rem; min-height:100px; border:1px solid rgba(15,23,42,0.08);">
          <div style="font-size:0.82rem; opacity:0.8;">{label}</div>
          <div style="font-size:1.6rem; font-weight:700; margin-top:0.2rem;">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def send_group_message(text: str, runtime) -> None:
    runtime.process_text(text, sender_id='operator', channel_hint='group-chat')


st.markdown(
    """
    <style>
      .stApp {
        background:
          radial-gradient(circle at top left, rgba(253, 230, 138, 0.25), transparent 30%),
          radial-gradient(circle at top right, rgba(125, 211, 252, 0.18), transparent 28%),
          linear-gradient(180deg, #fffdf7 0%, #f6fbff 52%, #fff8f1 100%);
      }
      .block-container {
        padding-top: 1.2rem;
        padding-bottom: 1.5rem;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

db = DbReader(DB_PATH)
from dashboard.backend.chatops_runtime import ChatOpsRuntime  # noqa: E402
runtime = ChatOpsRuntime(DB_PATH, repo_root=str(_REPO_ROOT))
model_router = ModelRouter(config={})
portfolio = db.get_portfolio_snapshot()
connection = db.get_latest_connection_status()
risk = db.get_latest_risk_record()
monitor_counts = db.get_today_monitor_counts()
provider_health = model_router.list_provider_health()
workflow_assignments = model_router.list_workflow_assignments()
system_mode = db.get_system_mode()
open_tasks = db.get_open_tasks(limit=6)
pending_approvals = db.get_pending_approvals(limit=6)
recent_summaries = db.get_recent_daily_summaries(limit=3)

main_col, chat_col = st.columns([4.8, 2.0], gap='large')

with main_col:
    st.markdown('# Trading Agents Console')
    st.caption('Phase 2: Multi-agent group chat and workflow collaboration for the China futures trading console.')
    st.info('Phase 2 scope: Telegram-style right drawer chat, natural language plus @agent -command flows, in-chat approvals, archive-aware context, and cross-page collaboration for strategy, risk, news, portfolio, and reconciliation.')

    summary_cols = st.columns(4)
    with summary_cols[0]:
        metric_card('System Mode', ui_status(system_mode), system_mode)
    with summary_cols[1]:
        metric_card('Connection', ui_status(connection.get('status') or 'DISCONNECTED'), str(connection.get('status') or 'DISCONNECTED'))
    with summary_cols[2]:
        metric_card('Open Positions', str(portfolio['position_count']), 'CONNECTED')
    with summary_cols[3]:
        metric_card('Equity', f"{float(portfolio['account'].get('equity') or 0):,.2f}", 'CONNECTED')

    action_cols = st.columns([1.2, 1.2, 1.2, 2.4])
    with action_cols[0]:
        if st.button('Flatten All', use_container_width=True, type='primary'):
            send_group_message('@execution flatten current positions', runtime)
            st.rerun()
    with action_cols[1]:
        if st.button('Freeze / Circuit', use_container_width=True):
            send_group_message('@risk enter circuit mode now', runtime)
            st.rerun()
    with action_cols[2]:
        if st.button('Generate Archive', use_container_width=True):
            runtime.run_archive_for_day()
            st.rerun()
    with action_cols[3]:
        st.info(f"Trading day: {portfolio['account'].get('trading_day') or 'Unknown'} | Last connection: {connection.get('ts') or 'Unknown'}")

    left_col, center_col, right_col = st.columns([1.15, 1.45, 1.25], gap='large')
    with left_col:
        st.markdown('### Current Status')
        st.markdown(f"""
- Risk state: **{ui_status(risk.get('current_state', 'NORMAL'))}**
- Available funds: **{float(portfolio['account'].get('available') or 0):,.2f}**
- Margin in use: **{float(portfolio['account'].get('margin') or 0):,.2f}**
- Unrealized PnL: **{portfolio['unrealized_pnl']:,.2f}**
""")
        st.markdown('### Chat Behavior')
        st.markdown("""
- Use natural language for discussion and clarification.
- Use `@agent -command...` when the agent asks for an explicit action.
- The right panel is shared across all pages.
""")
        st.checkbox('Auto refresh every 4 seconds', value=True, key='auto_refresh')

    with center_col:
        st.markdown('### Open Tasks')
        if open_tasks:
            for task in open_tasks:
                st.markdown(f"- `{task['priority']}` `{task['status']}` {task['workflow_type']} -> {task['target_role']}")
        else:
            st.info('No active tasks right now.')
        st.markdown('### Pending Approvals')
        if pending_approvals:
            for approval in pending_approvals:
                st.markdown(f"- `{approval['requested_action']}` / `{approval.get('instrument') or 'N/A'}` / `{approval['status']}`")
        else:
            st.caption('No approvals pending.')
        st.markdown('### Recent Archives')
        if recent_summaries:
            for summary in recent_summaries:
                st.markdown(f"- `{summary['trading_day']}` {summary['headline']}")
        else:
            st.caption('No archive summaries yet.')

    with right_col:
        st.markdown('### Provider Status')
        for provider in provider_health:
            tone = 'CONNECTED' if provider['configured'] else 'DISCONNECTED'
            fg, bg = STATUS_STYLE.get(tone, ('#124559', '#edf6f9'))
            st.markdown(
                f"""
                <div style="background:{bg}; color:{fg}; border-radius:16px; padding:0.8rem 0.9rem; margin-bottom:0.55rem;">
                  <div style="font-weight:700;">{provider['display_name']}</div>
                  <div style="font-size:0.82rem; opacity:0.82;">Model: {provider['model_name']}</div>
                  <div style="font-size:0.82rem; opacity:0.82;">{provider['message']}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        st.markdown('### Workflow Mapping')
        for assignment in workflow_assignments:
            label = assignment['workflow_role']
            providers = ' / '.join(assignment.get('provider_ids') or []) or 'unconfigured'
            st.markdown(f'- `{label}` -> {providers}')
        st.markdown('### Today Counters')
        st.markdown(f"""
- Orders: **{monitor_counts['order_count']}**
- Cancels: **{monitor_counts['cancel_count']}**
- Fills: **{monitor_counts['fill_count']}**
- Duplicates: **{monitor_counts['duplicate_count']}**
""")

with chat_col:
    render_chat_widget(db_path=DB_PATH, repo_root=str(_REPO_ROOT), key_prefix='home', expanded=False)

if st.session_state.auto_refresh:
    time.sleep(4)
    st.rerun()
