from __future__ import annotations

import html
import sys
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from dashboard.backend.chat_types import ChatMessage  # noqa: E402
from dashboard.backend.chatops_runtime import ChatOpsRuntime  # noqa: E402
from dashboard.streamlit_mvp.db_reader import DbReader  # noqa: E402

GROUP_CHANNEL = "group-chat"
CHAT_OPEN_KEY = "telegram_group_chat_open"
REPLY_TARGET_KEY = "telegram_group_chat_reply_target"
GROUP_MEMBERS = [
    ("You", "operator", "#111827"),
    ("Orchestration", "orchestration", "#2563eb"),
    ("Strategy", "strategy", "#7c3aed"),
    ("Risk", "risk", "#dc2626"),
    ("News", "news", "#0f766e"),
    ("Portfolio", "portfolio", "#c2410c"),
    ("Reconciler", "reconciliation", "#475569"),
]
ROLE_LABELS = {
    "orchestration": "Orchestration",
    "strategy": "Strategy",
    "risk": "Risk",
    "news": "News",
    "portfolio": "Portfolio",
    "reconciliation": "Reconciler",
    "execution": "Execution",
    "system": "System",
    "monitor": "Monitor",
    "audit": "Audit",
}
SEVERITY_STYLES = {
    "success": {"fg": "#14532d", "accent": "#16a34a", "bg": "#ecfdf5"},
    "warning": {"fg": "#92400e", "accent": "#f59e0b", "bg": "#fffbeb"},
    "critical": {"fg": "#991b1b", "accent": "#ef4444", "bg": "#fef2f2"},
    "info": {"fg": "#1f2937", "accent": "#60a5fa", "bg": "#f8fafc"},
}


def inject_chat_widget_css() -> None:
    st.markdown(
        """
        <style>
          .tg-collapsed {
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 24px;
            background: linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
            box-shadow: 0 20px 50px rgba(15,23,42,0.08);
            padding: 0.95rem;
          }
          .tg-collapsed-title {
            font-size: 0.94rem;
            font-weight: 700;
            color: #0f172a;
          }
          .tg-collapsed-subtitle {
            margin-top: 0.2rem;
            font-size: 0.78rem;
            color: #64748b;
          }
          .tg-preview {
            margin-top: 0.75rem;
            border-radius: 16px;
            background: #f8fafc;
            border: 1px solid rgba(15, 23, 42, 0.06);
            padding: 0.72rem 0.8rem;
          }
          .tg-preview-item {
            padding: 0.32rem 0;
            color: #334155;
            font-size: 0.8rem;
            line-height: 1.35;
          }
          .tg-shell {
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 26px;
            background: linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
            box-shadow: 0 24px 70px rgba(15,23,42,0.1);
            overflow: hidden;
          }
          .tg-header {
            padding: 0.95rem 1rem 0.75rem 1rem;
            border-bottom: 1px solid rgba(15, 23, 42, 0.07);
            background: linear-gradient(135deg, rgba(37,99,235,0.1) 0%, rgba(14,165,233,0.06) 100%);
          }
          .tg-header-title {
            font-size: 1rem;
            font-weight: 700;
            color: #0f172a;
          }
          .tg-header-subtitle {
            margin-top: 0.18rem;
            color: #475569;
            font-size: 0.78rem;
            line-height: 1.35;
          }
          .tg-members {
            display: flex;
            flex-wrap: wrap;
            gap: 0.35rem;
            margin-top: 0.7rem;
          }
          .tg-member {
            display: inline-flex;
            align-items: center;
            gap: 0.32rem;
            padding: 0.24rem 0.55rem;
            border-radius: 999px;
            background: rgba(255,255,255,0.75);
            border: 1px solid rgba(15, 23, 42, 0.06);
            font-size: 0.74rem;
            color: #1e293b;
          }
          .tg-member-dot {
            width: 0.52rem;
            height: 0.52rem;
            border-radius: 999px;
            display: inline-block;
          }
          .tg-feed {
            padding: 0.95rem 0.85rem 0.4rem 0.85rem;
            background: linear-gradient(180deg, rgba(248,250,252,0.76) 0%, rgba(255,255,255,0.92) 100%);
          }
          .tg-day-sep {
            display: flex;
            justify-content: center;
            margin: 0.2rem 0 0.8rem 0;
          }
          .tg-day-sep span {
            display: inline-block;
            padding: 0.18rem 0.58rem;
            border-radius: 999px;
            background: rgba(226,232,240,0.9);
            color: #475569;
            font-size: 0.7rem;
            font-weight: 700;
          }
          .tg-row {
            display: flex;
            margin-bottom: 0.78rem;
          }
          .tg-row-user {
            justify-content: flex-end;
          }
          .tg-row-assistant {
            justify-content: flex-start;
          }
          .tg-row-system {
            justify-content: center;
          }
          .tg-bubble {
            max-width: 94%;
            border-radius: 22px;
            padding: 0.75rem 0.85rem 0.6rem 0.85rem;
            box-shadow: 0 8px 20px rgba(15,23,42,0.05);
            border: 1px solid rgba(15, 23, 42, 0.06);
          }
          .tg-bubble-user {
            background: linear-gradient(180deg, #e0f2fe 0%, #dbeafe 100%);
            border-color: rgba(37,99,235,0.18);
          }
          .tg-bubble-assistant {
            background: #ffffff;
          }
          .tg-bubble-system {
            background: #f8fafc;
            border-style: dashed;
          }
          .tg-card-approval {
            border-left: 4px solid #f59e0b;
            background: linear-gradient(180deg, #fffaf0 0%, #fffbeb 100%);
          }
          .tg-card-workflow {
            border-left: 4px solid #3b82f6;
          }
          .tg-card-task {
            border-left: 4px solid #64748b;
          }
          .tg-name-row {
            display: flex;
            align-items: center;
            gap: 0.45rem;
            margin-bottom: 0.24rem;
          }
          .tg-avatar {
            width: 1.35rem;
            height: 1.35rem;
            border-radius: 999px;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            font-size: 0.62rem;
            font-weight: 800;
            color: white;
          }
          .tg-name {
            font-size: 0.77rem;
            font-weight: 700;
            color: #334155;
          }
          .tg-reply {
            margin-bottom: 0.46rem;
            padding: 0.36rem 0.5rem;
            border-radius: 14px;
            background: rgba(226,232,240,0.65);
            border-left: 3px solid rgba(37,99,235,0.55);
          }
          .tg-reply-name {
            font-size: 0.68rem;
            font-weight: 700;
            color: #1d4ed8;
          }
          .tg-reply-text {
            margin-top: 0.08rem;
            color: #475569;
            font-size: 0.75rem;
            line-height: 1.35;
          }
          .tg-content {
            color: #0f172a;
            font-size: 0.9rem;
            line-height: 1.48;
            white-space: pre-wrap;
            word-break: break-word;
          }
          .tg-meta {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 0.55rem;
            margin-top: 0.42rem;
          }
          .tg-time {
            color: #64748b;
            font-size: 0.7rem;
          }
          .tg-badges {
            display: flex;
            flex-wrap: wrap;
            gap: 0.28rem;
          }
          .tg-badge {
            display: inline-block;
            border-radius: 999px;
            padding: 0.14rem 0.42rem;
            font-size: 0.66rem;
            font-weight: 700;
            letter-spacing: 0.01em;
            background: #e2e8f0;
            color: #334155;
          }
          .tg-composer {
            border-top: 1px solid rgba(15, 23, 42, 0.07);
            padding: 0.65rem 0.85rem 0.85rem 0.85rem;
            background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
          }
          .tg-composer-hint {
            color: #64748b;
            font-size: 0.76rem;
            margin-bottom: 0.45rem;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_chat_widget(*, db_path: str, repo_root: str, key_prefix: str, expanded: bool = False) -> None:
    inject_chat_widget_css()
    if CHAT_OPEN_KEY not in st.session_state:
        st.session_state[CHAT_OPEN_KEY] = expanded
    if REPLY_TARGET_KEY not in st.session_state:
        st.session_state[REPLY_TARGET_KEY] = "No reply"

    db = DbReader(db_path)
    runtime = ChatOpsRuntime(db_path, repo_root=repo_root)
    messages = db.get_live_chat_messages(channel=GROUP_CHANNEL, limit=60)
    pending_approvals = db.get_pending_approvals(limit=4)

    if not st.session_state[CHAT_OPEN_KEY]:
        _render_collapsed(messages)
        action_cols = st.columns([1.6, 1.0])
        with action_cols[1]:
            if st.button("Open", key=f"{key_prefix}-chat-open", use_container_width=True):
                st.session_state[CHAT_OPEN_KEY] = True
                st.rerun()
        return

    st.markdown('<div class="tg-shell">', unsafe_allow_html=True)
    _render_header(key_prefix=key_prefix)
    _render_feed(messages)
    _render_action_strip(pending_approvals=pending_approvals, runtime=runtime, key_prefix=key_prefix)
    _render_composer(messages, runtime=runtime, key_prefix=key_prefix)
    st.markdown('</div>', unsafe_allow_html=True)


def _render_collapsed(messages: list[ChatMessage]) -> None:
    st.markdown('<div class="tg-collapsed">', unsafe_allow_html=True)
    st.markdown('<div class="tg-collapsed-title">Agent Group Chat</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="tg-collapsed-subtitle">Right-side Telegram-style group chat. Open when you need to talk, hide when you need the full dashboard.</div>',
        unsafe_allow_html=True,
    )
    preview = messages[-2:] if messages else []
    if preview:
        items = ''.join(
            f'<div class="tg-preview-item"><strong>{html.escape(_display_name(message))}</strong>: {html.escape(_single_line(message.content))}</div>'
            for message in preview
        )
        st.markdown(f'<div class="tg-preview">{items}</div>', unsafe_allow_html=True)
    else:
        st.markdown(
            '<div class="tg-preview"><div class="tg-preview-item">No messages yet. Start with natural language, then use <code>@agent -command...</code> if the agent confirms an action.</div></div>',
            unsafe_allow_html=True,
        )
    st.markdown('</div>', unsafe_allow_html=True)


def _render_header(*, key_prefix: str) -> None:
    member_markup = ''.join(
        f'<span class="tg-member"><span class="tg-member-dot" style="background:{color};"></span>{html.escape(name)}</span>'
        for name, _, color in GROUP_MEMBERS
    )
    st.markdown(
        f"""
        <div class="tg-header">
          <div class="tg-header-title">Trading Agent Group</div>
          <div class="tg-header-subtitle">One room for you and the agent team. Mention an agent to discuss, then send an explicit command only after confirmation.</div>
          <div class="tg-members">{member_markup}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    hide_cols = st.columns([1.55, 1.0])
    with hide_cols[1]:
        if st.button("Hide", key=f"{key_prefix}-chat-hide", use_container_width=True):
            st.session_state[CHAT_OPEN_KEY] = False
            st.rerun()


def _render_feed(messages: list[ChatMessage]) -> None:
    with st.container(height=560, border=False):
        st.markdown('<div class="tg-feed"><div id="tg-chat-feed-end"></div>', unsafe_allow_html=True)
        if not messages:
            st.info("No group messages yet.")
        else:
            current_day = None
            for message in messages:
                message_day = _day_key(message.ts)
                if message_day != current_day:
                    current_day = message_day
                    st.markdown(_render_day_separator_html(current_day), unsafe_allow_html=True)
                st.markdown(_render_message_html(message), unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    _scroll_feed_to_latest()


def _render_action_strip(*, pending_approvals: list[dict[str, object]], runtime: ChatOpsRuntime, key_prefix: str) -> None:
    if pending_approvals:
        st.caption("Pending approvals")
        for approval in pending_approvals:
            _render_approval_actions(approval, runtime=runtime, key_prefix=key_prefix)


def _render_approval_actions(approval: dict[str, object], *, runtime: ChatOpsRuntime, key_prefix: str) -> None:
    approval_id = str(approval.get('approval_id') or '')
    requested_action = str(approval.get('requested_action') or 'action')
    instrument = str(approval.get('instrument') or 'N/A')
    risk_level = str(approval.get('risk_level') or 'unknown')
    with st.container(border=True):
        st.markdown(f"**{requested_action}**  `{instrument}`  risk=`{risk_level}`")
        st.caption(f"approval_id={approval_id} | expires_at={approval.get('expires_at') or '-'}")
        cols = st.columns(3)
        with cols[0]:
            if st.button("Approve", key=f"{key_prefix}-approve-{approval_id}", use_container_width=True):
                runtime.resolve_approval(approval_id, 'approve')
                st.rerun()
        with cols[1]:
            if st.button("Reject", key=f"{key_prefix}-reject-{approval_id}", use_container_width=True):
                runtime.resolve_approval(approval_id, 'reject')
                st.rerun()
        with cols[2]:
            if st.button("Review", key=f"{key_prefix}-review-{approval_id}", use_container_width=True):
                runtime.resolve_approval(approval_id, 'review')
                st.rerun()

def _render_composer(messages: list[ChatMessage], *, runtime: ChatOpsRuntime, key_prefix: str) -> None:
    st.markdown('<div class="tg-composer">', unsafe_allow_html=True)
    st.markdown(
        '<div class="tg-composer-hint">Natural language first. Use <code>@agent -command...</code> only when the agent asks for explicit execution.</div>',
        unsafe_allow_html=True,
    )

    reply_choices = _reply_choices(messages)
    selected_reply = st.selectbox(
        "Reply target",
        options=list(reply_choices.keys()),
        index=list(reply_choices.keys()).index(st.session_state.get(REPLY_TARGET_KEY, "No reply")) if st.session_state.get(REPLY_TARGET_KEY, "No reply") in reply_choices else 0,
        key=f"{key_prefix}-reply-target",
        label_visibility="collapsed",
    )
    st.session_state[REPLY_TARGET_KEY] = selected_reply
    reply_to = reply_choices[selected_reply]
    if reply_to:
        st.caption(f"Replying to {reply_to['author']}: {reply_to['preview']}")

    with st.form(f"{key_prefix}-group-chat-form", clear_on_submit=True):
        text = st.text_area(
            "Message",
            label_visibility="collapsed",
            height=92,
            placeholder="@news please check whether there are important overnight policy updates\nor @reconciler -commandlog latest",
            key=f"{key_prefix}-group-chat-input",
        )
        cols = st.columns([1.0, 1.0, 1.8])
        with cols[0]:
            submitted = st.form_submit_button("Send", use_container_width=True)
        with cols[1]:
            clear_reply = st.form_submit_button("Clear Reply", use_container_width=True)
    if clear_reply:
        st.session_state[REPLY_TARGET_KEY] = "No reply"
        st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    if submitted and text.strip():
        with st.spinner("Agent is typing..."):
            runtime.process_text(
                text.strip(),
                sender_id="operator",
                channel_hint=GROUP_CHANNEL,
                reply_to=reply_to,
            )
        st.session_state[REPLY_TARGET_KEY] = "No reply"
        st.rerun()


def _render_message_html(message: ChatMessage) -> str:
    role = _message_role(message)
    severity = SEVERITY_STYLES.get(message.severity, SEVERITY_STYLES["info"])
    badges = ''.join(f'<span class="tg-badge">{html.escape(tag)}</span>' for tag in _message_badges(message))
    bubble_class = {
        "user": "tg-bubble tg-bubble-user",
        "assistant": "tg-bubble tg-bubble-assistant",
        "system": "tg-bubble tg-bubble-system",
    }[role]
    message_type = str((message.meta or {}).get('_message_type') or 'plain')
    if message_type == 'approval_card':
        bubble_class += ' tg-card-approval'
    elif message_type == 'summary_card' and ((message.meta or {}).get('task_id') or (message.meta or {}).get('workflow_run_id')):
        bubble_class += ' tg-card-workflow'
    elif message_type == 'plain' and role == 'system':
        bubble_class += ' tg-card-task'

    bubble_bg = "#dbeafe" if role == "user" else severity["bg"] if role == "system" else "#ffffff"
    reply_markup = _render_reply_quote(message)
    return (
        f'<div class="tg-row tg-row-{role}">'
        f'<div class="{bubble_class}" style="border-color:{severity["accent"]}33; background:{bubble_bg};">'
        f'<div class="tg-name-row"><span class="tg-avatar" style="background:{_avatar_color(message)};">{html.escape(_avatar_text(message))}</span>'
        f'<div class="tg-name" style="color:{severity["fg"]};">{html.escape(_display_name(message))}</div></div>'
        f'{reply_markup}'
        f'<div class="tg-content">{html.escape(message.content)}</div>'
        f'<div class="tg-meta"><div class="tg-badges">{badges}</div>'
        f'<div class="tg-time">{html.escape(_format_ts(message.ts))}</div></div>'
        '</div></div>'
    )


def _render_reply_quote(message: ChatMessage) -> str:
    meta = message.meta or {}
    reply_meta = meta.get('reply_to') or meta.get('in_reply_to') or {}
    if not isinstance(reply_meta, dict) or not reply_meta:
        return ''
    author = html.escape(str(reply_meta.get('author') or 'Reply'))
    preview = html.escape(str(reply_meta.get('preview') or ''))
    return (
        '<div class="tg-reply">'
        f'<div class="tg-reply-name">{author}</div>'
        f'<div class="tg-reply-text">{preview}</div>'
        '</div>'
    )


def _render_day_separator_html(day_key: str) -> str:
    return f'<div class="tg-day-sep"><span>{html.escape(day_key)}</span></div>'


def _message_role(message: ChatMessage) -> str:
    if message.author_kind == "user":
        return "user"
    if message.author_kind == "assistant":
        return "assistant"
    return "system"


def _display_name(message: ChatMessage) -> str:
    if message.author_kind == "user":
        return "You"
    role = (message.workflow_role or "").strip().lower()
    if role in ROLE_LABELS:
        return ROLE_LABELS[role]
    if message.agent_name:
        return str(message.agent_name)
    return "System"


def _message_badges(message: ChatMessage) -> list[str]:
    badges: list[str] = []
    meta = message.meta or {}
    message_type = str(meta.get('_message_type') or 'plain')
    if message_type == 'approval_card' or meta.get("approval_id") or meta.get("approval"):
        badges.append("Approval")
    if meta.get("task_id") or meta.get("workflow_run_id"):
        badges.append("Workflow")
    if message.author_kind == "system":
        badges.append("System")
    if message.severity and message.severity != "info":
        badges.append(message.severity.title())
    return badges[:3]


def _reply_choices(messages: list[ChatMessage]) -> dict[str, dict[str, str] | None]:
    options: dict[str, dict[str, str] | None] = {"No reply": None}
    recent = [message for message in messages if message.author_kind in {"user", "assistant"}][-8:]
    for message in reversed(recent):
        label = f"{_display_name(message)} | {_single_line(message.content)[:42]}"
        options[label] = {
            "message_id": message.id,
            "author": _display_name(message),
            "preview": _single_line(message.content)[:120],
        }
    return options


def _avatar_text(message: ChatMessage) -> str:
    name = _display_name(message)
    letters = ''.join(ch for ch in name if ch.isalnum())[:2]
    return letters.upper() or 'AG'


def _avatar_color(message: ChatMessage) -> str:
    role = (message.workflow_role or '').lower()
    for _, member_role, color in GROUP_MEMBERS:
        if member_role == role:
            return color
    if message.author_kind == 'user':
        return '#111827'
    return '#64748b'


def _single_line(value: str) -> str:
    return " ".join((value or "").split())[:120]


def _format_ts(value: str) -> str:
    if not value:
        return "Now"
    ts = value.replace("T", " ")
    return ts[:19]


def _day_key(value: str) -> str:
    if not value:
        return 'Today'
    return value.replace('T', ' ')[:10]


def _scroll_feed_to_latest() -> None:
    components.html(
        """
        <script>
        const candidates = [...window.parent.document.querySelectorAll('div[data-testid="stVerticalBlock"]')];
        const scrollables = candidates.filter((node) => {
          const style = window.parent.getComputedStyle(node);
          return style.overflowY === 'auto' && node.scrollHeight > node.clientHeight + 20;
        });
        const target = scrollables.sort((a, b) => a.getBoundingClientRect().left - b.getBoundingClientRect().left).at(-1);
        if (target) {
          target.scrollTop = target.scrollHeight;
        }
        </script>
        """,
        height=0,
    )
