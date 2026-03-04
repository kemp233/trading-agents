from __future__ import annotations
"""交易统计监控页面（指标 #4、5、6）"""
import logging
import os
import sys
import time
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components
import yaml

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from dashboard.streamlit_mvp.db_reader import DbReader  # noqa: E402

logger = logging.getLogger(__name__)

_DEFAULT_DB = str(_REPO_ROOT / "data" / "trading.db")
DB_PATH = os.environ.get("AIAGENTTS_DB", _DEFAULT_DB)

db = DbReader(DB_PATH)

# ---------------------------------------------------------------------------
# 页面标题
# ---------------------------------------------------------------------------
st.title("📊 交易统计监控")

# ---------------------------------------------------------------------------
# 四个计数卡片
# ---------------------------------------------------------------------------
counts = db.get_today_monitor_counts()

col1, col2, col3, col4 = st.columns(4)
col1.metric("📝 报单笔数", counts["order_count"])
col2.metric("❌ 撤单笔数", counts["cancel_count"])
col3.metric("✅ 成交笔数", counts["fill_count"])
col4.metric("⚠️ 重复报单次数", counts["duplicate_count"])

st.caption(f"最后更新时间：{time.strftime('%Y-%m-%d %H:%M:%S')}")

st.divider()

# ---------------------------------------------------------------------------
# 阈値占用进度条（指标 #4、#5、#6）
# ---------------------------------------------------------------------------
st.subheader("📊 阈値占用进度")

_RISK_PARAMS_PATH = _REPO_ROOT / "config" / "risk_params.yaml"
try:
    with _RISK_PARAMS_PATH.open("r", encoding="utf-8") as f:
        risk_data = yaml.safe_load(f)
except Exception as e:
    logger.error("Failed to load risk_params.yaml: %s", e)
    risk_data = {}

fm = risk_data.get("futures_monitor", {})
max_orders = int(fm.get("max_orders_per_day", 500))
max_cancels = int(fm.get("max_cancels_per_day", 200))
max_duplicates = int(fm.get("max_duplicate_orders", 5))
warning_pct = float(fm.get("warning_pct", 0.8))

def _pct(val: int, limit: int) -> float:
    return min(1.0, val / limit) if limit > 0 else 0.0

order_pct = _pct(counts["order_count"], max_orders)
cancel_pct = _pct(counts["cancel_count"], max_cancels)
dup_pct = _pct(counts["duplicate_count"], max_duplicates)

st.progress(order_pct, text=f"报单笔数：{counts['order_count']} / {max_orders}")
st.progress(cancel_pct, text=f"撤单笔数：{counts['cancel_count']} / {max_cancels}")
st.progress(dup_pct, text=f"重复报单：{counts['duplicate_count']} / {max_duplicates}")

# 预警提示
over_warning = []
if order_pct >= warning_pct:
    over_warning.append("报单笔数")
if cancel_pct >= warning_pct:
    over_warning.append("撤单笔数")
if dup_pct >= warning_pct:
    over_warning.append("重复报单")

if over_warning:
    st.warning(f"⚠️ 以下指标已达预警线（{int(warning_pct * 100)}%）：{'、'.join(over_warning)}")

st.divider()

# ---------------------------------------------------------------------------
# 阈値预警展示
# ---------------------------------------------------------------------------
alerts = db.get_today_monitor_alerts()
warnings = [a for a in alerts if a.get("level") == "WARNING"]
breaches = [a for a in alerts if a.get("level") == "BREACH"]

if breaches:
    latest_breach = breaches[0]
    field = latest_breach.get("field", "unknown")
    st.error(
        f"⚠️ 阈値超限 (BREACH): 字段={field}, 当前値={latest_breach.get('current_value')}, "
        f"限制={latest_breach.get('limit_value')}"
    )
    components.html(
        f'<script>window.alert("\\u26a0\\ufe0f \\u9608\\u5024\\u8d85\\u9650\\uff1a{field}");</script>',
        height=0,
    )
elif warnings:
    latest_warning = warnings[0]
    field = latest_warning.get("field", "unknown")
    st.warning(
        f"🟡 阈値预警 (WARNING): 字段={field}, 当前値={latest_warning.get('current_value')}, "
        f"限制={latest_warning.get('limit_value')}"
    )
else:
    st.success("✅ 今日暂无阈値预警")

# ---------------------------------------------------------------------------
# 自动刷新（每 5 秒）
# ---------------------------------------------------------------------------
time.sleep(5)
st.rerun()
