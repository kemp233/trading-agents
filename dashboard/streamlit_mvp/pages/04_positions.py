from __future__ import annotations
"""持仓与资金面板（指标 #2）"""
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

st.set_page_config(page_title="持仓与资金", layout="wide")
st.title("📂 持仓与资金")

# ===========================================================================
# Section 1 —— 账户资金摘要（指标 #2 截图用）
# ===========================================================================
st.header("💰 账户资金摘要")

account = db.get_latest_account_info()
available = account.get("available", 0)
margin = account.get("margin", 0)
equity = account.get("equity", 0)

if available == 0 and margin == 0 and equity == 0:
    st.info("暂无资金数据，等待 CTP 账户回报")
else:
    col1, col2, col3 = st.columns(3)
    col1.metric("可用资金（元）", f"{available:,.2f}")
    col2.metric("冻结保证金（元）", f"{margin:,.2f}")
    col3.metric("权益（元）", f"{equity:,.2f}")

st.divider()

# ===========================================================================
# Section 2 —— 当前持仓（指标 #2 截图用）
# ===========================================================================
st.header("📊 当前持仓")

positions = db.get_positions()
if positions:
    df_pos = pd.DataFrame(positions)
    col_rename = {
        "symbol": "品种",
        "venue": "交易所",
        "side": "方向",
        "quantity": "手数",
        "entry_price": "开仓均价",
        "unrealized_pnl": "浮动盈亏",
        "updated_at": "更新时间",
    }
    df_pos = df_pos.rename(columns={k: v for k, v in col_rename.items() if k in df_pos.columns})
    st.dataframe(df_pos, use_container_width=True)
else:
    st.info("当前无持仓")

st.divider()

# ===========================================================================
# Section 3 —— 当日委托记录（指标 #2 截图用）
# ===========================================================================
st.header("📄 当日委托记录")

orders = db.get_orders(limit=100)
if orders:
    df_orders = pd.DataFrame(orders)
    col_rename_o = {
        "created_at": "时间",
        "order_id": "委托号",
        "symbol": "品种",
        "side": "方向",
        "quantity": "数量",
        "price": "价格",
        "status": "状态",
        "filled_quantity": "成交量",
        "filled_price": "成交价",
    }
    df_orders = df_orders.rename(columns={k: v for k, v in col_rename_o.items() if k in df_orders.columns})

    # 状态过滤器
    if "状态" in df_orders.columns:
        statuses = ["全部"] + df_orders["状态"].dropna().unique().tolist()
        sel = st.selectbox("状态过滤", statuses, key="order_status_filter")
        if sel != "全部":
            df_orders = df_orders[df_orders["状态"] == sel]

    st.dataframe(df_orders, use_container_width=True)
    st.caption(f"共 {len(df_orders)} 条委托记录")
else:
    st.info("今日暂无委托记录")

# ===========================================================================
# 自动刷新（3 秒）
# ===========================================================================
time.sleep(3)
st.rerun()
