"""
CTP 底层 TdApi 直接测试（绕过 vnpy EventEngine）
运行： python test_ctp_raw.py
"""
import os
import sys
import time
import socket

USER_ID    = "256354"
BROKER_ID  = "9999"
PASSWORD   = os.getenv("CTP_PASSWORD", "")
AUTH_CODE  = "0000000000000000"
APP_ID     = "client_aiagentts_1.0.0"
FRONT_TD   = "tcp://180.168.146.187:10130"
FLOW_PATH  = "./ctp_flow_td/"   # CTP 必须要展尾断高线

if not PASSWORD:
    print("[ERROR] 请先设置: $env:CTP_PASSWORD = \"Abc6610195@\"")
    sys.exit(1)

# ── 必须先创建流文件目录，CTP 不会自动创建 ──
os.makedirs(FLOW_PATH, exist_ok=True)
print(f"[OK] 流文件目录: {os.path.abspath(FLOW_PATH)}")

from vnpy_ctp.api import TdApi


class TestTdApi(TdApi):
    """CTP 底层 API 测试类，直接重写回调"""
    connected = False
    authenticated = False
    logged_in = False

    def onFrontConnected(self) -> None:
        print("[CALLBACK] OnFrontConnected ✔ 前置机已连接")
        self.connected = True
        # 发送穿透式认证
        req = {
            "BrokerID":  BROKER_ID,
            "UserID":    USER_ID,
            "AuthCode":  AUTH_CODE,
            "AppID":     APP_ID,
        }
        self.reqAuthenticate(req, 1)
        print("[ACTION] reqAuthenticate 已发送")

    def onFrontDisconnected(self, reason: int) -> None:
        print(f"[CALLBACK] OnFrontDisconnected 断线 reason={reason}")

    def onRspAuthenticate(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        if error and error.get("ErrorID", 0) != 0:
            print(f"[CALLBACK] 穿透式认证失败: ErrorID={error['ErrorID']} Msg={error['ErrorMsg']}")
            return
        print("[CALLBACK] OnRspAuthenticate 认证成功 ✔")
        self.authenticated = True
        # 登录
        req = {
            "BrokerID": BROKER_ID,
            "UserID":   USER_ID,
            "Password": PASSWORD,
        }
        self.reqUserLogin(req, 2)
        print("[ACTION] reqUserLogin 已发送")

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        if error and error.get("ErrorID", 0) != 0:
            print(f"[CALLBACK] 登录失败: ErrorID={error['ErrorID']} Msg={error['ErrorMsg']}")
            return
        print("[CALLBACK] OnRspUserLogin 登录成功 ✔✔✔")
        print(f"  TradingDay : {data.get('TradingDay')}")
        print(f"  SessionID  : {data.get('SessionID')}")
        self.logged_in = True

    def onRspError(self, error: dict, reqid: int, last: bool) -> None:
        print(f"[CALLBACK] OnRspError: {error}")


print(f"\n测试配置: user={USER_ID} broker={BROKER_ID} front={FRONT_TD}")
print("-" * 55)

api = TestTdApi()
api.createFtdcTraderApi(FLOW_PATH)
print(f"[OK] createFtdcTraderApi 完成")

api.registerFront(FRONT_TD)
api.subscribePrivateTopic(0)
api.subscribePublicTopic(0)
api.init()
print("[OK] init() 已调用，等待 OnFrontConnected...(10秒)")

# 等待10秒，每秒打点
for i in range(10):
    time.sleep(1)
    status = []
    if api.connected:    status.append("Connected")
    if api.authenticated: status.append("Authenticated")
    if api.logged_in:    status.append("LoggedIn")
    print(f"  [{i+1:2d}s] {', '.join(status) or 'waiting...'}")
    if api.logged_in:
        break

print("-" * 55)
if api.logged_in:
    print("✔ CTP 登录完全成功!")
elif api.connected:
    print("⚠ 已连接但未完成登录 (认证或密码问题)")
else:
    print("✖ 未收到 OnFrontConnected —— 可能是 CTP DLL 或网络问题")
    print("建议: 尝试备用前置 tcp://180.168.146.187:10131")

try:
    api.release()
except Exception:
    pass
