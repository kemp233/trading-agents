"""
CTP 连接诊断脚本
运行： python test_ctp_debug.py
作用：打印所有 vnpy 事件和日志，帮助判断登录失败原因
"""
import asyncio
import os
import socket
import sys

# 配置
USER_ID    = "256354"
BROKER_ID  = "9999"
PASSWORD   = os.getenv("CTP_PASSWORD", "")
AUTH_CODE  = "0000000000000000"
APP_ID     = "client_aiagentts_1.0.0"
FRONT_ADDR = "tcp://180.168.146.187:10130"
MD_ADDR    = "tcp://180.168.146.187:10111"


def test_tcp(host: str, port: int, timeout: float = 5.0) -> bool:
    """测试 TCP 端口是否可达"""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception as e:
        print(f"  ✖ TCP 无法连接 {host}:{port} —— {e}")
        return False


async def main():
    # ── Step 1: 检查密码 ───────────────────────
    print("="*55)
    print(f"  user_id   : {USER_ID}")
    print(f"  broker_id : {BROKER_ID}")
    print(f"  password  : {'[set]' if PASSWORD else '[EMPTY - 未设置 CTP_PASSWORD]'}")
    print(f"  auth_code : {AUTH_CODE}")
    print(f"  app_id    : {APP_ID}")
    print(f"  front TD  : {FRONT_ADDR}")
    print(f"  front MD  : {MD_ADDR}")
    print("="*55)

    if not PASSWORD:
        print("\n[ERROR] 密码未设置！请先执行:")
        print("  $env:CTP_PASSWORD = \"Abc6610195@\"")
        sys.exit(1)

    # ── Step 2: TCP 端口测试 ────────────────────
    print("\n[Step 1] 测试网络连通性...")
    host = "180.168.146.187"
    td_ok = test_tcp(host, 10130)
    md_ok = test_tcp(host, 10111)

    if td_ok:
        print(f"  ✔ TD 前置 {host}:10130 可达")
    if md_ok:
        print(f"  ✔ MD 前置 {host}:10111 可达")

    if not td_ok:
        print("\n[ERROR] TD 前置无法连接，可能原因：")
        print("  1. 网络防火墙拦截了 TCP 10130 端口")
        print("  2. 你的公司/家庭网络不允许访问 SimNow")
        print("  3. 尝试备用地址: tcp://180.168.146.187:10131")
        sys.exit(1)

    # ── Step 3: CTP 登录测试 ───────────────────
    print("\n[Step 2] 尝试 CTP 登录（打印所有事件）...")

    from vnpy.event import EventEngine, Event
    from vnpy_ctp.gateway import CtpGateway

    login_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    received: list[str] = []

    def on_any_event(event: Event):
        data = event.data
        msg = getattr(data, "msg", str(data))
        line = f"  [EVENT:{event.type}] {msg}"
        print(line)
        received.append(msg)
        # 监测登录成功 / 失败关键字
        keywords_ok  = ["登录成功", "交易服务器"]
        keywords_err = ["登录失败", "密码错误", "ErrorID", "AuthCode", "认证失败"]
        if any(k in msg for k in keywords_ok) and not login_event.is_set():
            print("  → 检测到登录成功关键字!")
            loop.call_soon_threadsafe(login_event.set)
        if any(k in msg for k in keywords_err) and not login_event.is_set():
            print("  → 检测到登录失败关键字!")
            loop.call_soon_threadsafe(login_event.set)

    def on_account(event: Event):
        print(f"  [ACCOUNT 事件] {event.data}  → 登录成功!")
        if not login_event.is_set():
            loop.call_soon_threadsafe(login_event.set)

    ee = EventEngine()
    ee.register("eLog",      on_any_event)
    ee.register("eAccount.", on_account)
    ee.start()

    gw = CtpGateway(ee, "CTP")
    gw.connect({
        "用户名":   USER_ID,
        "密码":     PASSWORD,
        "经纪商代码": BROKER_ID,
        "交易服务器": FRONT_ADDR,
        "行情服务器": MD_ADDR,
        "产品名称": APP_ID,
        "授权编码": AUTH_CODE,
        "产品信息": "",
    })

    print("  等待登录回调（最多 30 秒）...")
    try:
        await asyncio.wait_for(login_event.wait(), timeout=30.0)
        print("\n[结果] 登录成功 ✔")
    except asyncio.TimeoutError:
        print("\n[结果] 登录超时 ✖")
        print("收到的全部事件如下：")
        for m in received:
            print(f"  {m}")
        if not received:
            print("  [无任何事件] —— 可能是网络无法连接前置机")
    finally:
        try:
            gw.close()
            ee.stop()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
