"""Direct TdApi login probe against SimNow 7x24."""

from __future__ import annotations

import os
import time
from pathlib import Path

from vnpy_ctp.api import TdApi

USER_ID = os.getenv("CTP_USER_ID", "")
BROKER_ID = os.getenv("CTP_BROKER_ID", "9999")
PASSWORD = os.getenv("CTP_PASSWORD", "")
AUTH_CODE = os.getenv("CTP_AUTH_CODE", "0000000000000000")
APP_ID = os.getenv("CTP_APP_ID", "simnow_client_test")
FRONT_TD = os.getenv("CTP_TD_FRONT", "tcp://182.254.243.31:40001")
PRODUCTION_MODE = os.getenv("CTP_COUNTER_ENV", "实盘") == "实盘"
FLOW_PATH = "./ctp_flow_raw/"


class ProbeTdApi(TdApi):
    connected = False
    authenticated = False
    logged_in = False
    error = None

    def onFrontConnected(self) -> None:
        print("[CALLBACK] OnFrontConnected")
        self.connected = True
        ret = self.reqAuthenticate(
            {
                "BrokerID": BROKER_ID,
                "UserID": USER_ID,
                "AuthCode": AUTH_CODE,
                "AppID": APP_ID,
            },
            1,
        )
        print(f"[ACTION] reqAuthenticate ret={ret}")

    def onRspAuthenticate(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        print(f"[CALLBACK] OnRspAuthenticate data={data} error={error} reqid={reqid} last={last}")
        if error and error.get("ErrorID", 0) not in (0,):
            message = str(error.get("ErrorMsg", ""))
            if "豁免终端认证" not in message:
                self.error = ("authenticate", error)
                return
        self.authenticated = True
        ret = self.reqUserLogin(
            {
                "BrokerID": BROKER_ID,
                "UserID": USER_ID,
                "Password": PASSWORD,
            },
            2,
        )
        print(f"[ACTION] reqUserLogin ret={ret}")

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        print(f"[CALLBACK] OnRspUserLogin data={data} error={error} reqid={reqid} last={last}")
        if error and error.get("ErrorID", 0) != 0:
            self.error = ("login", error)
            return
        self.logged_in = True

    def onRspError(self, error: dict, reqid: int, last: bool) -> None:
        print(f"[CALLBACK] OnRspError error={error} reqid={reqid} last={last}")
        self.error = ("error", error)

    def onFrontDisconnected(self, reason: int) -> None:
        print(f"[CALLBACK] OnFrontDisconnected reason={reason}")
        self.error = ("disconnect", reason)


if not USER_ID:
    raise SystemExit("Set CTP_USER_ID before running this script.")

if not PASSWORD:
    raise SystemExit("Set CTP_PASSWORD before running this script.")

Path(FLOW_PATH).mkdir(parents=True, exist_ok=True)
print(f"[CONFIG] user={USER_ID} broker={BROKER_ID} front={FRONT_TD} production_mode={PRODUCTION_MODE}")

api = ProbeTdApi()
api.createFtdcTraderApi(FLOW_PATH, PRODUCTION_MODE)
api.registerFront(FRONT_TD)
api.subscribePrivateTopic(0)
api.subscribePublicTopic(0)
api.init()

for i in range(30):
    time.sleep(1)
    print(
        f"[TICK {i + 1:02d}] connected={api.connected} authenticated={api.authenticated} logged_in={api.logged_in} error={api.error}"
    )
    if api.logged_in or api.error:
        break

if api.logged_in:
    print("[RESULT] LOGIN_OK")
else:
    print(f"[RESULT] LOGIN_FAIL {api.error}")

api.release()
