"""Connectivity + login diagnostics for the current SimNow setup."""

from __future__ import annotations

import os
import socket
import subprocess
import sys

TD_FRONT = os.getenv("CTP_TD_FRONT", "tcp://182.254.243.31:40001")
MD_FRONT = os.getenv("CTP_MD_FRONT", "tcp://182.254.243.31:40011")
PASSWORD_SET = bool(os.getenv("CTP_PASSWORD"))
COUNTER_ENV = os.getenv("CTP_COUNTER_ENV", "实盘")


def split_front(front: str) -> tuple[str, int]:
    _, host_port = front.split("://", 1)
    host, port = host_port.split(":", 1)
    return host, int(port)


def test_tcp(front: str) -> bool:
    host, port = split_front(front)
    try:
        with socket.create_connection((host, port), timeout=5.0):
            print(f"[TCP] {front} OK")
            return True
    except Exception as exc:  # pragma: no cover
        print(f"[TCP] {front} FAIL: {exc}")
        return False


if __name__ == "__main__":
    print(f"[CONFIG] TD={TD_FRONT}")
    print(f"[CONFIG] MD={MD_FRONT}")
    print(f"[CONFIG] COUNTER_ENV={COUNTER_ENV}")
    print(f"[CONFIG] PASSWORD={'set' if PASSWORD_SET else 'empty'}")

    td_ok = test_tcp(TD_FRONT)
    md_ok = test_tcp(MD_FRONT)
    if not td_ok:
        raise SystemExit("TD front unreachable")
    if not PASSWORD_SET:
        raise SystemExit("Set CTP_PASSWORD before running login diagnostics.")

    cmd = [sys.executable, "test_ctp_raw.py"]
    print(f"[RUN] {' '.join(cmd)}")
    raise SystemExit(subprocess.call(cmd))
