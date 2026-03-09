from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

from vnpy_ctp.api import TdApi


class ProbeTd(TdApi):
    def __init__(
        self,
        *,
        front: str,
        broker_id: str,
        user_id: str,
        password: str,
        app_id: str,
        auth_code: str,
        log_path: Path,
    ) -> None:
        super().__init__()
        self.front = front
        self.broker_id = broker_id
        self.user_id = user_id
        self.password = password
        self.app_id = app_id
        self.auth_code = auth_code
        self.log_path = log_path
        self.done = False
        self.login_error_id = None
        self.login_error_msg = None

    def write(self, line: str) -> None:
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        text = f'[{timestamp}] {line}'
        print(text)
        with self.log_path.open('a', encoding='utf-8') as fh:
            fh.write(text + '\n')

    def onFrontConnected(self) -> None:
        self.write('EVENT FrontConnected')
        self.reqAuthenticate(
            {
                'BrokerID': self.broker_id,
                'UserID': self.user_id,
                'AppID': self.app_id,
                'AuthCode': self.auth_code,
            },
            1,
        )

    def onFrontDisconnected(self, reason: int) -> None:
        self.write(f'EVENT FrontDisconnected reason={reason}')
        self.done = True

    def onRspAuthenticate(self, data, error, reqid, last) -> None:
        self.write(
            f'EVENT RspAuthenticate reqid={reqid} last={last} '
            f'error={error} data={data}'
        )
        self.reqUserLogin(
            {
                'BrokerID': self.broker_id,
                'UserID': self.user_id,
                'Password': self.password,
            },
            2,
        )

    def onRspUserLogin(self, data, error, reqid, last) -> None:
        self.login_error_id = (error or {}).get('ErrorID')
        self.login_error_msg = (error or {}).get('ErrorMsg')
        self.write(
            f'EVENT RspUserLogin reqid={reqid} last={last} '
            f'error={error} data={data}'
        )
        self.done = True

    def onRspError(self, error, reqid, last) -> None:
        self.write(f'EVENT RspError reqid={reqid} last={last} error={error}')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='CTP real login probe')
    parser.add_argument('--front', required=True)
    parser.add_argument('--broker-id', default='9999')
    parser.add_argument('--user-id', default=os.getenv('CTP_USER_ID', ''))
    parser.add_argument('--password', default=os.getenv('CTP_PASSWORD', ''))
    parser.add_argument('--app-id', default='simnow_client_test')
    parser.add_argument('--auth-code', default='0000000000000000')
    parser.add_argument('--production-mode', choices=['true', 'false'], default='true')
    parser.add_argument('--timeout', type=int, default=25)
    parser.add_argument('--label', default='')
    parser.add_argument('--output-dir', default='logs/ctp_probe')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    production_mode = args.production_mode.lower() == 'true'

    if not args.user_id:
        raise SystemExit('Set CTP_USER_ID or pass --user-id.')
    if not args.password:
        raise SystemExit('Set CTP_PASSWORD or pass --password.')

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    label = args.label or (
        args.front.replace('tcp://', '').replace(':', '_')
        + ('_prod' if production_mode else '_test')
    )
    log_path = output_dir / f'{label}.log'
    flow_dir = output_dir / f'{label}_flow'
    flow_dir.mkdir(parents=True, exist_ok=True)

    probe = ProbeTd(
        front=args.front,
        broker_id=args.broker_id,
        user_id=args.user_id,
        password=args.password,
        app_id=args.app_id,
        auth_code=args.auth_code,
        log_path=log_path,
    )
    probe.write(
        'START '
        f'front={args.front} broker={args.broker_id} user={args.user_id} '
        f'app_id={args.app_id} production_mode={production_mode}'
    )
    probe.createFtdcTraderApi(str(flow_dir).encode('gbk', errors='ignore'), production_mode)
    probe.subscribePrivateTopic(0)
    probe.subscribePublicTopic(0)
    probe.registerFront(args.front)
    probe.init()

    started = time.time()
    while time.time() - started < args.timeout and not probe.done:
        time.sleep(0.2)

    if not probe.done:
        probe.write(f'RESULT timeout after {args.timeout}s')
        return 2

    if probe.login_error_id == 0:
        probe.write('RESULT login_success')
        return 0

    probe.write(
        f'RESULT login_failed error_id={probe.login_error_id} '
        f'error_msg={probe.login_error_msg}'
    )
    return 1


if __name__ == '__main__':
    raise SystemExit(main())

