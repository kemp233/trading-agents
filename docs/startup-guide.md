# Futures Startup Guide

## Environment
- Windows 10/11
- Python 3.11+
- Install dependencies in the project virtual environment, including `vnpy_ctp`

## Required configuration
Edit `futures/config/risk_params_futures.yaml` or override with environment variables:
- `broker_id`
- `user_id`
- `ctp_td_front_addr`
- `ctp_md_front_addr`
- `ctp_counter_env` (` µ≈Ã` or `≤‚ ‘`)
- `app_id`
- `auth_code`
- `password` should stay empty in the file and come from `CTP_PASSWORD`

Recommended SimNow 7x24 defaults:
- `broker_id=9999`
- `ctp_td_front_addr=tcp://182.254.243.31:40001`
- `ctp_md_front_addr=tcp://182.254.243.31:40011`
- `ctp_counter_env= µ≈Ã`

## Environment variables
PowerShell:
```powershell
$env:CTP_USER_ID = "your-ctp-user-id"
$env:CTP_PASSWORD = "your trading password"
$env:CTP_BROKER_ID = "9999"
$env:CTP_TD_FRONT = "tcp://182.254.243.31:40001"
$env:CTP_MD_FRONT = "tcp://182.254.243.31:40011"
$env:CTP_COUNTER_ENV = " µ≈Ã"
```

## Startup
Run:
```powershell
venv\Scripts\python.exe -m futures.run_futures
```

The startup flow is:
1. Initialize `data/trading.db`
2. Start `StateWriter`
3. Connect CTP TD gateway
4. Query account and positions
5. Subscribe configured market-data symbols
6. Start `OutboxDispatcher`
7. Launch Streamlit dashboard

## Diagnostics
- Direct login probe: `venv\Scripts\python.exe test_ctp_raw.py`
- TCP + login diagnostics: `venv\Scripts\python.exe test_ctp_debug.py`

## Failure categories
- Front unreachable: network, firewall, wrong front address
- Handshake failure: wrong counter environment for the target front
- Login failed: account, password, or environment qualification issue
- Account query timeout: login succeeded but trading queries did not return
- Market-data subscription issue: symbol-exchange mapping or MD front problem

