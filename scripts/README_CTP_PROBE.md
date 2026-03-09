# CTP VPN 排查探针

这套脚本用于你手动关闭 VPN 后，逐个测试 SimNow CTP 前置。

## 运行前

1. 先完全断开 VPN。
2. 打开 PowerShell。
3. 进入仓库根目录：

```powershell
cd E:\Trading_Agent_System\trading-agents
```

## 一次跑完所有测试

先设置环境变量，避免把密码写进命令行历史：

```powershell
$env:CTP_PASSWORD = '<YOUR_CTP_PASSWORD>'
powershell -ExecutionPolicy Bypass -File .\scripts\run_ctp_probe_matrix.ps1
```

如果你不设置 `CTP_PASSWORD`，脚本会在运行时交互提示输入密码。

## 它会依次测试

- 第二套 7x24：`182.254.243.31:40001`，`production_mode=true`
- 第一套：`180.168.146.187:10201`，`production_mode=true`
- 第一套：`180.168.146.187:10202`，`production_mode=true`
- 第一套：`180.168.146.187:10201`，`production_mode=false`
- 第一套：`180.168.146.187:10202`，`production_mode=false`

## 日志位置

所有日志会落在：

```text
E:\Trading_Agent_System\trading-agents\logs\ctp_probe
```

重点看每个日志里的这些行：

- `EVENT FrontConnected`
- `EVENT RspAuthenticate`
- `EVENT RspUserLogin`
- `RESULT ...`

## 如果你只想单独试一个前置

```powershell
$env:CTP_PASSWORD = '<YOUR_CTP_PASSWORD>'
.\venv\Scripts\python.exe .\scripts\ctp_probe_runner.py `
  --front tcp://182.254.243.31:40001 `
  --production-mode true `
  --label manual_env2 `
  --output-dir .\logs\ctp_probe
```

## 你跑完后回传给我

把这些内容发我就够了：

1. `run_ctp_probe_matrix.ps1` 的整段终端输出
2. `logs\ctp_probe` 目录下所有 `.log` 文件
