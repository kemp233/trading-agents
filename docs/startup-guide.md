# 系统启动与操作指南

## 1. 环境准备
- **操作系统**: Windows 10/11 (由于 CTP DLL 依赖，暂不支持 Linux/Mac 原生运行)。
- **Python 版本**: 3.11 及以上。
- **依赖库**: 运行 `pip install -r requirements.txt` 及 `pip install vnpy_ctp`。

## 2. 配置文件
修改 `futures/config/risk_params_futures.yaml`，填入以下信息：
- `user_id`: 你的 SimNow 或实盘账号。
- `password`: 交易密码（建议通过环境变量 `CTP_PASSWORD` 设置）。
- `broker_id`: 9999 (SimNow 默认)。
- `ctp_front_addr`: 前置地址。

## 3. 启动流程
### 3.1 核心后端启动
在项目根目录下执行：
```powershell
python -m futures.run_futures
```
系统将按顺序执行：DB 初始化 -> 启动 StateWriter -> 连接 CTP -> 启动 Outbox 调度器 -> 自动唤起 Streamlit 界面。

### 3.2 仪表盘操作
Streamlit 默认在 `http://localhost:8501` 运行。
- **00_login_connection**: 查看连接状态及硬件信息采集结果。
- **01_monitor**: 监控报单、撤单实时流量。
- **02_risk_control**: 风险阈值调整及应急一键全撤（Emergency 按钮）。
- **03_logs**: 查看分级别运行日志。
- **04_positions**: 实时仓位核对。

## 4. 停止流程
在终端窗口按 `Ctrl + C`。系统将自动执行：
1. 停止 Outbox 调度。
2. 断开 CTP 连接（TdApi/MdApi 登出）。
3. 停止 StateWriter 写入。
4. 安全关闭 SQLite WAL 句柄。
