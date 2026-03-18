FROM python:3.11-slim

WORKDIR /app

# 安装系统依赖（ta-lib 需要编译、CTP 库依赖）
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 安装 Python 依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目文件
COPY . .

# 暴露 Streamlit 端口
EXPOSE 8501

# 启动期货交易系统（根据您的启动脚本）
CMD ["python", "-m", "futures.run_futures"]
