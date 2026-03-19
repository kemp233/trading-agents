FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 安装系统依赖
# 1. build-essential, wget, ca-certificates (原有)
# 2. locales, locales-all (解决 CTP SDK 初始化崩溃)
# 3. dmidecode, lshw (解决看穿式监管硬件指纹采集)
# 4. tzdata (确保容器时间正确)
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    ca-certificates \
    locales \
    locales-all \
    dmidecode \
    lshw \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# 强制设置语言环境（极其重要，解决 std::runtime_error）
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8
ENV LANGUAGE=en_US.UTF-8
# 设置时区为上海
ENV TZ=Asia/Shanghai

# 复制依赖文件
COPY requirements.txt .

# 安装 Python 依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目文件
COPY . .

# 暴露 Streamlit 端口（如果有使用到）
EXPOSE 8501

# 启动期货交易系统
CMD ["python", "-m", "futures.run_futures"]
