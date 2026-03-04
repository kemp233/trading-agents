"""
CRYPTO PLACEHOLDER - Phase 2 Only

加密货币独立启动入口（占位符）

合规声明：
  本文件当前为空占位符。
  加密货币板块需在期货接入测试（东证期货 CTP）通过后，
  在独立合规环境下另行开发（Issue #7、#12）。

禁止导入任何 futures/ 专用模块。
"""

import asyncio
from loguru import logger


async def main() -> None:
    raise NotImplementedError(
        "crypto/run_crypto.py 尚未实现。"
        "请待期货接入测试通过后，在 Phase 2 中开发。"
    )


if __name__ == "__main__":
    logger.warning("[Crypto] Phase 2 占位符，当前不可运行。")
    asyncio.run(main())
