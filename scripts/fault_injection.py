"""Phase 1 故障注入工具

用法:
    python scripts/fault_injection.py --test duplicate_order
    python scripts/fault_injection.py --test crash_recovery
    python scripts/fault_injection.py --test rate_limit_429
"""
import argparse
import logging

logger = logging.getLogger(__name__)


def test_duplicate_order():
    """重复投递同一个 TradeIntent, 验证 idempotency_key 去重"""
    logger.info("Testing: duplicate order submission...")
    # TODO: implement


def test_crash_recovery():
    """写入 outbox 后 kill 进程, 重启验证自动补发"""
    logger.info("Testing: crash recovery...")
    # TODO: implement


def test_rate_limit_429():
    """模拟 429 限频, 验证退避 + DEGRADED"""
    logger.info("Testing: 429 rate limit backoff...")
    # TODO: implement


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fault Injection Tests')
    parser.add_argument('--test', required=True,
                       choices=['duplicate_order', 'crash_recovery', 'rate_limit_429'])
    args = parser.parse_args()

    tests = {
        'duplicate_order': test_duplicate_order,
        'crash_recovery': test_crash_recovery,
        'rate_limit_429': test_rate_limit_429,
    }
    tests[args.test]()
