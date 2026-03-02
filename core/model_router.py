"""Model Router R0-R6 — 交易时段零 LLM 调用"""
import logging

logger = logging.getLogger(__name__)


class ModelRouter:
    """模型路由器
    
    R0: 数据校验 — 无 LLM, Pydantic 硬编码
    R1: 技术信号 — 无 LLM, TA-Lib / pandas
    R2: 新闻分类 — 离线 DeepSeek-V3, 结果缓存
    R3: 语义校验 — 无 LLM, 含期货乘数/tick/lot
    R4: 策略审批 — 离线 DeepSeek-R1
    R5: 复盘分析 — 离线 DeepSeek-R1
    R6: 故障降级 — 按故障类型分三级
    """

    def __init__(self, config: dict):
        self._config = config
        self._llm_enabled = False  # 在线模式下始终 False

    def route(self, route_id: str, **kwargs):
        router_map = {
            'R0': self._r0_data_validation,
            'R1': self._r1_technical_signal,
            'R2': self._r2_news_classification,
            'R3': self._r3_semantic_validation,
            'R4': self._r4_strategy_approval,
            'R5': self._r5_post_review,
            'R6': self._r6_fault_handling,
        }
        handler = router_map.get(route_id)
        if not handler:
            raise ValueError(f"Unknown route: {route_id}")
        return handler(**kwargs)

    def _r0_data_validation(self, **kwargs):
        # Pydantic schema validation — no LLM
        pass

    def _r1_technical_signal(self, **kwargs):
        # Pure math — no LLM
        pass

    def _r2_news_classification(self, **kwargs):
        # Offline only — check cache first
        if self._llm_enabled:
            raise RuntimeError("R2 cannot run during trading hours")
        pass

    def _r3_semantic_validation(self, **kwargs):
        # No LLM — includes futures multiplier/tick/lot validation
        pass

    def _r4_strategy_approval(self, **kwargs):
        # Offline only
        if self._llm_enabled:
            raise RuntimeError("R4 is offline-only")
        pass

    def _r5_post_review(self, **kwargs):
        # Offline only — post-market
        pass

    def _r6_fault_handling(self, fault_type: str, **kwargs):
        """R6a: non-critical, R6b: critical, R6c: fatal"""
        pass
