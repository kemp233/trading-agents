"""Command Router — 解析 @Agent /command 指令"""
import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

# 支持的指令映射
COMMAND_MAP = {
    'all': {
        '/status': 'cmd_all_status',
        '/report': 'cmd_all_report',
    },
    'Risk_Governor': {
        '/state': 'cmd_risk_state',
        '/recover': 'cmd_risk_recover',
        '/circuit': 'cmd_risk_circuit',
    },
    'Strategy': {
        '/pause': 'cmd_strategy_pause',
        '/resume': 'cmd_strategy_resume',
        '/list': 'cmd_strategy_list',
    },
    'Order_Manager': {
        '/cancel_all': 'cmd_order_cancel_all',
        '/flatten': 'cmd_order_flatten',
    },
    'Market_Data': {
        '/subscribe': 'cmd_market_subscribe',
        '/health': 'cmd_market_health',
    },
    'News': {
        '/latest': 'cmd_news_latest',
        '/windows': 'cmd_news_windows',
    },
    'Reconciler': {
        '/check': 'cmd_reconciler_check',
    },
    'Portfolio': {
        '/pnl': 'cmd_portfolio_pnl',
        '/exposure': 'cmd_portfolio_exposure',
    },
}


def parse_command(text: str) -> Optional[dict]:
    """解析 @Agent /command [args] 格式的指令"""
    pattern = r'@(\w+)\s+(/\w+)\s*(.*)?'
    match = re.match(pattern, text.strip())
    if not match:
        return None
    return {
        'agent': match.group(1),
        'command': match.group(2),
        'args': match.group(3).strip() if match.group(3) else '',
    }


class CommandRouter:
    def __init__(self, agents: dict):
        self._agents = agents

    async def execute(self, text: str) -> dict:
        parsed = parse_command(text)
        if not parsed:
            return {'error': f'无法解析指令: {text}'}

        agent_name = parsed['agent']
        command = parsed['command']
        args = parsed['args']

        if agent_name == 'all':
            results = {}
            for name, agent in self._agents.items():
                try:
                    result = await agent.handle_command(command, args)
                    results[name] = result
                except Exception as e:
                    results[name] = f'Error: {e}'
            return {'agent': 'all', 'command': command, 'results': results}

        agent = self._agents.get(agent_name)
        if not agent:
            return {'error': f'未知 Agent: {agent_name}'}

        try:
            result = await agent.handle_command(command, args)
            return {'agent': agent_name, 'command': command, 'result': result}
        except Exception as e:
            return {'error': f'{agent_name} 执行失败: {e}'}
