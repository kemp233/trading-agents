"""VenueOrderSpec — 统一订单语义层 (v3 新增)"""
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal, Optional


@dataclass
class VenueOrderSpec:
    """所有 Venue 共享的统一订单规格
    
    Order Manager 只生成 VenueOrderSpec,
    各 VenueAdapter 负责翻译为交易所原生参数。
    """
    symbol: str
    side: Literal['BUY', 'SELL']
    order_type: Literal['MARKET', 'LIMIT', 'STOP']
    quantity: Decimal              # 已按 lot_size 对齐
    price: Optional[Decimal] = None  # 已按 tick_size 对齐
    time_in_force: Literal['GTC', 'IOC', 'FOK'] = 'GTC'
    reduce_only: bool = False
    post_only: bool = False
    hedge_flag: Literal['SPEC', 'HEDGE'] = 'SPEC'
    client_order_id: str = ''


@dataclass
class VenueReceipt:
    """交易所回执"""
    venue_order_id: str
    client_order_id: str
    status: Literal['NEW', 'PARTIALLY_FILLED', 'FILLED', 'CANCELED', 'REJECTED']
    filled_quantity: Decimal = Decimal('0')
    filled_price: Decimal = Decimal('0')
    timestamp: str = ''
    raw_response: dict = None

    def __post_init__(self):
        if self.raw_response is None:
            self.raw_response = {}


@dataclass
class VenuePosition:
    """交易所持仓"""
    symbol: str
    side: Literal['LONG', 'SHORT']
    quantity: Decimal
    entry_price: Decimal
    unrealized_pnl: Decimal = Decimal('0')
