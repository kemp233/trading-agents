"""VenueAdapter ABC — 交易所适配器基类"""
from abc import ABC, abstractmethod
from venue.venue_order_spec import VenueOrderSpec, VenueReceipt, VenuePosition


class VenueAdapter(ABC):
    """各交易所适配器的统一接口
    
    Order Manager 只与此接口交互,
    不直接调用任何交易所 SDK。
    """

    @abstractmethod
    async def submit_order(self, spec: VenueOrderSpec) -> VenueReceipt:
        ...

    @abstractmethod
    async def cancel_order(self, client_order_id: str) -> VenueReceipt:
        ...

    @abstractmethod
    async def query_order(self, client_order_id: str) -> VenueReceipt:
        ...

    @abstractmethod
    async def query_positions(self) -> list[VenuePosition]:
        ...

    @abstractmethod
    async def get_market_status(self, symbol: str) -> 'MarketStatus':
        ...
