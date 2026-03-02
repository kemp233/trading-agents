"""CTP Adapter — 翻译 VenueOrderSpec → CTP 报单结构体 (Phase 2)"""
import logging
from venue.base_adapter import VenueAdapter
from venue.venue_order_spec import VenueOrderSpec, VenueReceipt, VenuePosition

logger = logging.getLogger(__name__)


class CTPAdapter(VenueAdapter):
    """Phase 2 实现 — 通过 vn.py 接入 CTP
    
    CTP 特有语义翻译:
    - hedge_flag → CTP 投机套保标志
    - side + reduce_only → CTP 开平标志
    - time_in_force → CTP 有效期类型
    """

    async def submit_order(self, spec: VenueOrderSpec) -> VenueReceipt:
        raise NotImplementedError("Phase 2: implement with vn.py")

    async def cancel_order(self, client_order_id: str) -> VenueReceipt:
        raise NotImplementedError("Phase 2: implement")

    async def query_order(self, client_order_id: str) -> VenueReceipt:
        raise NotImplementedError("Phase 2: implement")

    async def query_positions(self) -> list[VenuePosition]:
        raise NotImplementedError("Phase 2: implement")

    async def get_market_status(self, symbol: str):
        raise NotImplementedError("Phase 2: implement")
