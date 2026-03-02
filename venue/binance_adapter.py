"""Binance Adapter — 翻译 VenueOrderSpec → Binance API"""
import logging
from decimal import Decimal
from venue.base_adapter import VenueAdapter
from venue.venue_order_spec import VenueOrderSpec, VenueReceipt, VenuePosition

logger = logging.getLogger(__name__)


class BinanceAdapter(VenueAdapter):
    def __init__(self, api_key: str, api_secret: str, testnet: bool = True):
        self._api_key = api_key
        self._api_secret = api_secret
        self._testnet = testnet
        self._client = None  # ccxt.binance instance

    async def submit_order(self, spec: VenueOrderSpec) -> VenueReceipt:
        params = {
            'symbol': spec.symbol,
            'side': spec.side,
            'type': spec.order_type,
            'quantity': float(spec.quantity),
        }
        if spec.client_order_id:
            params['newClientOrderId'] = spec.client_order_id
        if spec.reduce_only:
            params['reduceOnly'] = True
        if spec.price:
            params['price'] = float(spec.price)
            params['timeInForce'] = spec.time_in_force

        # TODO: Phase 1 实现 — ccxt 调用
        logger.info(f"BinanceAdapter.submit_order: {params}")
        raise NotImplementedError("Phase 1: implement with ccxt")

    async def cancel_order(self, client_order_id: str) -> VenueReceipt:
        raise NotImplementedError("Phase 1: implement")

    async def query_order(self, client_order_id: str) -> VenueReceipt:
        raise NotImplementedError("Phase 1: implement")

    async def query_positions(self) -> list[VenuePosition]:
        raise NotImplementedError("Phase 1: implement")

    async def get_market_status(self, symbol: str):
        raise NotImplementedError("Phase 1: implement")
