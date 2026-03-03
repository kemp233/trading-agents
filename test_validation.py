"""Quick validation script for semantic validators"""
from decimal import Decimal
import sys
sys.path.insert(0, 'E:\\Trading_Agent_System\\trading-agents')

from validators.semantic_validators import SemanticValidators, ValidationResult
from core.instrument_master import InstrumentSpec


class MockIntent:
    def __init__(self, side, quantity, price=None, reference_price=None, 
                 settlement_price=None, prev_close=None):
        self.side = side
        self.quantity = Decimal(str(quantity))
        self.price = Decimal(str(price)) if price else None
        self.reference_price = Decimal(str(reference_price)) if reference_price else None
        self.settlement_price = Decimal(str(settlement_price)) if settlement_price else None
        self.prev_close = Decimal(str(prev_close)) if prev_close else None


def test_lot_size_alignment():
    print("Testing lot_size alignment...")
    spec = InstrumentSpec(
        symbol='rb2510',
        asset_class='cn_futures',
        tick_size=Decimal('1'),
        lot_size=Decimal('1'),
        contract_multiplier=Decimal('10'),
        max_leverage=10,
        margin_rate=Decimal('0.10'),
        price_limit_pct=Decimal('0.07'),
        trading_hours={'day': '09:00-15:00'},
    )
    
    validator = SemanticValidators(None, {'max_notional_per_trade': 100000})
    
    # Valid case
    intent = MockIntent('BUY', 5, 100)
    result = validator.validate_trade_intent(intent, spec)
    assert result.valid, f"Expected valid, got errors: {result.errors}"
    print("  ✓ Valid lot_size (5) passed")
    
    # Invalid case
    intent = MockIntent('BUY', 5.5, 3000)
    result = validator.validate_trade_intent(intent, spec)
    assert not result.valid, "Expected invalid for lot_size 5.5"
    assert "not aligned to lot_size" in result.errors[0]
    assert "suggest: 5" in result.errors[0]
    print("  ✓ Invalid lot_size (5.5) correctly rejected with suggestion")


def test_tick_size_alignment():
    print("\nTesting tick_size alignment...")
    spec = InstrumentSpec(
        symbol='rb2510',
        asset_class='cn_futures',
        tick_size=Decimal('1'),
        lot_size=Decimal('1'),
        contract_multiplier=Decimal('10'),
        max_leverage=10,
        margin_rate=Decimal('0.10'),
        price_limit_pct=Decimal('0.07'),
        trading_hours={'day': '09:00-15:00'},
    )
    
    validator = SemanticValidators(None, {'max_notional_per_trade': 100000})
    
    # Valid case
    intent = MockIntent('BUY', 5, 100)
    result = validator.validate_trade_intent(intent, spec)
    assert result.valid, f"Expected valid, got errors: {result.errors}"
    print("  ✓ Valid tick_size (100) passed")
    
    # Invalid case - BUY (round up)
    intent = MockIntent('BUY', 5, 100.5)
    result = validator.validate_trade_intent(intent, spec)
    assert not result.valid, "Expected invalid for tick_size 100.5"
    assert "not aligned to tick_size" in result.errors[0]
    assert "suggest: 101" in result.errors[0]
    print("  ✓ Invalid tick_size (100.5 BUY) correctly rejected with round-up suggestion")
    
    # Invalid case - SELL (round down)
    intent = MockIntent('SELL', 5, 100.5)
    result = validator.validate_trade_intent(intent, spec)
    assert not result.valid, "Expected invalid for tick_size 100.5"
    assert "not aligned to tick_size" in result.errors[0]
    assert "suggest: 100" in result.errors[0]
    print("  ✓ Invalid tick_size (100.5 SELL) correctly rejected with round-down suggestion")


def test_notional_with_multiplier():
    print("\nTesting notional with contract_multiplier...")
    spec = InstrumentSpec(
        symbol='rb2510',
        asset_class='cn_futures',
        tick_size=Decimal('1'),
        lot_size=Decimal('1'),
        contract_multiplier=Decimal('10'),
        max_leverage=10,
        margin_rate=Decimal('0.10'),
        price_limit_pct=Decimal('0.07'),
        trading_hours={'day': '09:00-15:00'},
    )
    
    validator = SemanticValidators(None, {'max_notional_per_trade': 10000})
    
    # Valid case: 300 * 5 * 10 = 15000 > 10000, should fail
    intent = MockIntent('BUY', 5, 300)
    result = validator.validate_trade_intent(intent, spec)
    assert not result.valid, "Expected invalid for notional > max"
    assert "notional" in result.errors[0].lower()
    assert "exceeds max" in result.errors[0]
    assert "300×5×10" in result.errors[0]
    print("  ✓ Notional exceeding max correctly rejected with breakdown")
    
    # Valid case: 100 * 5 * 10 = 5000 < 10000, should pass
    intent = MockIntent('BUY', 5, 100)
    result = validator.validate_trade_intent(intent, spec)
    assert result.valid, f"Expected valid, got errors: {result.errors}"
    print("  ✓ Notional within limit passed")


def test_price_limit_with_base_price():
    print("\nTesting price limit with base price...")
    spec = InstrumentSpec(
        symbol='rb2510',
        asset_class='cn_futures',
        tick_size=Decimal('1'),
        lot_size=Decimal('1'),
        contract_multiplier=Decimal('10'),
        max_leverage=10,
        margin_rate=Decimal('0.10'),
        price_limit_pct=Decimal('0.07'),
        trading_hours={'day': '09:00-15:00'},
    )
    
    validator = SemanticValidators(None, {'max_notional_per_trade': 1000000})
    
    # Valid case: within limits
    intent = MockIntent('BUY', 5, 3000, reference_price=3000)
    result = validator.validate_trade_intent(intent, spec)
    assert result.valid, f"Expected valid, got errors: {result.errors}"
    print("  ✓ Price within limit passed")
    
    # Invalid case: above upper bound (3000 * 1.07 = 3210)
    intent = MockIntent('BUY', 5, 3220, reference_price=3000)
    result = validator.validate_trade_intent(intent, spec)
    assert not result.valid, "Expected invalid for price above limit"
    assert "outside limit" in result.errors[0]
    print("  ✓ Price above limit correctly rejected")
    
    # Invalid case: below lower bound (3000 * 0.93 = 2790)
    intent = MockIntent('SELL', 5, 2780, reference_price=3000)
    result = validator.validate_trade_intent(intent, spec)
    assert not result.valid, "Expected invalid for price below limit"
    assert "outside limit" in result.errors[0]
    print("  ✓ Price below limit correctly rejected")


def test_price_limit_without_base_price():
    print("\nTesting price limit without base price (warning)...")
    spec = InstrumentSpec(
        symbol='rb2510',
        asset_class='cn_futures',
        tick_size=Decimal('1'),
        lot_size=Decimal('1'),
        contract_multiplier=Decimal('10'),
        max_leverage=10,
        margin_rate=Decimal('0.10'),
        price_limit_pct=Decimal('0.07'),
        trading_hours={'day': '09:00-15:00'},
    )
    
    validator = SemanticValidators(None, {'max_notional_per_trade': 1000000})
    
    # No base price - should warn but not error
    intent = MockIntent('BUY', 5, 3000)
    result = validator.validate_trade_intent(intent, spec)
    assert result.valid, f"Expected valid (warning only), got errors: {result.errors}"
    assert len(result.warnings) > 0, "Expected warning for missing base price"
    assert "missing base price" in result.warnings[0]
    print("  ✓ Missing base price generates warning (not error)")


def test_crypto_no_price_limit():
    print("\nTesting crypto (no price limit)...")
    spec = InstrumentSpec(
        symbol='BTCUSDT',
        asset_class='crypto_perp',
        tick_size=Decimal('0.1'),
        lot_size=Decimal('0.001'),
        contract_multiplier=Decimal('1'),
        max_leverage=20,
        margin_rate=Decimal('0.05'),
        price_limit_pct=None,
        trading_hours={'24/7': True},
    )
    
    validator = SemanticValidators(None, {'max_notional_per_trade': 1000000})
    
    # Crypto should not check price limits even with base price
    intent = MockIntent('BUY', 1, 50000, reference_price=50000)
    result = validator.validate_trade_intent(intent, spec)
    assert result.valid, f"Expected valid, got errors: {result.errors}"
    print("  ✓ Crypto asset class skips price limit check")


if __name__ == '__main__':
    test_lot_size_alignment()
    test_tick_size_alignment()
    test_notional_with_multiplier()
    test_price_limit_with_base_price()
    test_price_limit_without_base_price()
    test_crypto_no_price_limit()
    print("\n✅ All validation tests passed!")
