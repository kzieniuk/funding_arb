import pytest
import pandas as pd
from datetime import datetime, timedelta, timezone
from collector import BinanceFuturesCollector, BinanceSpotCollector, HyperliquidCollector

@pytest.fixture
def date_range():
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(hours=4)
    return start_date, end_date

def test_binance_futures(date_range):
    start_date, end_date = date_range
    bf = BinanceFuturesCollector()
    symbol = 'BTC/USDT'
    
    print(f"\nTesting Binance Futures: {symbol}")
    
    # Test Funding Rates
    df_fund = bf.fetch_funding_rates(symbol, start_date, end_date)
    assert not df_fund.empty, "Binance Futures funding rates should not be empty"
    assert len(df_fund) > 0, "Binance Futures funding rates length should be > 0"
    
    # Test Prices
    df_price = bf.fetch_prices(symbol, start_date, end_date)
    assert not df_price.empty, "Binance Futures prices should not be empty"
    assert len(df_price) > 0, "Binance Futures prices length should be > 0"

def test_binance_spot(date_range):
    start_date, end_date = date_range
    bs = BinanceSpotCollector()
    symbol = 'BTC/USDT'
    
    print(f"\nTesting Binance Spot: {symbol}")
    
    # Test Prices
    df_price = bs.fetch_prices(symbol, start_date, end_date)
    assert not df_price.empty, "Binance Spot prices should not be empty"
    assert len(df_price) > 0, "Binance Spot prices length should be > 0"

def test_hyperliquid(date_range):
    start_date, end_date = date_range
    hl = HyperliquidCollector()
    symbol = 'BTC/USDC:USDC'
    
    print(f"\nTesting Hyperliquid: {symbol}")
    
    # Test Funding Rates
    df_fund = hl.fetch_funding_rates(symbol, start_date, end_date)
    assert not df_fund.empty, "Hyperliquid funding rates should not be empty"
    assert len(df_fund) > 0, "Hyperliquid funding rates length should be > 0"
    
    # Test Prices
    df_price = hl.fetch_prices(symbol, start_date, end_date)
    assert not df_price.empty, "Hyperliquid prices should not be empty"
    assert len(df_price) > 0, "Hyperliquid prices length should be > 0"
