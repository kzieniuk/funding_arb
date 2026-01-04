import os
import sys
import random
import pandas as pd
from datetime import datetime, timedelta, timezone

# Add parent directory to path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collector.binance_futures import BinanceFuturesCollector
from collector.hyperliquid import HyperliquidCollector
from database.database_manager import DatabaseManager

def get_random_markets(markets, count=10):
    # Filter for USDT/USDC pairs which are active/spot/future depending on context
    # For futures, usually end in USDT or USDC
    valid_symbols = [
        symbol for symbol, market in markets.items() 
        if (market['quote'] in ['USDT', 'USDC']) and market.get('active', True)
    ]
    
    if len(valid_symbols) < count:
        return valid_symbols
    return random.sample(valid_symbols, count)

def main():
    print("Initializing collectors and database...")
    db = DatabaseManager('data/crypto.duckdb')
    bf = BinanceFuturesCollector()
    hl = HyperliquidCollector()
    
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=30)
    
    print(f"Collection period: {start_date} to {end_date}")
    
    # --- Binance Futures ---
    print("\n--- Binance Futures Collection ---")
    try:
        markets = bf.fetch_markets()
        print(f"Fetched {len(markets)} markets.")
        
        # Convert list to dict for helper function if needed, or adjust helper
        # ccxt load_markets returns dict, but we returned load_markets() result directly which IS a dict.
        # Wait, bf.fetch_markets returns load_markets() result which is a dict of symbol->market.
        
        target_symbols = get_random_markets(markets, 10)
        print(f"Selected symbols: {target_symbols}")
        
        for symbol in target_symbols:
            print(f"Processing {symbol}...")
            try:
                # Funding
                df_fund = bf.fetch_funding_rates(symbol, start_date, end_date)
                if not df_fund.empty:
                    df_fund['ticker'] = symbol
                    db.insert_funding(df_fund, 'binance_futures')
                
                # Prices
                df_price = bf.fetch_prices(symbol, start_date, end_date)
                if not df_price.empty:
                    df_price['ticker'] = symbol
                    db.insert_ohlcv(df_price, 'binance_futures')
                    
            except Exception as e:
                print(f"Error processing {symbol}: {e}")
                
    except Exception as e:
        print(f"Binance Futures error: {e}")

    # --- Hyperliquid ---
    print("\n--- Hyperliquid Collection ---")
    try:
        markets = hl.fetch_markets()
        print(f"Fetched {len(markets)} markets.")
        
        target_symbols = get_random_markets(markets, 10)
        print(f"Selected symbols: {target_symbols}")
        
        for symbol in target_symbols:
            print(f"Processing {symbol}...")
            try:
                # Funding
                df_fund = hl.fetch_funding_rates(symbol, start_date, end_date)
                if not df_fund.empty:
                    df_fund['ticker'] = symbol
                    db.insert_funding(df_fund, 'hyperliquid')
                
                # Prices
                df_price = hl.fetch_prices(symbol, start_date, end_date)
                if not df_price.empty:
                    df_price['ticker'] = symbol
                    db.insert_ohlcv(df_price, 'hyperliquid')
                    
            except Exception as e:
                print(f"Error processing {symbol}: {e}")

    except Exception as e:
         print(f"Hyperliquid error: {e}")

    db.close()
    print("\nDone.")

if __name__ == "__main__":
    main()
