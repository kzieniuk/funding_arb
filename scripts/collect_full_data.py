import os
import sys
import time
import ccxt
import pandas as pd
from datetime import datetime, timedelta, timezone

# Add parent directory to path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collector.binance_futures import BinanceFuturesCollector
from collector.hyperliquid import HyperliquidCollector
from database.database_manager import DatabaseManager

def get_all_markets(markets):
    # Filter for USDT/USDC pairs which are active
    valid_symbols = [
        symbol for symbol, market in markets.items() 
        if (market['quote'] in ['USDT', 'USDC']) and market.get('active', True)
    ]
    return valid_symbols

def collect_exchange_data(exchange_name, collector, db, start_date, end_date):
    print(f"\n--- Starting Collection for {exchange_name} ---")
    try:
        markets = collector.fetch_markets()
        all_symbols = get_all_markets(markets)
        total_symbols = len(all_symbols)
        print(f"Found {total_symbols} active markets.")
        
        for i, symbol in enumerate(all_symbols):
            print(f"[{i+1}/{total_symbols}] Processing {symbol}...")
            
            # --- Funding ---
            try:
                for attempt in range(3):
                    try:
                        df_fund = collector.fetch_funding_rates(symbol, start_date, end_date)
                        if not df_fund.empty:
                            df_fund['ticker'] = symbol
                            db.insert_funding(df_fund, exchange_name)
                        else:
                            print(f"  Warning: No funding data found for {symbol}")
                        break # Success, exit retry loop
                    except (ccxt.RateLimitExceeded, ccxt.NetworkError) as e:
                        if attempt < 2:
                            wait_time = (attempt + 1) * 10
                            print(f"  RateLimit/Network error (Funding) for {symbol}: {e}. Retrying in {wait_time}s...")
                            time.sleep(wait_time)
                        else:
                            print(f"  Failed to fetch funding for {symbol} after 3 attempts due to: {e}")
                            raise e # Re-raise to be caught by outer, or just log and skip
                    except Exception as e:
                        print(f"  Error fetching funding for {symbol}: {e}")
                        break # Non-retriable error
            except Exception as e:
                # Outer catch to ensure we proceed to prices
                pass

            # --- Prices ---
            try:
                for attempt in range(3):
                    try:
                        df_price = collector.fetch_prices(symbol, start_date, end_date)
                        if not df_price.empty:
                            df_price['ticker'] = symbol
                            db.insert_ohlcv(df_price, exchange_name)
                        else:
                            print(f"  Warning: No price data found for {symbol}")
                        break
                    except (ccxt.RateLimitExceeded, ccxt.NetworkError) as e:
                        if attempt < 2:
                            wait_time = (attempt + 1) * 10
                            print(f"  RateLimit/Network error (Prices) for {symbol}: {e}. Retrying in {wait_time}s...")
                            time.sleep(wait_time)
                        else:
                            print(f"  Failed to fetch prices for {symbol} after 3 attempts due to: {e}")
                            raise e
                    except Exception as e:
                        print(f"  Error fetching prices for {symbol}: {e}")
                        break
            except Exception as e:
                pass
            
            # Basic rate limit sleep if needed, though ccxt handles it generally.
            # Adding a small manual pause just in case to be safe for long runs.
            time.sleep(0.1)

    except Exception as e:
        print(f"Critical error initializing {exchange_name} collection: {e}")

def main():
    print("Initializing collectors and database...")
    db = DatabaseManager('data/crypto.duckdb')
    
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=365) # 1 year data
    print(f"Collection period: {start_date} to {end_date}")

    # 1. Binance Futures
    bf = BinanceFuturesCollector()
    collect_exchange_data('binance_futures', bf, db, start_date, end_date)

    # 2. Hyperliquid
    hl = HyperliquidCollector()
    collect_exchange_data('hyperliquid', hl, db, start_date, end_date)

    db.close()
    print("\nFull Data Collection Complete.")

if __name__ == "__main__":
    main()
