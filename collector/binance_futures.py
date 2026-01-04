import ccxt
import pandas as pd
import time
from datetime import datetime, timezone

class BinanceFuturesCollector:
    def __init__(self):
        self.exchange = ccxt.binance({
            'options': {'defaultType': 'future'},
            'enableRateLimit': True
        })

    def fetch_markets(self):
        """
        Fetch all available markets.
        :return: List of market objects
        """
        return self.exchange.load_markets()
    
    def _to_timestamp(self, date_str):
        if isinstance(date_str, int):
            return date_str
        if isinstance(date_str, datetime):
            return int(date_str.replace(tzinfo=timezone.utc).timestamp() * 1000)
        dt = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

    def fetch_funding_rates(self, symbol, start_date, end_date):
        """
        Fetch funding rates for a specific symbol within a date range.
        
        :param symbol: Trading symbol (e.g., 'BTC/USDT')
        :param start_date: Start date (ISO string, datetime, or ms timestamp)
        :param end_date: End date (ISO string, datetime, or ms timestamp)
        :return: DataFrame containing funding rates
        """
        start_ts = self._to_timestamp(start_date)
        end_ts = self._to_timestamp(end_date)
        
        all_funding_rates = []
        since = start_ts
        
        while since < end_ts:
            try:
                # Binance fetchFundingRateHistory usually takes symbol, since, limit
                # Note: support varies by exchange, but binance has it.
                rates = self.exchange.fetch_funding_rate_history(symbol, since=since, limit=1000)
                
                if not rates:
                    break
                
                # Filter out any that are before our since time (redundancy check) 
                # and append
                new_rates = [r for r in rates if r['timestamp'] >= since]
                
                if not new_rates:
                    # If we get results but none are new (shouldn't happen with correct usage), break to prevent infinite loop
                    break

                all_funding_rates.extend(new_rates)
                
                # Update since to the last timestamp + 1ms to get next batch
                last_ts = rates[-1]['timestamp']
                if last_ts == since:
                    # Avoid infinite loop if no progress
                    break
                since = last_ts + 1
                
                if since >= end_ts:
                    break
                
                # Small sleep to be safe, though rate limiter handles it
                # time.sleep(0.1) 
                
            except Exception as e:
                print(f"Error fetching funding rates: {e}")
                break
        
        df = pd.DataFrame(all_funding_rates)
        if not df.empty:
            df = df[df['timestamp'] <= end_ts]
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df

    def fetch_prices(self, symbol, start_date, end_date, timeframe='1h'):
        """
        Fetch OHLCV prices for a specific symbol within a date range.
        
        :param symbol: Trading symbol (e.g., 'BTC/USDT')
        :param start_date: Start date (ISO string, datetime, or ms timestamp)
        :param end_date: End date (ISO string, datetime, or ms timestamp)
        :param timeframe: Timeframe string (default '1h')
        :return: DataFrame containing OHLCV data
        """
        start_ts = self._to_timestamp(start_date)
        end_ts = self._to_timestamp(end_date)
        
        all_ohlcv = []
        since = start_ts
        
        while since < end_ts:
            try:
                ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=1000)
                
                if not ohlcv:
                    break
                
                all_ohlcv.extend(ohlcv)
                
                last_ts = ohlcv[-1][0]
                
                # Calculate duration of one candle to increment 'since' safely
                duration_ms = self.exchange.parse_timeframe(timeframe) * 1000
                
                # If we received fewer items than limit, we might have reached the end or current time
                if len(ohlcv) < 1000:
                    break

                # Next batch starts after the last candle
                # However, confirm we are actually moving forward
                if last_ts == since: 
                     since += duration_ms # Force move if stuck
                else:
                    since = last_ts + 1

            except Exception as e:
                print(f"Error fetching prices: {e}")
                break
                
        # Columns: timestamp, open, high, low, close, volume
        df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        if not df.empty:
            df = df[df['timestamp'] <= end_ts]
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df
