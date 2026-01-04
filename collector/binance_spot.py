import ccxt
import pandas as pd
from datetime import datetime, timezone

class BinanceSpotCollector:
    def __init__(self):
        self.exchange = ccxt.binance({
            'options': {'defaultType': 'spot'},
            'enableRateLimit': True
        })

    def _to_timestamp(self, date_str):
        if isinstance(date_str, int):
            return date_str
        if isinstance(date_str, datetime):
            return int(date_str.replace(tzinfo=timezone.utc).timestamp() * 1000)
        dt = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

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
                
                if len(ohlcv) < 1000:
                    break

                if last_ts == since: 
                     since += duration_ms 
                else:
                    since = last_ts + 1

            except Exception as e:
                print(f"Error fetching prices: {e}")
                break
                
        df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        if not df.empty:
            df = df[df['timestamp'] <= end_ts]
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df
