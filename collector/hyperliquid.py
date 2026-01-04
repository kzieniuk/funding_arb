import ccxt
import pandas as pd
from datetime import datetime, timezone

class HyperliquidCollector:
    def __init__(self):
        self.exchange = ccxt.hyperliquid({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',
                'fetchMarkets': {
                    'types': ['swap'] # Only fetch perps to avoid "Too many DEXes" error from HIP-3
                }
            }, 
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
        
        :param symbol: Trading symbol (e.g., 'BTC/USDC:USDC') - Check CCXT mapping
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
                # fetchFundingRateHistory is not always standard, check if supported. 
                # If not, fallback to fetchFundingRate (current) which doesn't give history.
                # However, many exchanges in CCXT support fetchFundingRateHistory.
                # If Hyperliquid doesn't, we might need a workaround.
                # Assuming CCXT standard compliance or standard method availability.
                if self.exchange.has['fetchFundingRateHistory']:
                    rates = self.exchange.fetch_funding_rate_history(symbol, since=since, limit=1000)
                else:
                    # Provide empty list if not supported to avoid crash, or better, implement custom if needed
                    # But for now, let's assume it works or we catch exception.
                    print("Warning: fetchFundingRateHistory not supported by this ccxt version for Hyperliquid.")
                    break
                
                if not rates:
                    break
                
                new_rates = [r for r in rates if r['timestamp'] >= since]
                if not new_rates:
                    break

                all_funding_rates.extend(new_rates)
                
                last_ts = rates[-1]['timestamp']
                if last_ts == since:
                    break
                since = last_ts + 1
                
                if since >= end_ts:
                    break
                    
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
        
        :param symbol: Trading symbol
        :param start_date: Start date
        :param end_date: End date
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
