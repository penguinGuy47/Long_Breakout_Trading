"""
Algorithmic Trading System Skeleton

This script uses Alpaca API for fetching 1-minute candle data and Public API for order execution.
Runs during market hours (9:30 AM - 4:00 PM EST) and polls every 60 seconds.
"""

import time
import os
import uuid
import requests
import datetime
import pytz
from dotenv import load_dotenv
from typing import Dict, Any, List

load_dotenv()

class HybridAPIClient:
    """
    Hybrid client using Alpaca for market data and Public for trading.
    """

    def __init__(self, alpaca_api_key: str, alpaca_secret_key: str, public_api_key: str, public_secret_key: str):
        """
        Initializes the hybrid API client with credentials.
        
        Args:
            alpaca_api_key (str): Alpaca API key.
            alpaca_secret_key (str): Alpaca secret key.
            public_api_key (str): Public API key.
            public_secret_key (str): Public secret key.
        """
        # Alpaca setup
        self.alpaca_headers = {
            "APCA-API-KEY-ID": alpaca_api_key,
            "APCA-API-SECRET-KEY": alpaca_secret_key,
            "accept": "application/json"
        }
        
        # Public setup
        self.public_api_key = public_api_key
        self.public_secret_key = public_secret_key
        self.public_access_token = None
        self.public_account_id = None
        self._authenticate_public()
        print("HybridAPIClient initialized with Alpaca data and Public trading credentials.")

    def _authenticate_public(self):
        """Fetches and caches Public access token and account ID."""
        print(f"Attempting Public authentication with API Key: {self.public_api_key[:4]}...")
        data = {"validityInMinutes": 1440, "secret": self.public_secret_key}
        headers = {'Content-Type': 'application/json'}
        try:
            r = requests.post("https://api.public.com/userapiauthservice/personal/access-tokens", 
                            json=data, headers=headers, timeout=10)
            print(f"Authentication response: {r.status_code}, {r.text}")
            if r.status_code != 200:
                raise ValueError(f"Failed to get access token: {r.status_code}, {r.text}")
            self.public_access_token = r.json()["accessToken"]
        except requests.exceptions.RequestException as e:
            raise ValueError(f"Network error during authentication: {str(e)}")

        headers = {'Authorization': f'Bearer {self.public_access_token}'}
        try:
            r = requests.get("https://api.public.com/userapigateway/trading/account", 
                           headers=headers, timeout=10)
            print(f"Account fetch response: {r.status_code}, {r.text}")
            if r.status_code != 200:
                raise ValueError(f"Failed to get account ID: {r.status_code}, {r.text}")
            self.public_account_id = r.json()["accounts"][0]['accountId']
        except requests.exceptions.RequestException as e:
            raise ValueError(f"Network error fetching account: {str(e)}")

    def _refresh_token_if_needed(self, response):
        """Refreshes Public token on 401 unauthorized."""
        if response.status_code == 401:
            print("Access token expired. Refreshing...")
            self._authenticate_public()

    def fetch_latest_candles(self, symbol: str, timeframe: str = '1Min', limit: int = 2) -> List[Dict[str, Any]]:
        """
        Fetches the latest historical bars (candles) for a symbol using Alpaca API.

        Args:
            symbol (str): The stock symbol (e.g., 'MSTX').
            timeframe (str): Candle timeframe (e.g., '1Min' for 1 minute).
            limit (int): Number of bars to fetch.

        Returns:
            List[Dict[str, Any]]: List of candle dicts.
        """
        print(f"Fetching latest {timeframe} candles for {symbol} from Alpaca...")
        url = "https://data.alpaca.markets/v2/stocks/bars"
        params = {
            "symbols": symbol.upper(),
            "timeframe": timeframe,
            "limit": limit,
            "adjustment": "raw"
        }
        try:
            r = requests.get(url, headers=self.alpaca_headers, params=params, timeout=10)
            if r.status_code != 200:
                raise ValueError(f"Failed to fetch candles: {r.status_code}, {r.text}")
            data = r.json()
            if not data.get("bars", {}).get(symbol.upper()):
                raise ValueError(f"No bars data for {symbol}: {r.text}")
            bars = data["bars"][symbol.upper()]
            formatted_bars = [
                {
                    'open': bar['o'],
                    'high': bar['h'],
                    'low': bar['l'],
                    'close': bar['c'],
                    'volume': bar['v'],
                    'timestamp': bar['t']
                } for bar in bars
            ]
            return formatted_bars
        except requests.exceptions.RequestException as e:
            raise ValueError(f"Network error fetching candles: {str(e)}")

    def place_order(self, symbol: str, side: str, quantity: int, buffer_pct: float = 0.022) -> bool:
        """
        Places a trading order using Public API.

        Args:
            symbol (str): The stock symbol.
            side (str): 'BUY' or 'SELL'.
            quantity (int): The number of shares to trade.
            buffer_pct (float): Buffer percentage for limit price.

        Returns:
            bool: True if successful.
        """
        print(f"Placing {side} order for {quantity} shares of {symbol} via Public...")
        # Fetch last price from Alpaca (simplified; use for limit price)
        candles = self.fetch_latest_candles(symbol, '1Min', 1)
        last_price = candles[0]['close'] if candles else None
        if not last_price:
            print("Failed to fetch last price for limit order.")
            return False
        
        # Calculate limit price
        if side.upper() == 'BUY':
            limit_price = last_price * (1 + buffer_pct)
        else:  # SELL
            limit_price = last_price * (1 - buffer_pct)
        
        url = f"https://api.public.com/userapigateway/trading/{self.public_account_id}/order"
        order_id = str(uuid.uuid4())
        data = {
            "orderId": order_id,
            "instrument": {"symbol": symbol.upper(), "type": "EQUITY"},
            "orderSide": side.upper(),
            "orderType": "LIMIT",
            "expiration": {"timeInForce": "DAY"},
            "quantity": str(quantity),
            "limitPrice": str(round(limit_price, 2))
        }
        headers = {'Authorization': f'Bearer {self.public_access_token}'}
        r = requests.post(url, json=data, headers=headers, timeout=10)
        self._refresh_token_if_needed(r)
        if r.status_code == 200:
            print(f"LIMIT {side} order placed at {limit_price}")
            return True
        else:
            print(f"Order failed: {r.status_code}, {r.text}")
            # Fallback to MARKET
            data["orderType"] = "MARKET"
            del data["limitPrice"]
            r = requests.post(url, json=data, headers=headers, timeout=10)
            success = r.status_code == 200
            print(f"Fallback to MARKET: {'success' if success else 'failed'}")
            return success

class CandleBreakoutStrategy:
    def __init__(self, api_client: HybridAPIClient, symbol: str, quantity: int = 1):
        self.api_client = api_client
        self.symbol = symbol
        self.quantity = quantity
        self.position_size = 0
        self.prev_high = None
        self.prev_low = None
    
    def on_new_candle(self, current_candle_data: Dict[str, float]):
        if self.prev_high is None or self.prev_low is None:
            self.prev_high = current_candle_data['high']
            self.prev_low = current_candle_data['low']
            return
        
        current_close = current_candle_data['close']
        
        long_entry = current_close > self.prev_high and self.position_size == 0
        long_exit = current_close < self.prev_low and self.position_size > 0
        
        if long_entry:
            if self.api_client.place_order(self.symbol, 'BUY', self.quantity):
                self.position_size = self.quantity
        
        if long_exit:
            if self.api_client.place_order(self.symbol, 'SELL', self.position_size):
                self.position_size = 0
        
        self.prev_high = current_candle_data['high']
        self.prev_low = current_candle_data['low']

class TradingContext:
    def __init__(self, api_client: HybridAPIClient, strategy: CandleBreakoutStrategy, symbol: str):
        self.api_client = api_client
        self.strategy = strategy
        self.symbol = symbol
        self.portfolio = {"cash": 10000.0, "positions": {}}
        print("TradingContext initialized.")

    def is_market_open(self) -> bool:
        est = pytz.timezone('US/Eastern')
        now = datetime.datetime.now(est)
        if now.weekday() >= 5:
            return False
        open_time = datetime.time(9, 30)
        close_time = datetime.time(16, 0)
        return open_time <= now.time() < close_time

    def run_trading_loop(self, interval_seconds: int = 60):
        print("Starting trading loop...")
        while True:
            try:
                if self.is_market_open():
                    candles = self.api_client.fetch_latest_candles(self.symbol, '1Min', 2)
                    if len(candles) == 2:
                        current_candle = {
                            'high': candles[-1]['high'],
                            'low': candles[-1]['low'],
                            'close': candles[-1]['close']
                        }
                        self.strategy.on_new_candle(current_candle)
                    else:
                        print("Insufficient candles fetched.")
                else:
                    print("Market closed. Sleeping for 5 minutes...")
                    time.sleep(300)
                    continue
                time.sleep(interval_seconds)
            except Exception as e:
                print(f"An error occurred: {e}")
                time.sleep(60)

if __name__ == "__main__":
    api_client = HybridAPIClient(
        alpaca_api_key=os.getenv("ALPACA_API_KEY"),
        alpaca_secret_key=os.getenv("ALPACA_SECRET_KEY"),
        public_api_key="your_public_api_key",
        public_secret_key=os.getenv("PUBLIC_SECRET_KEY")
    )
    strategy = CandleBreakoutStrategy(api_client=api_client, symbol="MSTX", quantity=1)
    trading_app = TradingContext(api_client=api_client, strategy=strategy, symbol="MSTX")
    trading_app.run_trading_loop()