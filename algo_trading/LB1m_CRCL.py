"""
Algorithmic Trading System Skeleton

This script provides the foundational class structure for a simple algorithmic
trading system, including a PublicAPIClient for market data and order
execution, a TradingContext to manage the overall process, and a
CandleBreakoutStrategy to define the trading logic.

Modifications:
- Cache access_token and account_id in PublicAPIClient __init__.
- Added fetch_latest_candles to get 1-min bars (last 2) for strategy.
- Run loop only during market hours (9:30 AM - 4:00 PM EST, Mon-Fri).
- Poll every 60 seconds for new candle data.
- Integrated order placement in strategy with actual API calls (using MARKET orders for simplicity).
- Strategy now takes api_client, symbol, and quantity for placing orders.
- Position size is managed simplistically (assume full fills); for production, query actual positions before trading.
- Added error handling for token expiration (basic refresh on 401).
- Imported necessary modules.
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

class PublicAPIClient:
    """
    Client for Public trading API.

    Handles authentication, caching token/account, market data, and orders.
    """

    def __init__(self, api_key: str, secret_key: str):
        """
        Initializes the API client, fetches and caches access token and account ID.
        
        Args:
            api_key (str): The API key for authentication.
            secret_key (str): The secret key for authentication.
        """
        self.api_key = api_key
        self.secret_key = secret_key
        self.access_token = None
        self.account_id = None
        self._authenticate()
        print("PublicAPIClient initialized with cached token and account ID.")

    def _authenticate(self):
        """Fetches access token and account ID."""
        data = {
            "validityInMinutes": 1440,  # 24 hours
            "secret": self.secret_key
        }
        r = requests.post("https://api.public.com/userapiauthservice/personal/access-tokens", json=data)
        if r.status_code != 200:
            raise ValueError("Failed to get access token: " + r.text)
        self.access_token = r.json()["accessToken"]
        
        headers = {'Authorization': f'Bearer {self.access_token}'}
        r = requests.get("https://api.public.com/userapigateway/trading/account", headers=headers)
        if r.status_code != 200:
            raise ValueError("Failed to get account ID: " + r.text)
        self.account_id = r.json()["accounts"][0]['accountId']

    def _refresh_token_if_needed(self, response):
        """Refreshes token on 401 unauthorized."""
        if response.status_code == 401:
            print("Access token expired. Refreshing...")
            self._authenticate()

    def fetch_latest_candles(self, symbol: str, timeframe: str = '1m', limit: int = 2) -> List[Dict[str, Any]]:
        """
        Fetches the latest historical bars (candles) for a symbol.

        Args:
            symbol (str): The stock symbol.
            timeframe (str): Candle timeframe (e.g., '1m' for 1 minute).
            limit (int): Number of bars to fetch (e.g., 2 for prev and current).

        Returns:
            List[Dict[str, Any]]: List of candle dicts with 'open', 'high', 'low', 'close', 'volume', 'timestamp'.
        """
        url = "https://data.alpaca.markets/v2/stocks/bars/latest?symbols=MSTX"

        headers = {"accept": "application/json"}

        response = requests.get(url, headers=headers)

        print(response.text)
        
        print(f"Fetching latest {timeframe} candles for {symbol}...")
        url = "https://api.public.com/marketdata/bars"
        params = {"symbol": symbol.upper(), "timeframe": timeframe, "limit": limit}
        headers = {'Authorization': f'Bearer {self.access_token}'}
        r = requests.get(url, params=params, headers=headers)
        self._refresh_token_if_needed(r)
        if r.status_code != 200:
            raise ValueError("Failed to fetch candles: " + r.text)
        return r.json()  # Assumes format: list of {'open': float, 'high': float, 'low': float, 'close': float, 'volume': int, 'timestamp': str}

    def place_order(self, symbol: str, side: str, quantity: int) -> bool:
        """
        Places a trading order.

        Args:
            symbol (str): The stock symbol.
            side (str): 'BUY' or 'SELL'.
            quantity (int): The number of shares to trade.

        Returns:
            bool: True if successful.
        """
        print(f"Placing {side} order for {quantity} shares of {symbol}...")
        url = f"https://api.public.com/userapigateway/trading/{self.account_id}/order"
        order_id = str(uuid.uuid4())
        data = {
            "orderId": order_id,
            "instrument": {
                "symbol": symbol.upper(),
                "type": "EQUITY"
            },
            "orderSide": side.upper(),
            "orderType": "MARKET",
            "expiration": {
                "timeInForce": "DAY"
            },
            "quantity": str(quantity)
        }
        headers = {'Authorization': f'Bearer {self.access_token}'}
        r = requests.post(url, json=data, headers=headers)
        self._refresh_token_if_needed(r)
        if r.status_code == 200:
            print("Order placed successfully.")
            return True
        else:
            print(f"Order failed: {r.text}")
            return False

class CandleBreakoutStrategy:
    def __init__(self, api_client: PublicAPIClient, symbol: str, quantity: int = 1):
        self.api_client = api_client
        self.symbol = symbol
        self.quantity = quantity
        self.position_size = 0
        self.prev_high = None
        self.prev_low = None
    
    def on_new_candle(self, current_candle_data: Dict[str, float]):
        """
        Processes new candle data to determine entry and exit signals.
        """
        # Ensure we have data from at least one previous candle
        if self.prev_high is None or self.prev_low is None:
            self.prev_high = current_candle_data['high']
            self.prev_low = current_candle_data['low']
            return
        
        current_close = current_candle_data['close']
        
        # Entry condition
        long_entry = current_close > self.prev_high and self.position_size == 0
        
        # Exit condition
        long_exit = current_close < self.prev_low and self.position_size > 0
        
        if long_entry:
            if self.api_client.place_order(self.symbol, 'BUY', self.quantity):
                self.position_size = self.quantity
        
        if long_exit:
            if self.api_client.place_order(self.symbol, 'SELL', self.position_size):
                self.position_size = 0
        
        # Update previous high and low for the next candle
        self.prev_high = current_candle_data['high']
        self.prev_low = current_candle_data['low']

class TradingContext:
    """
    Orchestrates the trading system.
    """
    def __init__(self, api_client: PublicAPIClient, strategy: CandleBreakoutStrategy, symbol: str):
        self.api_client = api_client
        self.strategy = strategy
        self.symbol = symbol
        self.portfolio: Dict[str, Any] = {"cash": 900.0, "positions": {}}
        print("TradingContext initialized.")

    def is_market_open(self) -> bool:
        """Checks if current time is within market hours (9:30-16:00 EST, Mon-Fri)."""
        est = pytz.timezone('US/Eastern')
        now = datetime.datetime.now(est)
        if now.weekday() >= 5:  # Saturday or Sunday
            return False
        open_time = datetime.time(9, 30)
        close_time = datetime.time(16, 0)
        return open_time <= now.time() < close_time

    def run_trading_loop(self, interval_seconds: int = 60):
        """
        Runs the trading loop, polling every minute during market hours.
        """
        print("Starting trading loop...")
        while True:
            try:
                if self.is_market_open():
                    candles = self.api_client.fetch_latest_candles(self.symbol, '1m', 2)
                    print("1")
                    if len(candles) == 2:
                        print("2")
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
                    time.sleep(300)  # Longer sleep outside hours
                    continue

                time.sleep(interval_seconds)
                print("10")
            
            except Exception as e:
                print(f"An error occurred: {e}")
                time.sleep(60)  # Retry after delay

if __name__ == "__main__":
    api_client = PublicAPIClient(api_key="your_api_key", secret_key=os.getenv("SECRET_API_KEY"))
    strategy = CandleBreakoutStrategy(api_client=api_client, symbol="MSTX", quantity=1)
    trading_app = TradingContext(api_client=api_client, strategy=strategy, symbol="MSTX")
    trading_app.run_trading_loop()