import os
import time
import json
import hmac
import base64
import hashlib
import urllib.parse
import urllib.request
import websockets
import asyncio
import collections # <-- NEW IMPORT
from decimal import Decimal, ROUND_DOWN, getcontext
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Deque # <-- NEW IMPORT
from dotenv import load_dotenv

# Higher precision math for crypto sizing
getcontext().prec = 28

load_dotenv()
API_KEY = os.getenv("KRAKEN_API_KEY")
SECRET_KEY = os.getenv("KRAKEN_API_SECRET")  # base64 string from Kraken

ADD_ORDER_PATH = "/0/private/AddOrder"
BASE_URL = "https://api.kraken.com"
KRAKEN_WS_URI = "wss://ws.kraken.com/v2"
OHLC_PATH = "/0/public/OHLC"
PAIRS_PATH = "/0/public/AssetPairs"
BALANCE_PATH = "/0/private/Balance"
TICKER_PATH = "/0/public/Ticker"


# =============== HTTP helpers (Kept for initial pair config) ===============

def _nonce_ms() -> str:
    return str(int(time.time() * 1000))

def _kraken_sign(secret_b64: str, path: str, nonce: str, postdata_str: str) -> str:
    if not secret_b64:
        raise ValueError("KRAKEN_API_SECRET missing.")
    sha = hashlib.sha256((nonce + postdata_str).encode()).digest()
    msg = path.encode() + sha
    secret = base64.b64decode(secret_b64)
    sig = hmac.new(secret, msg, hashlib.sha512).digest()
    return base64.b64encode(sig).decode()

def _http_post(path: str, fields: Dict[str, str]) -> Dict[str, Any]:
    if not API_KEY or not SECRET_KEY:
        raise ValueError("Missing KRAKEN_API_KEY or KRAKEN_API_SECRET.")

    # Ensure nonce
    if "nonce" not in fields:
        fields["nonce"] = _nonce_ms()

    postdata = urllib.parse.urlencode(fields)
    headers = {
        "API-Key": API_KEY,
        "API-Sign": _kraken_sign(SECRET_KEY, path, fields["nonce"], postdata),
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "kraken-breakout-bot/1.0",
    }
    req = urllib.request.Request(
        url=BASE_URL + path,
        data=postdata.encode(),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode())
    return data

def _http_get(url: str, params: Dict[str, str]) -> Dict[str, Any]:
    qs = urllib.parse.urlencode(params)
    full = f"{url}?{qs}" if qs else url
    req = urllib.request.Request(full, method="GET", headers={"User-Agent": "kraken-breakout-bot/1.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())

# =============== Kraken client (Modified to remove polling) ===============

@dataclass
class PairRules:
    pair_key: str
    altname: str
    ordermin: Decimal
    costmin: Optional[Decimal]
    lot_decimals: int
    tick_size: Decimal
    quote: str
    base: str
    wsname: str # <-- NEW FIELD for WebSocket symbol ("ARB/USD")

class KrakenClient:
    def __init__(self, symbol: str, interval: int = 1):
        self.symbol_in = symbol.replace("/", "")
        self.interval = interval
        self.rules = self._fetch_pair_rules(self.symbol_in)
        # Use a Deque to store the last N candles for the strategy
        # Max length of 3: two for the strategy logic, one to handle the current incomplete candle
        self.candles: Deque[Dict[str, Decimal]] = collections.deque(maxlen=3) 

    def _fetch_pair_rules(self, altname_guess: str) -> PairRules:
        # ... (unchanged logic for fetching pair rules) ...
        data = _http_get(BASE_URL + PAIRS_PATH, {"pair": altname_guess})
        if data.get("error"):
            raise ValueError(f"AssetPairs error: {data['error']}")
        result = data.get("result", {})
        if not result:
            data = _http_get(BASE_URL + PAIRS_PATH, {})
            result = data.get("result", {})
            candidates = [ (k, v) for k, v in result.items() if v.get("altname", "").upper() == altname_guess.upper() ]
            if not candidates:
                raise ValueError(f"Could not resolve Kraken pair for {altname_guess}")
            key, pairinfo = candidates[0]
        else:
            key, pairinfo = list(result.items())[0]

        altname = pairinfo.get("altname")
        lot_decimals = int(pairinfo.get("lot_decimals", 8))
        tick_decimals = int(pairinfo.get("pair_decimals", 5))
        tick_size = Decimal("1").scaleb(-tick_decimals)

        ordermin = Decimal(str(pairinfo.get("ordermin", "0.0") or "0.0"))
        costmin = pairinfo.get("costmin")
        costmin = Decimal(str(costmin)) if costmin is not None else None

        wsname = pairinfo.get("wsname", "").replace("XBT", "BTC") # Normalize XBT/USD to BTC/USD for v2 WS
        if "/" in wsname:
            base, quote = wsname.split("/")
        else:
            base, quote = altname[:3], altname[3:]

        return PairRules(
            pair_key=key,
            altname=altname,
            ordermin=ordermin if ordermin > 0 else Decimal("0"),
            costmin=costmin,
            lot_decimals=lot_decimals,
            tick_size=tick_size,
            quote=quote,
            base=base,
            wsname=wsname # <-- RETURN NEW FIELD
        )

    def update_candles(self, new_candles: List[Dict[str, Any]]):
        """Called by the WebSocket client to update the internal candle history."""
        # The WebSocket sends an array of candles. We only care about the latest two *complete* ones.
        for raw_c in new_candles:
            candle_obj = {
                "open": Decimal(str(raw_c["open"])),
                "high": Decimal(str(raw_c["high"])),
                "low":  Decimal(str(raw_c["low"])),
                "close":Decimal(str(raw_c["close"])),
                "volume":Decimal(str(raw_c["volume"])),
                "time": int(time.time()), # WSv2 uses interval_begin, but we just need a timestamp
                # Note: The WS data contains 'interval', 'type', and 'interval_begin', which can be used to distinguish 
                # between a *complete* candle (when the interval changes) and an *incomplete* update.
                # For simplicity, we treat every update as a potential new candle close for the strategy.
            }
            # For a 1m breakout strategy, the 'update' message is the new close of the candle.
            # We must replace the last element (the incomplete candle) or append a new one.
            # A more robust solution would check interval_begin to see if it's a new candle or an update to the current one.
            # Simplistic approach: Assume the strategy cares about the *latest* OHLC state.
            if len(self.candles) > 0:
                self.candles.pop()
            self.candles.append(candle_obj)

    # ---- sizing helpers ----
    def _round_volume(self, vol: Decimal) -> Decimal:
        # lot step is 10^(-lot_decimals)
        step = Decimal("1").scaleb(-self.rules.lot_decimals)
        # round DOWN to the permitted step
        return (vol // step) * step

    def _min_volume_for_price(self, price: Decimal) -> Decimal:
        """
        Compute the minimum volume that satisfies both ordermin (base) and costmin (quote),
        rounded to lot decimals.
        """
        candidates = [self.rules.ordermin] if self.rules.ordermin and self.rules.ordermin > 0 else []
        if self.rules.costmin and self.rules.costmin > 0 and price > 0:
            needed = (self.rules.costmin / price)
            candidates.append(needed)
        if not candidates:
            return self._round_volume(Decimal("0"))
        vol = max(candidates)
        return self._round_volume(vol)

    def place_post_only_limit(self, side: str, volume: Decimal, price: Decimal, expire_s: int = 5) -> Dict[str, Any]:
        # ... (order placement logic) ...
        # Coerce volume up to min requirements
        min_vol = self._min_volume_for_price(price)
        if min_vol > 0 and volume < min_vol:
            volume = min_vol

        fields = {
            "nonce": _nonce_ms(),
            "ordertype": "limit",
            "type": side.lower(),
            "volume": f"{volume.normalize()}",
            "pair": self.rules.altname,               # Kraken likes altname for trading
            "price": f"{price.normalize()}",
            "oflags": "post",                         # post-only
            "timeinforce": "GTD",
            "expiretm": f"+{expire_s}",
        }
        resp = _http_post(ADD_ORDER_PATH, fields)
        return resp

    def get_balances(self) -> Dict[str, Any]:
        """Fetch account balances (private endpoint)."""
        fields: Dict[str, str] = {"nonce": _nonce_ms()}
        return _http_post(BALANCE_PATH, fields)

    def get_available_base_balance(self) -> Decimal:
        """Return available balance for the base asset of the trading pair."""
        try:
            resp = self.get_balances()
            if resp.get("error"):
                return Decimal("0")
            result = resp.get("result", {})
            # Kraken uses asset codes like "ARB", "USD", etc.
            raw = result.get(self.rules.base, "0")
            return Decimal(str(raw))
        except Exception:
            return Decimal("0")

    def get_best_bid_ask(self) -> Optional[Dict[str, Decimal]]:
        """Fetch best bid/ask from public Ticker for the current pair."""
        try:
            # Kraken Ticker uses pair key or altname. Use rules.altname to be safe.
            data = _http_get(BASE_URL + TICKER_PATH, {"pair": self.rules.altname})
            if data.get("error"):
                return None
            result = data.get("result", {})
            # result is a map of pair-> { a: [ask,...], b:[bid,...] }
            if not result:
                return None
            _, payload = list(result.items())[0]
            ask = Decimal(str(payload["a"][0]))
            bid = Decimal(str(payload["b"][0]))
            return {"bid": bid, "ask": ask}
        except Exception:
            return None

# =============== WebSocket Client (NEW) ===============

class OHLCSocketClient:
    def __init__(self, client: KrakenClient, strategy: Optional["CandleBreakoutStrategy"] = None):
        self.client = client
        self.strategy = strategy
        self.ws_uri = KRAKEN_WS_URI
        self.symbol = client.rules.wsname  # Use "ARB/USD"
        self.interval = client.interval   # e.g., 1 minute
        self.subscription_msg = {
            "method": "subscribe",
            "params": {
                "channel": "ohlc",
                "symbol": [self.symbol],
                "interval": self.interval,
                "snapshot": True,
            }
        }
        self.is_subscribed = False

    async def run(self):
        """Manages the connection and listens for OHLC data."""
        while True:
            try:
                print(f"Connecting to Kraken WebSocket V2 for {self.symbol}...")
                async with websockets.connect(self.ws_uri) as websocket:
                    # Send subscription message
                    await websocket.send(json.dumps(self.subscription_msg))
                    print(f"Sent OHLC subscription for {self.symbol}@{self.interval}m.")

                    # Process incoming messages
                    async for message in websocket:
                        data = json.loads(message)

                        print(f"[WS Message] {data}")
                        
                        # Handle connection status messages
                        if data.get("channel") == "status":
                            print(f"Connection status: {data.get('type', 'unknown')}")
                            continue
                            
                        # Handle subscription acknowledgement
                        if data.get("method") == "subscribe" and data.get("success") == True:
                            result = data.get("result", {})
                            print(f"Subscription confirmed for {result.get('channel', 'unknown')} channel!")
                            self.is_subscribed = True
                            continue
                        
                        # Handle heartbeat messages
                        if data.get("channel") == "heartbeat":
                            # Print position size, latest close, and previous candle high/low
                            pos_str = "n/a"
                            prev_h_str = "n/a"
                            prev_l_str = "n/a"
                            last_close_str = "n/a"

                            if self.strategy is not None:
                                pos_str = f"{self.strategy.position_size}"
                                if self.strategy.prev_high is not None:
                                    prev_h_str = f"{self.strategy.prev_high}"
                                if self.strategy.prev_low is not None:
                                    prev_l_str = f"{self.strategy.prev_low}"

                            if len(self.client.candles) >= 1:
                                try:
                                    last_close_str = f"{self.client.candles[-1]['close']}"
                                except Exception:
                                    pass

                            print(
                                f"{self.symbol} pos: {pos_str} | "
                                f"close: {last_close_str} | prev H/L: {prev_h_str}/{prev_l_str}"
                            )
                            continue
                        
                        # Handle OHLC data update (both snapshot and ongoing updates use 'ohlc' channel)
                        if data.get("channel") == "ohlc":
                            msg_type = data.get("type", "unknown")
                            print(f"Received OHLC {msg_type} message")
                            
                            # 1. Extract the list of candles from the 'data' key
                            raw_candles_list = data.get("data", []) 
                            
                            if raw_candles_list:
                                # 2. Pass the entire list to the main client's update function
                                self.client.update_candles(raw_candles_list) 
                                print(f"Processed {len(raw_candles_list)} candle(s) from {msg_type}.")
                            else:
                                print(f"Empty candle data in {msg_type} message")
                            
                        # Handle errors
                        if data.get("error"):
                            print(f"Kraken WebSocket Error: {data['error']}")
                            raise Exception("WebSocket API Error")

            except (websockets.ConnectionClosed, ConnectionRefusedError) as e:
                print(f"WebSocket connection lost/refused. Retrying in 5 seconds... ({e})")
                self.is_subscribed = False
                await asyncio.sleep(5)
            except Exception as e:
                print(f"An unexpected error occurred in WebSocket client: {e}. Retrying in 10 seconds.")
                self.is_subscribed = False
                await asyncio.sleep(10)

# =============== Strategy & Context (Modified for async) ===============

class CandleBreakoutStrategy:
    # ... (init and logic remains the same, but will be called differently) ...
    def __init__(self, client: KrakenClient, symbol: str, qty: Decimal = Decimal("1"), buffer_pct: Decimal = Decimal("0.02")):
        self.client = client
        self.symbol = symbol
        self.qty = qty
        self.buffer = buffer_pct
        self.position_size = Decimal("0")
        self.prev_high: Optional[Decimal] = None
        self.prev_low: Optional[Decimal] = None
        self.last_candle_time: Optional[int] = None # NEW: To track when a new candle is complete

    def on_new_candle(self, current: Dict[str, Decimal]):
        # Check if the candle time has actually rolled over (basic check)
        if self.last_candle_time is not None and self.last_candle_time >= current.get("time", 0):
            # This is just an update to the *current* candle, not a completed one.
            return 
        self.last_candle_time = current.get("time")


        # Initialize prev H/L on first tick
        if self.prev_high is None or self.prev_low is None:
            self.prev_high = current["high"]
            self.prev_low = current["low"]
            return

        close = current["close"]
        long_entry = (close > self.prev_high) and (self.position_size == 0)
        long_exit  = (close < self.prev_low) and (self.position_size > 0)

        # For post-only: BUY below market; SELL above market to avoid immediate match
        if long_entry:
            # Initial target below last close
            tick = self.client.rules.tick_size
            buy_price = (close * (Decimal("1.0") - self.buffer)).quantize(tick, rounding=ROUND_DOWN)
            attempts = 0
            while attempts < 3:
                resp = self.client.place_post_only_limit("buy", self.qty, buy_price, expire_s=5)
                print("BUY resp:", json.dumps(resp, indent=2))
                if not resp.get("error"):
                    self.position_size = self.qty
                    break
                # Refresh from ticker and move to maker-safe level at bid - 1 tick
                ba = self.client.get_best_bid_ask()
                if not ba:
                    break
                buy_price = (ba["bid"] - tick).quantize(tick, rounding=ROUND_DOWN)
                attempts += 1

        if long_exit:
            tick = self.client.rules.tick_size
            sell_price = (close * (Decimal("1.0") + self.buffer)).quantize(tick, rounding=ROUND_DOWN)
            # Use available balance to avoid insufficient funds on sell
            available_base = self.client.get_available_base_balance()
            sell_volume = min(self.position_size, available_base) if available_base > 0 else Decimal("0")
            if sell_volume <= 0:
                print(f"Skip SELL: no available {self.client.rules.base} balance")
                return
            attempts = 0
            while attempts < 3:
                resp = self.client.place_post_only_limit("sell", sell_volume, sell_price, expire_s=5)
                print("SELL resp:", json.dumps(resp, indent=2))
                if not resp.get("error"):
                    self.position_size -= sell_volume
                    break
                # Refresh from ticker and move to maker-safe level at ask + 1 tick
                ba = self.client.get_best_bid_ask()
                if not ba:
                    break
                sell_price = (ba["ask"] + tick).quantize(tick, rounding=ROUND_DOWN)
                attempts += 1

        # Roll previous candle levels
        self.prev_high = current["high"]
        self.prev_low  = current["low"]
        print(f"[{self.symbol}] Close: {close} | New Prev H/L: {self.prev_high}/{self.prev_low} | Pos: {self.position_size}")


class TradingContext:
    def __init__(self, client: KrakenClient, strategy: CandleBreakoutStrategy, symbol: str):
        self.client = client
        self.strategy = strategy
        self.symbol = symbol
        self.ws_client = OHLCSocketClient(client, strategy)

    async def run(self):
        """Main entry point, running the WS client and strategy loop concurrently."""
        print("Starting crypto trading loop (WS candles)...")
        # Sync local position with exchange balance at startup
        try:
            available_base = self.client.get_available_base_balance()
            if available_base > 0:
                self.strategy.position_size = available_base
                print(f"Synced position from exchange: {self.symbol} base balance = {available_base}")
            else:
                print(f"No base balance detected for {self.symbol.split('USD')[0]}; starting flat")
        except Exception as e:
            print(f"Could not sync starting position: {e}")
        # Run the WebSocket client in the background
        ws_task = asyncio.create_task(self.ws_client.run())

        # The main strategy loop runs independently, checking for new data
        while True:
            # Wait for the WebSocket client to be active and have enough data
            if self.ws_client.is_subscribed and len(self.client.candles) >= 1:
                # The strategy only needs the latest candle to determine the breakout
                last_candle = self.client.candles[-1]
                
                current = {
                    "high": last_candle["high"],
                    "low": last_candle["low"],
                    "close": last_candle["close"],
                    "time": last_candle["time"],
                }

                # IMPORTANT: Run the strategy logic based on the latest OHLC update
                self.strategy.on_new_candle(current)
            else:
                print("Waiting for WS connection and initial candle data...")
            
            # Since the WS provides data instantly, we don't need a heavy poll_seconds anymore.
            # A short sleep is good practice to yield control and prevent a tight loop.
            await asyncio.sleep(0.5)

# =============== Main (Modified for async) ===============

if __name__ == "__main__":
    if not API_KEY or not SECRET_KEY:
        raise SystemExit("Set KRAKEN_API_KEY and KRAKEN_API_SECRET in your environment/.env")

    SYMBOL = "ARBUSD"
    OHLC_INTERVAL_MINS = 1 # The interval the strategy is designed for

    client = KrakenClient(SYMBOL, interval=OHLC_INTERVAL_MINS) # Pass interval
    strategy = CandleBreakoutStrategy(client, SYMBOL, qty=Decimal("70"), buffer_pct=Decimal("0.02"))
    ctx = TradingContext(client, strategy, SYMBOL)
    
    # Run the main asynchronous context
    try:
        asyncio.run(ctx.run())
    except KeyboardInterrupt:
        print("\nClient terminated by user.")