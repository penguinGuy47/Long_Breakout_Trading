"""
Kraken Candle-Breakout Bot (1m)
- Fetches 1m OHLC from Kraken public API
- Places post-only GTD 5s limit orders on breakout
- Auto-adjusts volume to satisfy ordermin, costmin, and lot_decimals
"""

import os
import time
import json
import hmac
import base64
import hashlib
import urllib.parse
import urllib.request
import websockets
from decimal import Decimal, ROUND_DOWN, getcontext
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
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


# =============== HTTP helpers ===============

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

# =============== Kraken client ===============

@dataclass
class PairRules:
    pair_key: str           # canonical pair key used in responses (e.g., "ARBUSD")
    altname: str            # altname
    ordermin: Decimal       # minimum base-asset amount
    costmin: Optional[Decimal]  # minimum quote-asset value (may be None)
    lot_decimals: int       # decimals for volume step
    tick_size: Decimal      # min price increment
    quote: str              # quote currency, e.g., "USD"
    base: str               # base currency, e.g., "ARB"

class KrakenClient:
    def __init__(self, symbol: str):
        # Accept "ARBUSD" or "ARB/USD"; normalize to no-slash for requests
        self.symbol_in = symbol.replace("/", "")
        self.rules = self._fetch_pair_rules(self.symbol_in)

    def _fetch_pair_rules(self, altname_guess: str) -> PairRules:
        # Query asset pairs; find entry that matches our altname
        data = _http_get(BASE_URL + PAIRS_PATH, {"pair": altname_guess})
        if data.get("error"):
            raise ValueError(f"AssetPairs error: {data['error']}")
        result = data.get("result", {})
        if not result:
            # Fallback: try without passing pair filter
            data = _http_get(BASE_URL + PAIRS_PATH, {})
            result = data.get("result", {})
            # Try to find by altname across all
            candidates = [ (k, v) for k, v in result.items() if v.get("altname", "").upper() == altname_guess.upper() ]
            if not candidates:
                raise ValueError(f"Could not resolve Kraken pair for {altname_guess}")
            key, pairinfo = candidates[0]
        else:
            # When filtered, result may include one item but unknown key; pick first
            key, pairinfo = list(result.items())[0]

        altname = pairinfo.get("altname")
        lot_decimals = int(pairinfo.get("lot_decimals", 8))
        tick_decimals = int(pairinfo.get("pair_decimals", 5))
        tick_size = Decimal("1").scaleb(-tick_decimals)

        # Kraken may include 'ordermin' and 'costmin' in 'fees_maker' blocks or top-level; prefer top-level if present
        ordermin = Decimal(str(pairinfo.get("ordermin", "0.0") or "0.0"))
        costmin = pairinfo.get("costmin")
        costmin = Decimal(str(costmin)) if costmin is not None else None

        wsname = pairinfo.get("wsname", "")  # e.g., "ARB/USD"
        if "/" in wsname:
            base, quote = wsname.split("/")
        else:
            # fallback parse altname halves (best-effort)
            base, quote = altname[:3], altname[3:]

        return PairRules(
            pair_key=key,
            altname=altname,
            ordermin=ordermin if ordermin > 0 else Decimal("0"),
            costmin=costmin,
            lot_decimals=lot_decimals,
            tick_size=tick_size,
            quote=quote,
            base=base
        )

    def fetch_latest_candles(self, limit: int = 2, interval: int = 1) -> List[Dict[str, Any]]:
        """
        Returns the latest candles as a list of dicts: [{open, high, low, close, volume, time}, ...]
        interval: minutes (1, 5, 15, 60, etc. per Kraken)
        """
        data = _http_get(BASE_URL + OHLC_PATH, {"pair": self.rules.altname, "interval": str(interval)})
        if data.get("error"):
            raise ValueError(f"OHLC error: {data['error']}")
        # result is { "<pairkey>": [[time, open, high, low, close, vwap, volume, count], ...], "last": n }
        candles_raw = next(iter(data["result"].values()))
        out = []
        for c in candles_raw[-limit:]:
            out.append({
                "open": Decimal(str(c[1])),
                "high": Decimal(str(c[2])),
                "low":  Decimal(str(c[3])),
                "close":Decimal(str(c[4])),
                "volume":Decimal(str(c[6])),
                "time": int(c[0]),
            })
        return out

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
        """
        Place post-only GTD limit order. Automatically coerces volume up to min if needed.
        side: 'buy' or 'sell'
        """
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

# =============== Strategy & Context ===============

class CandleBreakoutStrategy:
    """
    Long-only breakout:
    - Enter when close > previous candle high AND no position
    - Exit when close < previous candle low AND have position
    Post-only limits: for BUY place slightly *below* last close; for SELL place slightly *above*.
    """
    def __init__(self, client: KrakenClient, symbol: str, qty: Decimal = Decimal("1"), buffer_pct: Decimal = Decimal("0.02")):
        self.client = client
        self.symbol = symbol
        self.qty = qty
        self.buffer = buffer_pct
        self.position_size = Decimal("0")
        self.prev_high: Optional[Decimal] = None
        self.prev_low: Optional[Decimal] = None

    def on_new_candle(self, current: Dict[str, Decimal]):
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
            buy_price = (close * (Decimal("1.0") - self.buffer)).quantize(self.client.rules.tick_size, rounding=ROUND_DOWN)
            resp = self.client.place_post_only_limit("buy", self.qty, buy_price, expire_s=5)
            print("BUY resp:", json.dumps(resp, indent=2))
            if not resp.get("error"):
                self.position_size = self.qty

        if long_exit:
            sell_price = (close * (Decimal("1.0") + self.buffer)).quantize(self.client.rules.tick_size, rounding=ROUND_DOWN)
            resp = self.client.place_post_only_limit("sell", self.position_size, sell_price, expire_s=5)
            print("SELL resp:", json.dumps(resp, indent=2))
            if not resp.get("error"):
                self.position_size = Decimal("0")

        # Roll previous candle levels
        self.prev_high = current["high"]
        self.prev_low  = current["low"]

class TradingContext:
    def __init__(self, client: KrakenClient, strategy: CandleBreakoutStrategy, symbol: str):
        self.client = client
        self.strategy = strategy
        self.symbol = symbol

    def run(self, poll_seconds: int = 60):
        print("Starting crypto trading loop (2m candles)...")
        while True:
            try:
                candles = self.client.fetch_latest_candles(limit=2, interval=2)
                if len(candles) >= 2:
                    last = candles[-1]
                    current = {
                        "high": last["high"],
                        "low": last["low"],
                        "close": last["close"],
                    }
                    print(f"[{self.symbol}] close={current['close']} high={current['high']} low={current['low']}")
                    self.strategy.on_new_candle(current)
                else:
                    print("Insufficient candles returned.")
                time.sleep(poll_seconds)
            except Exception as e:
                print("Error in loop:", e)
                time.sleep(10)

# =============== Main ===============

if __name__ == "__main__":
    if not API_KEY or not SECRET_KEY:
        raise SystemExit("Set KRAKEN_API_KEY and KRAKEN_API_SECRET in your environment/.env")

    SYMBOL = "ARBUSD"

    client = KrakenClient(SYMBOL)
    # Choose a small nominal qty; it will be auto-bumped to ordermin/costmin if too small
    strategy = CandleBreakoutStrategy(client, SYMBOL, qty=Decimal("1"), buffer_pct=Decimal("0.02"))
    ctx = TradingContext(client, strategy, SYMBOL)
    ctx.run(poll_seconds=60)
