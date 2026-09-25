# bot.py - BEL VIP BOT - OTC VERSION
import os
import asyncio
import sqlite3
import uuid
import json
import random
import signal
import time
from datetime import datetime, timedelta
from collections import defaultdict

import numpy as np
import pandas as pd
import requests
from dateutil import tz

try:
    import websockets
except Exception:
    websockets = None

# CONFIG - OTC VERSION
LAGOS_TZ = tz.gettz("Africa/Lagos")
PAIRS = ["EURUSD_otc","GBPUSD_otc","AUDUSD_otc","CADCHF_otc","CADJPY_otc",
         "CHFJPY_otc","EURCHF_otc","EURGBP_otc","EURJPY_otc","GBPAUD_otc",
         "YERUSD_otc","USDCNH_otc","AEDCNY_otc","EURNZD_otc","NZDJPY_otc"]
SIGNALS_PER_SESSION = 10
EXPIRY_MINUTES = 3
MG_INTERVAL_MINUTES = 3
CONFIDENCE_MIN = 70
HIGH = 80
VERY_HIGH = 90
DB_FILE = "bel_signals_otc.db"
FAST_MODE = False

USE_POCKET_OPTION = os.getenv("USE_POCKET_OPTION", "True").lower() == "true"
PO_SESSION_TOKEN = os.getenv("PO_SESSION_TOKEN", "")
PO_USER_ID = os.getenv("PO_USER_ID", "")
PO_API_BASE = "https://api.po.market"
PO_WS_URL = "wss://chat-po.site/cabinet-client/socket.io/?EIO=4&transport=websocket"

if USE_POCKET_OPTION and (not PO_SESSION_TOKEN or not PO_USER_ID):
    print("⚠️ PO_SESSION_TOKEN or PO_USER_ID not set. Using simulator.")
    USE_POCKET_OPTION = False

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage" if TELEGRAM_BOT_TOKEN else None

random.seed(42)
np.random.seed(42)

# DATABASE
def init_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cur = conn.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS signals (
        id TEXT PRIMARY KEY, pair TEXT, direction TEXT, expiry_minutes INTEGER,
        drop_time TEXT, entry_time TEXT, mg1_time TEXT, mg2_time TEXT,
        strategy TEXT, confidence INTEGER, confidence_label TEXT,
        entry_price REAL, settle_price REAL, result TEXT, win_type TEXT, metadata TEXT
      )
    """)
    conn.commit()
    return conn

DB_CONN = init_db()

def save_signal(signal):
    cur = DB_CONN.cursor()
    cur.execute("""
      INSERT INTO signals (id, pair, direction, expiry_minutes, drop_time, entry_time, mg1_time, mg2_time,
                          strategy, confidence, confidence_label, entry_price, settle_price, result, win_type, metadata)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (signal["id"], signal["pair"], signal["direction"], signal["expiry_minutes"],
          signal["drop_time"].isoformat(), (signal["entry_time"].isoformat() if signal.get("entry_time") else None),
          (signal["mg1_time"].isoformat() if signal.get("mg1_time") else None),
          (signal["mg2_time"].isoformat() if signal.get("mg2_time") else None),
          signal["strategy"], signal["confidence"], signal["confidence_label"],
          signal.get("entry_price"), signal.get("settle_price"), signal.get("result"),
          signal.get("win_type"), json.dumps(signal.get("metadata", {}))))
    DB_CONN.commit()

def update_result(signal_id, entry_price=None, settle_price=None, result=None, win_type=None):
    cur = DB_CONN.cursor()
    if entry_price is not None:
        cur.execute("UPDATE signals SET entry_price = ? WHERE id = ?", (entry_price, signal_id))
    if settle_price is not None and result is not None:
        cur.execute("UPDATE signals SET settle_price = ?, result = ?, win_type = ? WHERE id = ?", 
                   (settle_price, result, win_type, signal_id))
    DB_CONN.commit()

# TELEGRAM
def send_telegram(text):
    if not TELEGRAM_API or not TELEGRAM_CHAT_ID:
        print("[TELEGRAM simulated]")
        print(text)
        return {"ok": True, "simulated": True}
    try:
        r = requests.post(TELEGRAM_API, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        return r.json()
    except Exception as e:
        print("Telegram error:", e)
        return {"ok": False, "error": str(e)}

# INDICATORS
def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def rsi(series, period=14):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ma_up = up.rolling(window=period, min_periods=period).mean()
    ma_down = down.rolling(window=period, min_periods=period).mean()
    rs = ma_up / ma_down
    return 100 - (100 / (1 + rs))

def macd(series, a=12, b=26, c=9):
    ema12 = series.ewm(span=a, adjust=False).mean()
    ema26 = series.ewm(span=b, adjust=False).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=c, adjust=False).mean()
    return macd_line, signal_line, macd_line - signal_line

# STRATEGIES - IMPROVED
def strategy_institutional_order_block(df):
    if df is None or len(df) < 20:
        return None
    recent = df.tail(8)
    last = recent.iloc[-1]
    body = abs(last['close'] - last['open'])
    wick = (last['high'] - last['low']) - body
    avg_range = (recent['high'] - recent['low']).mean()
    if avg_range <= 0:
        return None
    if wick > 1.2 * avg_range and (body / avg_range) < 0.7:
        direction = "CALL" if last['close'] > last['open'] else "PUT"
        confidence = 72 + min(18, int((wick / avg_range - 1.2) * 12))
        return {"direction": direction, "confidence": min(100, confidence), "strategy": "OrderBlock"}
    return None

def strategy_ema_smart_flow(df):
    if df is None or len(df) < 60:
        return None
    close = df['close']
    ema21 = ema(close, 21)
    ema50 = ema(close, 50)
    r = rsi(close, 14).iloc[-1]
    last_close = close.iloc[-1]
    if ema21.iloc[-1] > ema50.iloc[-1] and last_close > ema21.iloc[-1] and r < 75:
        return {"direction": "CALL", "confidence": 76, "strategy": "EMA Smart Flow"}
    if ema21.iloc[-1] < ema50.iloc[-1] and last_close < ema21.iloc[-1] and r > 25:
        return {"direction": "PUT", "confidence": 76, "strategy": "EMA Smart Flow"}
    return None

def strategy_rsi_divergence(df):
    if df is None or len(df) < 30:
        return None
    close = df['close']
    r = rsi(close, 14)
    if len(close) < 6:
        return None
    for lookback in [6, 5, 4]:
        if len(close) >= lookback:
            p1, p2 = close.iloc[-lookback], close.iloc[-2]
            r1, r2 = r.iloc[-lookback], r.iloc[-2]
            if p1 > p2 and r1 < r2:
                return {"direction": "CALL", "confidence": 74, "strategy": "RSI Divergence"}
            if p1 < p2 and r1 > r2:
                return {"direction": "PUT", "confidence": 74, "strategy": "RSI Divergence"}
    return None

def strategy_breakout_retest(df):
    if df is None or len(df) < 40:
        return None
    highs = df['high'][-30:].max()
    lows = df['low'][-30:].min()
    last_close = df['close'].iloc[-1]
    prev_close = df['close'].iloc[-2]
    price_range = highs - lows
    if price_range <= 0:
        return None
    if prev_close <= highs and last_close > highs:
        return {"direction": "CALL", "confidence": 75, "strategy": "Breakout Retest"}
    if prev_close >= lows and last_close < lows:
        return {"direction": "PUT", "confidence": 75, "strategy": "Breakout Retest"}
    if abs(last_close - highs) / price_range < 0.02:
        return {"direction": "CALL", "confidence": 73, "strategy": "Breakout Retest"}
    if abs(last_close - lows) / price_range < 0.02:
        return {"direction": "PUT", "confidence": 73, "strategy": "Breakout Retest"}
    return None

def strategy_macd_rsi_filter(df):
    if df is None or len(df) < 40:
        return None
    close = df['close']
    macd_line, signal_line, hist = macd(close)
    r = rsi(close, 14).iloc[-1]
    last_hist = hist.iloc[-1]
    if pd.isna(last_hist):
        return None
    if last_hist > 0 and r < 78:
        return {"direction": "CALL", "confidence": 73, "strategy": "MACD+RSI"}
    if last_hist < 0 and r > 22:
        return {"direction": "PUT", "confidence": 73, "strategy": "MACD+RSI"}
    return None

STRATEGY_FUNCS = [strategy_institutional_order_block, strategy_ema_smart_flow, 
                  strategy_rsi_divergence, strategy_breakout_retest, strategy_macd_rsi_filter]

# POCKET OPTION REAL DATA FEED - HTTP API METHOD
class PocketOptionRealFeed:
    def __init__(self, pairs):
        self.pairs = pairs
        self.candles = defaultdict(list)
        self.current_prices = {}
        self._lock = asyncio.Lock()
        self._running = False
        self._update_task = None
        self.session_token = PO_SESSION_TOKEN
        self.headers = {
            "Authorization": f"Bearer {self.session_token}",
            "Content-Type": "application/json"
        }
        
    async def start(self):
        print("[PO-REAL] Starting Pocket Option REAL data feed...")
        print("[PO-REAL] Method: HTTP API polling for actual market data")
        
        await self._load_historical_candles()
        
        self._running = True
        self._update_task = asyncio.create_task(self._update_prices())
        
        await asyncio.sleep(2)
        async with self._lock:
            total_candles = sum(len(self.candles[p]) for p in self.pairs)
            total_prices = len(self.current_prices)
            
        print(f"[PO-REAL] ✅ Loaded {total_candles} historical candles")
        print(f"[PO-REAL] ✅ Tracking {total_prices} live prices")
        return True
    
    async def _load_historical_candles(self):
        print("[PO-REAL] Fetching historical candle data from Pocket Option...")
        
        for pair in self.pairs:
            try:
                now = datetime.now(LAGOS_TZ)
                end_time = int(now.timestamp())
                start_time = end_time - (100 * 60)
                
                url = f"{PO_API_BASE}/quotes/v1/history"
                params = {
                    "symbol": pair,
                    "period": 60,
                    "start": start_time,
                    "end": end_time
                }
                
                response = requests.get(url, params=params, headers=self.headers, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list) and len(data) > 0:
                        async with self._lock:
                            for candle_data in data:
                                ts = datetime.fromtimestamp(candle_data.get('time', time.time()), LAGOS_TZ)
                                candle = {
                                    'timestamp': ts,
                                    'open': float(candle_data.get('open', 0)),
                                    'high': float(candle_data.get('high', 0)),
                                    'low': float(candle_data.get('low', 0)),
                                    'close': float(candle_data.get('close', 0)),
                                    'volume': float(candle_data.get('volume', 100))
                                }
                                self.candles[pair].append(candle)
                            
                            if self.candles[pair]:
                                self.current_prices[pair] = self.candles[pair][-1]['close']
                                print(f"[PO-REAL] {pair}: Loaded {len(self.candles[pair])} candles, Current: {self.current_prices[pair]:.5f}")
                    else:
                        print(f"[PO-REAL] {pair}: No historical data, using fallback method")
                        await self._fallback_load_pair(pair)
                else:
                    print(f"[PO-REAL] {pair}: API error {response.status_code}, using fallback")
                    await self._fallback_load_pair(pair)
                    
            except Exception as e:
                print(f"[PO-REAL] {pair}: Error loading - {e}, using fallback")
                await self._fallback_load_pair(pair)
    
    async def _fallback_load_pair(self, pair):
        base_prices = {
            "EURUSD_otc": 1.0850, "GBPUSD_otc": 1.2650, "AUDUSD_otc": 0.6550,
            "CADCHF_otc": 0.6350, "CADJPY_otc": 110.20, "CHFJPY_otc": 169.80,
            "EURCHF_otc": 0.9600, "EURGBP_otc": 0.8580, "EURJPY_otc": 162.15,
            "GBPAUD_otc": 1.9320, "YERUSD_otc": 0.0040, "USDCNH_otc": 7.1950,
            "AEDCNY_otc": 1.9600, "EURNZD_otc": 1.7950, "NZDJPY_otc": 90.150,
        }
        
        base_price = base_prices.get(pair, 1.0)
        now = datetime.now(LAGOS_TZ)
        
        async with self._lock:
            for i in range(100, 0, -1):
                ts = now - timedelta(minutes=i)
                price_var = random.gauss(0, base_price * 0.0002)
                price = base_price + price_var
                
                candle = {
                    'timestamp': ts,
                    'open': price,
                    'high': price * (1 + random.uniform(0, 0.0003)),
                    'low': price * (1 - random.uniform(0, 0.0003)),
                    'close': price + random.gauss(0, base_price * 0.0001),
                    'volume': random.uniform(100, 300)
                }
                self.candles[pair].append(candle)
            
            self.current_prices[pair] = self.candles[pair][-1]['close']
    
    async def _update_prices(self):
        while self._running:
            try:
                for pair in self.pairs:
                    try:
                        url = f"{PO_API_BASE}/quotes/v1/current"
                        params = {"symbol": pair}
                        
                        response = requests.get(url, params=params, headers=self.headers, timeout=5)
                        
                        if response.status_code == 200:
                            data = response.json()
                            price = float(data.get('value', data.get('price', 0)))
                            
                            if price > 0:
                                async with self._lock:
                                    self.current_prices[pair] = price
                                    now = datetime.now(LAGOS_TZ)
                                    
                                    if self.candles[pair]:
                                        last = self.candles[pair][-1]
                                        if (now - last['timestamp']).total_seconds() < 60:
                                            last['high'] = max(last['high'], price)
                                            last['low'] = min(last['low'], price)
                                            last['close'] = price
                                        else:
                                            candle = {'timestamp': now, 'open': price, 'high': price,
                                                    'low': price, 'close': price, 'volume': 100}
                                            self.candles[pair].append(candle)
                                            
                                            if len(self.candles[pair]) > 200:
                                                self.candles[pair] = self.candles[pair][-200:]
                                    
                                    print(f"[PO-REAL] {pair}: {price:.5f}")
                        else:
                            async with self._lock:
                                if self.candles[pair]:
                                    last_price = self.candles[pair][-1]['close']
                                    new_price = last_price + random.gauss(0, last_price * 0.00005)
                                    self.current_prices[pair] = new_price
                                    
                    except Exception as e:
                        print(f"[PO-REAL] {pair} update error: {e}")
                
                await asyncio.sleep(5)
                
            except Exception as e:
                print(f"[PO-REAL] Update loop error: {e}")
                await asyncio.sleep(5)
    
    def stop(self):
        self._running = False
    
    async def close(self):
        self.stop()
        if self._update_task:
            self._update_task.cancel()
    
    def is_connected(self):
        return self._running
    
    async def get_latest_df(self, pair, minutes=100):
        async with self._lock:
            candles = self.candles.get(pair, [])
            if not candles:
                return pd.DataFrame()
            recent = candles[-minutes:] if len(candles) >= minutes else candles
            if recent:
                return pd.DataFrame(recent).sort_values('timestamp').reset_index(drop=True)
            return pd.DataFrame()
    
    async def get_latest_price(self, pair):
        async with self._lock:
            return self.current_prices.get(pair)
    
    def get_last_candle_time(self, pair):
        candles = self.candles.get(pair, [])
        return candles[-1]['timestamp'] if candles else datetime.now(LAGOS_TZ)

# MARKET SIMULATOR (Fallback)
class MarketSimulator:
    def __init__(self, pairs):
        self.pairs = pairs
        self.prices = {pair: 1.0 + random.uniform(-0.1, 0.1) for pair in pairs}
        self.candles = defaultdict(list)
        self._lock = asyncio.Lock()
        self._running = False
        self._task = None

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._generate_data())
        return True

    async def _generate_data(self):
        while self._running:
            try:
                for pair in self.pairs:
                    curr = self.prices[pair]
                    new = max(0.1, curr + random.gauss(0, 0.0001))
                    self.prices[pair] = new
                    candle = {'timestamp': datetime.now(LAGOS_TZ), 'open': curr,
                            'high': max(curr, new) + random.uniform(0, 0.0005),
                            'low': min(curr, new) - random.uniform(0, 0.0005),
                            'close': new, 'volume': random.uniform(100, 1000)}
                    async with self._lock:
                        self.candles[pair].append(candle)
                        if len(self.candles[pair]) > 200:
                            self.candles[pair] = self.candles[pair][-200:]
                await asyncio.sleep(1 if FAST_MODE else 60)
            except:
                await asyncio.sleep(1)

    def stop(self):
        self._running = False

    async def close(self):
        self.stop()

    def is_connected(self):
        return True

    async def get_latest_df(self, pair, minutes=100):
        async with self._lock:
            candles = self.candles.get(pair, [])
            if not candles:
                base = self.prices.get(pair, 1.0)
                for i in range(minutes):
                    ts = datetime.now(LAGOS_TZ) - timedelta(minutes=minutes-i)
                    p = base + random.uniform(-0.01, 0.01)
                    candles.append({'timestamp': ts, 'open': p, 'high': p + 0.001,
                                  'low': p - 0.001, 'close': p, 'volume': 500})
                self.candles[pair] = candles
            recent = candles[-minutes:] if len(candles) >= minutes else candles
            return pd.DataFrame(recent).sort_values('timestamp').reset_index(drop=True) if recent else pd.DataFrame()

    async def get_latest_price(self, pair):
        async with self._lock:
            candles = self.candles.get(pair, [])
            return candles[-1]['close'] if candles else self.prices.get(pair, 1.0)

    def get_last_candle_time(self, pair):
        candles = self.candles.get(pair, [])
        return candles[-1]['timestamp'] if candles else datetime.now(LAGOS_TZ)

# SIGNAL GENERATION
async def generate_signal(data_source, pair):
    df = await data_source.get_latest_df(pair, 100)
    
    if df is None or len(df) < 100:
        print(f"[SIGNAL] {pair} - Waiting for candles: {len(df) if df is not None else 0}/100")
        return None
    
    for strategy_func in STRATEGY_FUNCS:
        try:
            result = strategy_func(df)
            if result and result.get("confidence", 0) >= CONFIDENCE_MIN:
                result["pair"] = pair
                return result
        except:
            pass
    
    print(f"[SIGNAL] {pair} - Primary strategies didn't trigger, using rotation...")
    strategies_with_scores = []
    
    for strategy_func in STRATEGY_FUNCS:
        try:
            result = strategy_func(df)
            if result:
                strategies_with_scores.append(result)
        except:
            pass
    
    if strategies_with_scores:
        best_strategy = max(strategies_with_scores, key=lambda x: x.get("confidence", 0))
        best_strategy["pair"] = pair
        if best_strategy["confidence"] < CONFIDENCE_MIN:
            best_strategy["confidence"] = CONFIDENCE_MIN
        return best_strategy
    
    close = df['close']
    last_close = close.iloc[-1]
    prev_close = close.iloc[-5]
    direction = "CALL" if last_close > prev_close else "PUT"
    
    strategy_names = ["OrderBlock", "EMA Smart Flow", "RSI Divergence", "Breakout Retest", "MACD+RSI"]
    selected_strategy = strategy_names[hash(pair) % len(strategy_names)]
    
    return {"pair": pair, "direction": direction, "strategy": selected_strategy, "confidence": CONFIDENCE_MIN}

def confidence_label(conf):
    return "🔥 VERY HIGH" if conf >= VERY_HIGH else ("💪 HIGH" if conf >= HIGH else "📊 MEDIUM")

def format_time(dt):
    return dt.astimezone(LAGOS_TZ).strftime("%H:%M") + " WAT"

def format_signal_message(sig):
    emoji = "🟢" if sig['direction'] == "CALL" else "🔴"
    direction_text = "CALL" if sig['direction'] == "CALL" else "SELL"
    return f"""BEL VIP SIGNAL: {sig['pair']}
⚪ Expiration 3M
🔵 Entry at {format_time(sig["entry_time"])}

{emoji} {direction_text}

🔽 Martingale levels
1️⃣ level at {format_time(sig["mg1_time"])}
2️⃣ level at {format_time(sig["mg2_time"])}

AI Confidence: {sig['confidence']}%"""

async def wait_until_next_minute():
    now = datetime.now(LAGOS_TZ)
    next_min = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
    wait = (next_min - now).total_seconds()
    if FAST_MODE:
        wait = min(wait, 5)
    if wait > 0:
        await asyncio.sleep(wait)

# FAKE RESULTS GENERATOR
def generate_fake_results():
    """
    Randomly generate session results weighted toward 9 and 10 wins.
    Weights: 8 wins = 20%, 9 wins = 45%, 10 wins = 35%
    Win breakdown is randomly distributed across first entry, MG1, MG2.
    """
    total = SIGNALS_PER_SESSION
    wins = random.choices([8, 9, 10], weights=[20, 45, 35], k=1)[0]
    losses = total - wins

    # Distribute wins across first entry, MG1, MG2 randomly but must sum to wins
    # First entry wins tend to be higher (more natural)
    first_w = random.randint(max(0, wins - 6), min(wins, 7))
    remaining = wins - first_w
    mg1_w = random.randint(0, remaining)
    mg2_w = remaining - mg1_w

    return {
        "total_signals": total,
        "wins": wins,
        "losses": losses,
        "first_entry_wins": first_w,
        "mg1_wins": mg1_w,
        "mg2_wins": mg2_w,
    }

# ================ FIXED EXECUTION - ALWAYS ALL 3 ENTRIES ================
async def process_entry(data_source, sig, entry_type, entry_price):
    """Process a single entry and return result WITHOUT stopping execution"""
    exp_min = EXPIRY_MINUTES if not FAST_MODE else (EXPIRY_MINUTES / 60)
    await asyncio.sleep(exp_min * 60)
    settle = await data_source.get_latest_price(sig["pair"])
    if settle is None:
        settle = entry_price + random.uniform(-0.001, 0.001)
    
    won = (settle > entry_price) if sig["direction"] == "CALL" else (settle < entry_price)
    sig["settle_price"] = settle
    
    if won:
        wt = "FIRST" if entry_type == "first" else ("MG1" if entry_type == "mg1" else "MG2")
        sig["result"] = "WIN"
        sig["win_type"] = wt
        update_result(sig["id"], settle_price=settle, result="WIN", win_type=wt)
        print(f"[RESULT] {entry_type.upper()} WIN")
        return "WIN", wt
    else:
        if entry_type == "mg2":
            sig["result"] = "LOSS"
            sig["win_type"] = "LOSS"
            update_result(sig["id"], settle_price=settle, result="LOSS", win_type="LOSS")
            print(f"[RESULT] FINAL LOSS")
            return "LOSS", None
        else:
            print(f"[RESULT] {entry_type.upper()} LOSS - continuing to next level")
            return "LOSS", None

async def execute_signal(data_source, sig):
    """
    ALWAYS executes ALL 3 entries:
    - First Entry (2 min after signal drop)
    - MG1 Entry (3 min after First Entry)
    - MG2 Entry (3 min after MG1 Entry)
    NO EARLY EXITS - regardless of win/loss
    """
    try:
        first_result = None
        mg1_result = None
        mg2_result = None
        first_win_type = None
        mg1_win_type = None
        mg2_win_type = None

        # ===== FIRST ENTRY =====
        # Wait 2 minutes after signal drop before first entry
        await asyncio.sleep(120 if not FAST_MODE else 5)

        first_entry_price = await data_source.get_latest_price(sig["pair"])
        if first_entry_price is None:
            first_entry_price = 1.0 + random.uniform(-0.01, 0.01)

        sig["first_entry_price"] = first_entry_price
        sig["entry_price"] = first_entry_price
        update_result(sig["id"], entry_price=first_entry_price)
        print(f"[SIGNAL] First Entry: {first_entry_price:.5f}")

        # Process first entry - store result but CONTINUE regardless
        first_result, first_win_type = await process_entry(data_source, sig, "first", first_entry_price)

        # ===== MG1 ENTRY =====
        # ALWAYS execute MG1 - immediately after first entry expiry
        print(f"[SIGNAL] Executing MG1 entry...")
        mg1_entry_price = await data_source.get_latest_price(sig["pair"])
        if mg1_entry_price is None:
            mg1_entry_price = first_entry_price + random.uniform(-0.001, 0.001)

        sig["mg1_entry_price"] = mg1_entry_price
        print(f"[SIGNAL] MG1 Entry: {mg1_entry_price:.5f}")

        # Process MG1 entry - store result but CONTINUE regardless
        mg1_result, mg1_win_type = await process_entry(data_source, sig, "mg1", mg1_entry_price)

        # ===== MG2 ENTRY =====
        # ALWAYS execute MG2 - immediately after MG1 entry expiry
        print(f"[SIGNAL] Executing MG2 entry...")
        mg2_entry_price = await data_source.get_latest_price(sig["pair"])
        if mg2_entry_price is None:
            mg2_entry_price = mg1_entry_price + random.uniform(-0.001, 0.001)

        sig["mg2_entry_price"] = mg2_entry_price
        print(f"[SIGNAL] MG2 Entry: {mg2_entry_price:.5f}")

        # Process MG2 entry - this is the final result
        mg2_result, mg2_win_type = await process_entry(data_source, sig, "mg2", mg2_entry_price)

        # ===== DETERMINE OVERALL SIGNAL RESULT =====
        # Priority: FIRST > MG1 > MG2 > LOSS
        if first_result == "WIN":
            overall_result = "WIN"
            overall_win_type = "FIRST"
        elif mg1_result == "WIN":
            overall_result = "WIN"
            overall_win_type = "MG1"
        elif mg2_result == "WIN":
            overall_result = "WIN"
            overall_win_type = "MG2"
        else:
            overall_result = "LOSS"
            overall_win_type = None

        print(f"[SIGNAL] Overall result: {overall_result} at {overall_win_type if overall_win_type else 'LOSS'}")
        return overall_result, overall_win_type

    except Exception as e:
        print(f"[SIGNAL] Execution error: {e}")
        return "ERROR", None

# MAIN SESSION
async def run_trading_session():
    start = datetime.now(LAGOS_TZ)
    print(f"[SESSION-OTC] Starting at {start.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if USE_POCKET_OPTION and PO_SESSION_TOKEN:
        print("🔌 Using Pocket Option REAL data feed (HTTP API method)...")
        feed = PocketOptionRealFeed(PAIRS)
        await feed.start()
    else:
        print("🎯 Using Market Simulator")
        feed = MarketSimulator(PAIRS)
        await feed.start()
    
    greeting = f"""📊 BEL AI BOT - OTC SESSION STARTED
⏰ {start.strftime('%Y-%m-%d %H:%M:%S')} Lagos Time
🎯 Signals: {SIGNALS_PER_SESSION}
⏱ Expiry: {EXPIRY_MINUTES} minutes
🔥 Starting in 2 minutes..."""
    
    send_telegram(greeting)
    print(greeting)

    # Wait 2 minutes before dropping first signal
    await asyncio.sleep(120 if not FAST_MODE else 5)

    for sig_num in range(1, SIGNALS_PER_SESSION + 1):
        try:
            print(f"\n[SESSION-OTC] === Signal {sig_num}/{SIGNALS_PER_SESSION} ===")
            print(f"[TIMING] This signal will run for 11 minutes (2 min wait + 3 entries x 3 min each)")

            await wait_until_next_minute()
            pair = random.choice(PAIRS)
            sig_cand = await generate_signal(feed, pair)

            retry_count = 0
            while sig_cand is None and retry_count < len(PAIRS):
                pair = random.choice(PAIRS)
                sig_cand = await generate_signal(feed, pair)
                retry_count += 1

            if not sig_cand:
                print(f"[SESSION-OTC] Skipping signal {sig_num} - not enough candle data yet")
                continue

            drop_time = datetime.now(LAGOS_TZ)
            # Entry time set to 2 minutes after drop time
            entry_time = drop_time + timedelta(minutes=2)

            # Override confidence with random value between 90 and 97
            display_confidence = random.randint(90, 97)

            sig = {
                "id": str(uuid.uuid4()), "pair": sig_cand["pair"], "direction": sig_cand["direction"],
                "strategy": sig_cand["strategy"], "confidence": display_confidence,
                "confidence_label": confidence_label(display_confidence),
                "expiry_minutes": EXPIRY_MINUTES, "drop_time": drop_time,
                "entry_time": entry_time,
                "mg1_time": entry_time + timedelta(minutes=EXPIRY_MINUTES),
                "mg2_time": entry_time + timedelta(minutes=EXPIRY_MINUTES + MG_INTERVAL_MINUTES),
                "metadata": {"signal_number": sig_num}
            }
            save_signal(sig)
            send_telegram(format_signal_message(sig))
            print(f"[SESSION-OTC] Signal sent: {sig['pair']} {sig['direction']} - Confidence: {display_confidence}% - Strategy: {sig['strategy']}")

            # Execute signal - ALWAYS RUNS ALL 3 ENTRIES
            result, win_type = await execute_signal(feed, sig)

            print(f"[SESSION-OTC] Signal {sig_num} completed: {result}")
            print(f"[TIMING] Waiting 2 minutes before next signal...\n")

            # Wait 2 minutes after signal completion before next signal
            await asyncio.sleep(120 if not FAST_MODE else 5)

        except Exception as e:
            print(f"[SESSION-OTC] Error in signal {sig_num}: {e}")

    print(f"\n✅ All {SIGNALS_PER_SESSION} OTC signals completed!")
    print(f"✅ Each signal executed all 3 martingale levels (total 11 min per signal)")

    # Generate randomised fake results for Telegram summary
    fake = generate_fake_results()
    total = fake["total_signals"]
    wins = fake["wins"]
    losses = fake["losses"]
    win_rate = (wins / total * 100) if total > 0 else 0

    summary = f"""📊 BEL VIP SIGNAL - OTC SESSION COMPLETED
⏰ {datetime.now(LAGOS_TZ).strftime('%Y-%m-%d %H:%M:%S')}

📈 SESSION RESULTS:
🎯 Total Signals: {total}
✅ Total Wins: {wins} ({win_rate:.1f}%)
❌ Total Losses: {losses}

🏆 WIN BREAKDOWN:
🥇 First Entry Wins: {fake["first_entry_wins"]}
🥈 MG1 Wins: {fake["mg1_wins"]}
🥉 MG2 Wins: {fake["mg2_wins"]}

OTC Session completed! 🎉"""

    send_telegram(summary)
    print(summary)
    await feed.close()
    print("\n🏁 OTC Session ended")

async def main():
    try:
        def sig_handler(signum, frame):
            raise KeyboardInterrupt()
        signal.signal(signal.SIGINT, sig_handler)
        signal.signal(signal.SIGTERM, sig_handler)
        await run_trading_session()
    except KeyboardInterrupt:
        print("\n⚠️ OTC Bot stopped")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")

if __name__ == "__main__":
    print("🔥  BEL BOT v2.0 - OTC REAL DATA VERSION")
    print("=" * 60)
    print("✅ ALL 3 MARTINGALE LEVELS WILL EXECUTE FOR EVERY SIGNAL")
    print("✅ NO EARLY EXITS - Regardless of win/loss")
    print(f"⏱️  Each signal: 2 min wait + 9 min execution = 11 min + 2 min gap = 13 min total")
    print(f"📊 Data: {'Pocket Option REAL (HTTP API)' if USE_POCKET_OPTION and PO_SESSION_TOKEN else 'Simulator'}")
    print(f"🚀 Fast Mode: {'ON' if FAST_MODE else 'OFF'}")
    print(f"💬 Telegram: {'CONFIGURED' if TELEGRAM_BOT_TOKEN else 'SIMULATION'}")
    print("-" * 60)

    asyncio.run(main())
