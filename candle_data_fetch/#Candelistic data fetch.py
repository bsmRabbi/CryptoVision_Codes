import pandas as pd
from binance.client import Client
from datetime import datetime, timedelta
import time
import os

# === 1. Connect to Binance API (public only, keys not needed for candles) ===
client = Client("", "")

# === 2. Input/Output paths ===
file_path = r"F:\CryptoData\Output\New Sentimen_Candle\CryptoPanic.csv"
output_file = r"F:\CryptoData\Output\New Sentimen_Candle\New folder\CryptoPanic_sentiment_filter.csv"
chunk_size = 500

# === 3. Coin mapping ===
symbol_map = {
    "Bitcoin": "BTCUSDT",
    "Ethereum": "ETHUSDT",
    "Ripple": "XRPUSDT",
    "XRP": "XRPUSDT",
    "Litecoin": "LTCUSDT",
    "Cardano": "ADAUSDT",
    "Polkadot": "DOTUSDT",
    "Binance Coin": "BNBUSDT",
    "BNB": "BNBUSDT",
    "Solana": "SOLUSDT",
    "Dogecoin": "DOGEUSDT",
    "Chainlink": "LINKUSDT",
    "Shiba Inu": "SHIBUSDT",
    "Polygon": "MATICUSDT",
    "Avalanche": "AVAXUSDT",
    "Uniswap": "UNIUSDT",
    "Terra": "LUNAUSDT",
    "VeChain": "VETUSDT",
}

# === 4. Candle cache ===
candle_cache = {}

def fetch_candles(symbol, start_date, end_date):
    """Fetch all 15-min candles for a given date range (UTC)."""
    start_str = start_date.strftime("%d %b, %Y %H:%M:%S")
    end_str = end_date.strftime("%d %b, %Y %H:%M:%S")
    
    for attempt in range(3):
        try:
            return client.get_historical_klines(
                symbol, Client.KLINE_INTERVAL_15MINUTE, start_str, end_str
            )
        except Exception as e:
            print(f"⏳ Retry {attempt+1} for {symbol} from {start_date} to {end_date} ({e})")
            time.sleep(2)
    return []

def get_ohlcv(symbol, date):
    """Return OHLCV nearest to timestamp using cache."""
    key = (symbol, date.date())
    if key not in candle_cache:
        day_start = datetime(date.year, date.month, date.day)
        day_end = day_start + timedelta(days=1)
        candle_cache[key] = fetch_candles(symbol, day_start, day_end)
        print(f"Fetched {symbol} candles for {date.date()}")

    candles = candle_cache[key]
    if not candles:
        return None, None, None, None, None

    target_ts = int(date.timestamp())
    closest = min(candles, key=lambda x: abs((int(x[0]) // 1000) - target_ts))
    o, h, l, c, v = closest[1:6]
    return float(o), float(h), float(l), float(c), float(v)

def process_row(row):
    symbol = symbol_map.get(row.get("Coin Type"))
    pub_date = row.get("Date Time")

    if not symbol or pd.isna(pub_date):
        return None, None, None, None, None, None, None, None

    o, h, l, c, v = get_ohlcv(symbol, pub_date)
    if o is None:
        return None, None, None, None, None, None, None, None

    move_oc = ((c - o) / o) * 100 if o else None
    move_hl = ((h - l) / l) * 100 if l else None
    
    # Calculate market move (Up or Down)
    market_move = "Up" if c > o else "Down" if c < o else "Flat"
    
    return o, h, l, c, v, move_oc, move_hl, market_move

# === 5. Process CSV in chunks and save results ===
first_write = not os.path.exists(output_file)

for chunk in pd.read_csv(file_path, chunksize=chunk_size):
    # Normalize column names
    chunk.columns = chunk.columns.str.strip()

    # Convert "Date Time" to datetime
    chunk["Date Time"] = pd.to_datetime(chunk["Date Time"], errors="coerce", utc=True)

    print(chunk[["Date Time", "Coin Type"]].head())  # Debug

    # Apply processing to each row and expand into new columns
    results = chunk.apply(process_row, axis=1, result_type="expand")
    chunk[["Open", "High", "Low", "Close", "Volume",
           "Movement_OpenClose_%", "Movement_HighLow_%", "Market_Move"]] = results

    chunk.dropna(subset=["Open", "High", "Low", "Close"], how="all", inplace=True)

    # Write to CSV
    chunk.to_csv(output_file, mode="a", index=False, header=first_write, encoding="utf-8")
    first_write = False
    print(f"✅ Processed {len(chunk)} rows, saved to {output_file}")