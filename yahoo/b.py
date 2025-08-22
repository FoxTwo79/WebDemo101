# backtest_premarket_915.py
# Requirements: python -m pip install pandas numpy requests pytz python-dateutil
# Optional (for Alpaca streaming): pip install websocket-client

import os, time, math, requests, datetime as dt
from dateutil import tz
import pandas as pd

# --------------------------
# CONFIG
# --------------------------
API_VENDOR = "polygon"  # "polygon" or "alpaca"
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "TtQDKmwPXtJ7A078n9cwckbHNpEO_eUe")
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "YOUR_ALPACA_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "YOUR_ALPACA_SECRET")
ALLOW_FRACTIONAL = True     # set False to floor to whole shares
BUY_TIME = "09:00:00"       # HH:MM:SS ET (pre-market allowed)
SELL_TIME = "09:14:00"      # choose last complete minute strictly before 09:15
DATE = dt.date.today()      # backtest date; set to a past trading day as needed
TIMEZONE = "America/New_York"
CASH_PER_SYMBOL = 100.0

# Symbols extracted from your screenshots
PENNY = [
    "HPEKW","THAR","ALUDW","NBY","SVREW","ATCL","RCKTW","CPOD","RCXT","GES","GLIN",
    "GLE","CCGWW","JUNS","CGTX","RAVE","FONR","PRLD","MSC","ZVYT","STEM","MBT","TLST"
]
ABOVE5 = [
    "SISI","RADX","SBEV","CLNN","MSC","AHL","INTEG","MIMI","SUPX","MEXS","SRRK","NESR","RMSG",
    "HTOOR","MDRR","NEON","KELYB","TVRD","IROH","DK","ATGL","GES"
]
SYMBOLS = sorted(set(PENNY + ABOVE5))

ET = tz.gettz(TIMEZONE)

# --------------------------
# Helpers
# --------------------------
def to_et(ts):
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=tz.UTC)
    return ts.astimezone(ET)

def parse_et(date_: dt.date, hhmmss: str) -> dt.datetime:
    h, m, s = map(int, hhmmss.split(":"))
    return dt.datetime(date_.year, date_.month, date_.day, h, m, s, tzinfo=ET)

def size_shares(cash, price, allow_fractional=True):
    if allow_fractional:
        return cash / price if price > 0 else 0.0
    return math.floor(cash / price) if price > 0 else 0

# --------------------------
# Data fetchers
# --------------------------
def fetch_polygon_1min(symbol: str, date_: dt.date) -> pd.DataFrame:
    # Aggregates 1/min for a single day including pre/post
    # https://polygon.io/docs/stocks/get_v2_aggs_ticker__stocksTicker__range__multiplier___timespan___from___to
    start = dt.datetime(date_.year, date_.month, date_.day, 4, 0, 0, tzinfo=ET)
    end   = dt.datetime(date_.year, date_.month, date_.day, 20, 0, 0, tzinfo=ET)  # 8PM post
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/min/"
        f"{int(start.timestamp()*1000)}/{int(end.timestamp()*1000)}"
        f"?adjusted=true&sort=asc&limit=50000&apiKey={POLYGON_API_KEY}"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    j = r.json()
    if j.get("results") is None:
        return pd.DataFrame()
    rows = []
    for x in j["results"]:
        ts = dt.datetime.fromtimestamp(x["t"]/1000.0, tz=tz.UTC).astimezone(ET)
        rows.append({"ts": ts, "open": x["o"], "high": x["h"], "low": x["l"], "close": x["c"], "volume": x["v"]})
    return pd.DataFrame(rows)

def fetch_alpaca_1min(symbol: str, date_: dt.date) -> pd.DataFrame:
    # https://docs.alpaca.markets/reference/stock-trades-bars
    start = dt.datetime(date_.year, date_.month, date_.day, 4, 0, 0, tzinfo=ET)
    end   = dt.datetime(date_.year, date_.month, date_.day, 20, 0, 0, tzinfo=ET)
    base = "https://data.alpaca.markets/v2/stocks/{sym}/bars"
    headers = {"APCA-API-KEY-ID": ALPACA_API_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY}
    params = {
        "timeframe": "1Min",
        "start": start.astimezone(tz.UTC).isoformat().replace("+00:00","Z"),
        "end": end.astimezone(tz.UTC).isoformat().replace("+00:00","Z"),
        "limit": 50000,
        "adjustment": "all",
        "feed": "sip",        # change to "iex" if your plan uses IEX
        "session": "all",     # include pre/post
    }
    r = requests.get(base.format(sym=symbol), headers=headers, params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    bars = j.get("bars", [])
    rows = []
    for b in bars:
        ts = dt.datetime.fromisoformat(b["t"].replace("Z","+00:00")).astimezone(ET)
        rows.append({"ts": ts, "open": b["o"], "high": b["h"], "low": b["l"], "close": b["c"], "volume": b["v"]})
    return pd.DataFrame(rows)

def fetch_bars(symbol: str, date_: dt.date) -> pd.DataFrame:
    if API_VENDOR == "polygon":
        return fetch_polygon_1min(symbol, date_)
    elif API_VENDOR == "alpaca":
        return fetch_alpaca_1min(symbol, date_)
    else:
        raise ValueError("Unknown API vendor")

# --------------------------
# Core backtest
# --------------------------
def backtest_day(symbols, date_: dt.date, buy_time="09:00:00", sell_time="09:14:00"):
    buy_ts  = parse_et(date_, buy_time)
    sell_ts = parse_et(date_, sell_time)

    reports = []
    for sym in symbols:
        df = fetch_bars(sym, date_)
        if df.empty:
            reports.append({"symbol": sym, "status": "no_data"})
            continue
        # Align to minute timestamps
        df = df.sort_values("ts").set_index("ts")

        def nearest_minute_price(ts_target):
            # exact minute bar at ts_target if exists else nearest previous bar
            if ts_target in df.index:
                return float(df.loc[ts_target, "open"])
            prior = df[df.index <= ts_target]
            if prior.empty: 
                return None
            # Use close of prior minute to simulate immediate next trade
            return float(prior.iloc[-1]["close"])

        buy_px = nearest_minute_price(buy_ts)
        sell_px = nearest_minute_price(sell_ts)

        if buy_px is None or sell_px is None:
            reports.append({"symbol": sym, "status": "no_trade"})
            continue

        shares = size_shares(CASH_PER_SYMBOL, buy_px, ALLOW_FRACTIONAL)
        pnl = (sell_px - buy_px) * shares
        ret_pct = (sell_px / buy_px - 1.0) * 100.0

        reports.append({
            "symbol": sym,
            "buy_px": round(buy_px, 4),
            "sell_px": round(sell_px, 4),
            "shares": round(shares, 6),
            "cash_alloc": CASH_PER_SYMBOL,
            "pnl": round(pnl, 2),
            "ret_pct": round(ret_pct, 3),
            "status": "filled"
        })
    return pd.DataFrame(reports).sort_values("symbol")

# --------------------------
# Live polling (run before 09:15 ET)
# --------------------------
def live_loop_until_0915(symbols, buy_time="09:00:00", poll_seconds=15):
    today = dt.date.today()
    buy_ts = parse_et(today, buy_time)
    cutoff = parse_et(today, "09:15:00")
    opened = False
    fills = {}
    print(f"Live loop… (ET now: {dt.datetime.now(tz=ET):%Y-%m-%d %H:%M:%S})")

    while dt.datetime.now(tz=ET) < cutoff:
        now_et = dt.datetime.now(tz=ET)
        for sym in symbols:
            try:
                df = fetch_bars(sym, today)
                if df.empty: 
                    continue
                df = df.sort_values("ts").set_index("ts")
                # Execute buy at/after BUY_TIME once
                if not opened and now_et >= buy_ts:
                    # use nearest minute <= now for buy
                    prior = df[df.index <= now_et]
                    if prior.empty: 
                        continue
                    buy_px = float(prior.iloc[-1]["close"])
                    shares = size_shares(CASH_PER_SYMBOL, buy_px, ALLOW_FRACTIONAL)
                    fills[sym] = {"buy_px": buy_px, "shares": shares}
                # Continuously compute mark-to-market P/L
                last = df.iloc[-1]
                if sym in fills:
                    mtm = (float(last["close"]) - fills[sym]["buy_px"]) * fills[sym]["shares"]
                    print(f"{now_et:%H:%M:%S} ET {sym}: last {float(last['close']):.4f}  P/L {mtm:+.2f}")
            except Exception as e:
                print(f"{sym} error: {e}")

        opened = True if now_et >= buy_ts else opened
        time.sleep(poll_seconds)

    # At cutoff, “sell” using last complete bar
    results = []
    df_report = backtest_day(symbols, today, buy_time=buy_time, sell_time="09:14:00")
    return df_report

if __name__ == "__main__":
    # Example: run a backtest for DATE and print summary
    report = backtest_day(SYMBOLS, DATE, BUY_TIME, SELL_TIME)
    print(report.to_string(index=False))
    print("\nTOTAL P/L: ${:.2f} | AVG %: {:.3f}%".format(report["pnl"].fillna(0).sum(),
                                                      report["ret_pct"].dropna().mean()))
