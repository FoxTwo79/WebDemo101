#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Real-time paper trading for pre-market U.S. stocks using yahooquery.
- Buys at BUY_TIME IST
- Sells at SELL_TIME IST
- $100 investment per stock
- Skips tickers with no available price instantly (no retries)
- Generates CSV with full trade details including P/L
"""

import pandas as pd
from yahooquery import Ticker
from datetime import datetime, time, timedelta
import pytz
import time as t

# Input & Output files
INPUT_CSV = "analysis_passed.csv"
OUTPUT_CSV = "paper_trade_results.csv"

# IST timezone
IST = pytz.timezone("Asia/Kolkata")

# Trade times in IST (AM for pre-market)
BUY_TIME = time(18, 31)    # 06:19 IST
SELL_TIME = time(18, 33)   # 06:29 IST

INVESTMENT = 100  # $100 per stock

# Load CSV and filter stocks that meet criteria
df = pd.read_csv(INPUT_CSV)
df = df[df['meets_filter'] == True].copy()
results = []

# ---------------- Helper Functions ---------------- #
def wait_until(target_time):
    now = datetime.now(IST)
    print(f"Current IST time: {now.strftime('%H:%M:%S')}")
    target_dt = now.replace(hour=target_time.hour, minute=target_time.minute,
                            second=0, microsecond=0)
    if target_dt < now:
        print(f"Target time {target_time.strftime('%H:%M:%S')} already passed, proceeding immediately.")
        return
    print(f"Waiting until target time {target_time.strftime('%H:%M:%S')} IST...")
    while datetime.now(IST) < target_dt:
        t.sleep(1)
    print(f"Reached target time: {datetime.now(IST).strftime('%H:%M:%S')} IST")

def get_live_price(tkr, ticker):
    summary = tkr.summary_detail
    if ticker in summary:
        price_data = summary[ticker]
        price = price_data.get('preMarketPrice') or price_data.get('regularMarketPrice')
        return price  # could be None
    return None

# ---------------- BUY PHASE ---------------- #
print(f"Target BUY time: {BUY_TIME.strftime('%H:%M:%S')}")
wait_until(BUY_TIME)

tickers_to_buy = df['ticker'].tolist()
tkr_all = Ticker(tickers_to_buy)

print(f"Starting buy phase for {len(tickers_to_buy)} tickers...")

for ticker in tickers_to_buy:
    price = get_live_price(tkr_all, ticker)
    if price is None:
        print(f"{ticker} price not available, skipping instantly.")
        continue
    shares = INVESTMENT / price
    results.append({
        'timestamp_utc': datetime.utcnow().isoformat(),
        'ticker': ticker,
        'buy_time_ist': datetime.now(IST).strftime("%H:%M:%S"),
        'buy_price': round(price, 2),
        'shares_bought': round(shares, 4),
        'investment': INVESTMENT
    })

print(f"Buy phase completed. Bought {len(results)} stocks.")

# ---------------- SELL PHASE ---------------- #
print(f"Target SELL time: {SELL_TIME.strftime('%H:%M:%S')}")
wait_until(SELL_TIME)

if results:
    tickers_sell = [trade['ticker'] for trade in results]
    tkr_all_sell = Ticker(tickers_sell)
    print("Fetching sell prices...")

    for trade in results:
        ticker = trade['ticker']
        price = get_live_price(tkr_all_sell, ticker)
        if price is None:
            trade['sell_price'] = None
            trade['pl_usd'] = None
            trade['pl_pct'] = None
            continue
        trade['sell_time_ist'] = SELL_TIME.strftime("%H:%M:%S")
        trade['sell_price'] = round(price, 2)
        trade['pl_usd'] = round((price - trade['buy_price']) * trade['shares_bought'], 2)
        trade['pl_pct'] = round((price - trade['buy_price']) / trade['buy_price'] * 100, 2)

# Save results
df_results = pd.DataFrame(results)
df_results.to_csv(OUTPUT_CSV, index=False)
print(f"Paper trade completed. Results saved to {OUTPUT_CSV}")
