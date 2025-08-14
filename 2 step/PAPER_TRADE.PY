#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Async bulk pre-market paper trading using yahooquery.
- Single bulk API call at BUY time
- Single bulk API call at SELL time
- $100 per stock
- Logs skips and portfolio P/L
"""

import pandas as pd
from yahooquery import Ticker
from datetime import datetime, time, timezone
import pytz
import asyncio
import sys

# ---------------- CONFIG ---------------- #
INPUT_CSV = "analysis_passed.csv"
OUTPUT_CSV = "paper_trade_results.csv"

IST = pytz.timezone("Asia/Kolkata")
BUY_TIME = time(16, 43)  # IST
SELL_TIME = time(16, 45) # IST
INVESTMENT = 100         # $ per stock


# ---------------- HELPERS ---------------- #
def log(msg):
    print(f"[{datetime.now(IST).strftime('%H:%M:%S')}] {msg}")

async def wait_until(target_time: time):
    now = datetime.now(IST)
    if now.time() > target_time:
        log(f"Target time {target_time.strftime('%H:%M:%S')} passed — proceeding immediately.")
        return
    target_dt = now.replace(hour=target_time.hour, minute=target_time.minute, second=0, microsecond=0)
    log(f"Waiting until {target_time.strftime('%H:%M:%S')} IST...")
    while datetime.now(IST) < target_dt:
        await asyncio.sleep(1)
    log(f"Reached {target_time.strftime('%H:%M:%S')} IST")

def get_live_prices_bulk(tickers):
    """One bulk request, return {ticker: price} dict."""
    try:
        tkr = Ticker(tickers)
        summary = tkr.summary_detail

        if not isinstance(summary, dict):
            log(f"Unexpected response type: {type(summary).__name__} — {summary}")
            return {}

        prices = {}
        for ticker in tickers:
            pdata = summary.get(ticker)
            if not isinstance(pdata, dict):
                log(f"{ticker} — invalid or missing data: {pdata}")
                continue

            price = (
                pdata.get('preMarketPrice')
                or pdata.get('regularMarketPrice')
                or pdata.get('postMarketPrice')
                or pdata.get('previousClose')  # last resort
            )
            print(price)
            if price is not None:
                prices[ticker] = round(price, 2)
            else:
                log(f"{ticker} — price unavailable.")

        return prices

    except Exception as e:
        log(f"Error fetching prices: {e}")
        return {}


# ---------------- MAIN ---------------- #
async def main():
    # Load input
    try:
        df = pd.read_csv(INPUT_CSV)
    except FileNotFoundError:
        log(f"Input CSV {INPUT_CSV} not found.")
        sys.exit(1)

    df = df[df.get('meets_filter', False) == True].copy()
    if df.empty:
        log("No tickers meet filter criteria. Exiting.")
        return

    tickers = df['ticker'].tolist()
    results = []

    # --- BUY PHASE ---
    await wait_until(BUY_TIME)
    log(f"Fetching BUY prices for {len(tickers)} tickers...")
    buy_prices = get_live_prices_bulk(tickers)

    for ticker, price in buy_prices.items():
        shares = round(INVESTMENT / price, 4)
        results.append({
            'timestamp_utc': datetime.now(timezone.utc).isoformat(),
            'ticker': ticker,
            'buy_time_ist': datetime.now(IST).strftime("%H:%M:%S"),
            'buy_price': price,
            'shares_bought': shares,
            'investment': INVESTMENT
        })

    log(f"BUY phase complete. Bought {len(results)} stocks.")

    # --- SELL PHASE ---
    await wait_until(SELL_TIME)
    tickers_to_sell = [r['ticker'] for r in results]
    log(f"Fetching SELL prices for {len(tickers_to_sell)} tickers...")
    sell_prices = get_live_prices_bulk(tickers_to_sell)

    total_pl_usd = 0
    for trade in results:
        ticker = trade['ticker']
        if ticker in sell_prices:
            sell_price = sell_prices[ticker]
            pl_usd = round((sell_price - trade['buy_price']) * trade['shares_bought'], 2)
            pl_pct = round((sell_price - trade['buy_price']) / trade['buy_price'] * 100, 2)
            trade.update({
                'sell_time_ist': datetime.now(IST).strftime("%H:%M:%S"),
                'sell_price': sell_price,
                'pl_usd': pl_usd,
                'pl_pct': pl_pct
            })
            total_pl_usd += pl_usd
        else:
            trade.update({'sell_price': None, 'pl_usd': None, 'pl_pct': None})

    # Save
    df_results = pd.DataFrame(results)
    df_results.to_csv(OUTPUT_CSV, index=False)

    log(f"Paper trade completed. Results saved to {OUTPUT_CSV}")
    log(f"Portfolio P/L: ${round(total_pl_usd, 2)} USD")


if __name__ == "__main__":
    asyncio.run(main())



# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-

# """
# Real-time paper trading for pre-market U.S. stocks using yahooquery.
# - Buys at BUY_TIME IST
# - Sells at SELL_TIME IST
# - $100 investment per stock
# - Skips tickers with no available price instantly (no retries)
# - Generates CSV with full trade details including P/L
# """

# import pandas as pd
# from yahooquery import Ticker
# from datetime import datetime, time, timedelta
# import pytz
# import time as t

# # Input & Output files
# INPUT_CSV = "analysis_passed.csv"
# OUTPUT_CSV = "paper_trade_results.csv"

# # IST timezone
# IST = pytz.timezone("Asia/Kolkata")

# # Trade times in IST (AM for pre-market)
# BUY_TIME = time(18, 31)    # 06:19 IST
# SELL_TIME = time(18, 33)   # 06:29 IST

# INVESTMENT = 100  # $100 per stock

# # Load CSV and filter stocks that meet criteria
# df = pd.read_csv(INPUT_CSV)
# df = df[df['meets_filter'] == True].copy()
# results = []

# # ---------------- Helper Functions ---------------- #
# def wait_until(target_time):
#     now = datetime.now(IST)
#     print(f"Current IST time: {now.strftime('%H:%M:%S')}")
#     target_dt = now.replace(hour=target_time.hour, minute=target_time.minute,
#                             second=0, microsecond=0)
#     if target_dt < now:
#         print(f"Target time {target_time.strftime('%H:%M:%S')} already passed, proceeding immediately.")
#         return
#     print(f"Waiting until target time {target_time.strftime('%H:%M:%S')} IST...")
#     while datetime.now(IST) < target_dt:
#         t.sleep(1)
#     print(f"Reached target time: {datetime.now(IST).strftime('%H:%M:%S')} IST")

# def get_live_price(tkr, ticker):
#     summary = tkr.summary_detail
#     if ticker in summary:
#         price_data = summary[ticker]
#         price = price_data.get('preMarketPrice') or price_data.get('regularMarketPrice')
#         return price  # could be None
#     return None

# # ---------------- BUY PHASE ---------------- #
# print(f"Target BUY time: {BUY_TIME.strftime('%H:%M:%S')}")
# wait_until(BUY_TIME)

# tickers_to_buy = df['ticker'].tolist()
# tkr_all = Ticker(tickers_to_buy)

# print(f"Starting buy phase for {len(tickers_to_buy)} tickers...")

# for ticker in tickers_to_buy:
#     price = get_live_price(tkr_all, ticker)
#     if price is None:
#         print(f"{ticker} price not available, skipping instantly.")
#         continue
#     shares = INVESTMENT / price
#     results.append({
#         'timestamp_utc': datetime.utcnow().isoformat(),
#         'ticker': ticker,
#         'buy_time_ist': datetime.now(IST).strftime("%H:%M:%S"),
#         'buy_price': round(price, 2),
#         'shares_bought': round(shares, 4),
#         'investment': INVESTMENT
#     })

# print(f"Buy phase completed. Bought {len(results)} stocks.")

# # ---------------- SELL PHASE ---------------- #
# print(f"Target SELL time: {SELL_TIME.strftime('%H:%M:%S')}")
# wait_until(SELL_TIME)

# if results:
#     tickers_sell = [trade['ticker'] for trade in results]
#     tkr_all_sell = Ticker(tickers_sell)
#     print("Fetching sell prices...")

#     for trade in results:
#         ticker = trade['ticker']
#         price = get_live_price(tkr_all_sell, ticker)
#         if price is None:
#             trade['sell_price'] = None
#             trade['pl_usd'] = None
#             trade['pl_pct'] = None
#             continue
#         trade['sell_time_ist'] = SELL_TIME.strftime("%H:%M:%S")
#         trade['sell_price'] = round(price, 2)
#         trade['pl_usd'] = round((price - trade['buy_price']) * trade['shares_bought'], 2)
#         trade['pl_pct'] = round((price - trade['buy_price']) / trade['buy_price'] * 100, 2)

# # Save results
# df_results = pd.DataFrame(results)
# df_results.to_csv(OUTPUT_CSV, index=False)
# print(f"Paper trade completed. Results saved to {OUTPUT_CSV}")
