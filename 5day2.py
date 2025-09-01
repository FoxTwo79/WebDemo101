#!/usr/bin/env python3
"""
Short Stock Consecutive Gains Scanner with RSI, VWAP & PreMarket Flag
pip install yfinance pandas numpy
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime

def load_tickers():
    """Load stock tickers from NASDAQ & OTHER list files"""
    tickers = []
    try:
        with open("nasdaqlisted.txt", 'r') as f:
            for line in f.readlines()[1:]:
                parts = line.split('|')
                if len(parts) >= 8:
                    symbol = parts[0].strip()
                    if (symbol and symbol.isalpha() and len(symbol) <= 5 and parts[3] == 'N'):
                        tickers.append(symbol)
    except FileNotFoundError:
        pass

    try:
        with open("otherlisted.txt", 'r') as f:
            for line in f.readlines()[1:]:
                parts = line.split('|')
                if len(parts) >= 3:
                    symbol = parts[0].strip()
                    exchange = parts[2].strip()
                    if (symbol and symbol.isalpha() and len(symbol) <= 5 and exchange in ['N', 'A']):
                        tickers.append(symbol)
    except FileNotFoundError:
        pass

    return sorted(list(set(tickers)))

def compute_RSI(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    RS = gain / loss
    return 100 - (100 / (1 + RS))

def compute_VWAP(df):
    typical_price = (df['High'] + df['Low'] + df['Close']) / 3
    vwap = (typical_price * df['Volume']).cumsum() / df['Volume'].cumsum()
    return vwap

def get_premarket_flag(ticker):
    """Check if ticker is up or down in pre-market vs yesterday’s close"""
    try:
        # Intraday with pre/post
        df = yf.download(ticker, period="1d", interval="1m", prepost=True, progress=False, auto_adjust=False)
        if df.empty:
            return "None"

        df = df.tz_localize(None)

        # Pre-market cutoff: before 13:30 UTC == 9:30 ET
        premarket = df[df.index.hour < 13]
        if premarket.empty:
            return "None"

        # Yesterday’s close
        hist = yf.download(ticker, period="2d", interval="1d", progress=False, auto_adjust=False)
        if len(hist) < 2:
            return "None"
        last_close = hist['Close'].iloc[-2]

        pre_price = premarket['Close'].iloc[-1]

        if pre_price > last_close:
            return "PreMarketUp"
        elif pre_price < last_close:
            return "PreMarketDown"
        else:
            return "None"
    except Exception:
        return "None"

def scan_stock(ticker, n_days=5, threshold=6.0):
    try:
        stock = yf.Ticker(ticker)
        data = stock.history(period="2mo")
        if len(data) < n_days + 14:  # need enough for RSI
            return None

        # Add RSI & VWAP
        data['RSI'] = compute_RSI(data['Close'])
        data['VWAP'] = compute_VWAP(data)

        # Last N+1 days
        recent = data.tail(n_days + 1)
        closes = recent['Close'].values
        baseline = closes[0]

        # # Check consecutive gains
        # changes = []
        # for i in range(1, len(closes)):
        #     change = ((closes[i] - baseline) / baseline) * 100
        #     changes.append(round(change, 2))
        #     if change < threshold:
        #         return None

        changes = []
        for i in range(1, len(closes)):
            prev_close = closes[i-1]
            change = ((closes[i] - prev_close) / prev_close) * 100
            changes.append(round(change, 2))
            if change < threshold:
                return None

        premarket_flag = get_premarket_flag(ticker)

        return {
            'ticker': ticker,
            'changes': changes,
            'prices': [round(x, 2) for x in recent['Close'].values[1:]],
            'volumes': [int(v) for v in recent['Volume'].values[1:]],
            'current_price': round(recent['Close'].iloc[-1], 2),
            'current_volume': int(recent['Volume'].iloc[-1]),
            'RSI': round(recent['RSI'].iloc[-1], 2),
            'VWAP': round(recent['VWAP'].iloc[-1], 2),
            'PremarketFlag': premarket_flag
        }
    except Exception:
        return None

def main():
    N_DAYS = 3
    THRESHOLD = 5.0
    # MAX_STOCKS = 1000  # adjust as needed
    MAX_STOCKS = 7728  # adjust as needed

    print("Loading tickers...")
    tickers = load_tickers()
    print(f"Loaded {len(tickers)} tickers")

    print(f"Scanning for {N_DAYS} consecutive days with {THRESHOLD}%+ gains...")
    results = []

    for i, ticker in enumerate(tickers[:MAX_STOCKS]):
        if i % 20 == 0:
            print(f"Progress: {i}/{MAX_STOCKS}")
        result = scan_stock(ticker, N_DAYS, THRESHOLD)
        if result:
            results.append(result)
            print(f"✓ Found: {ticker}")

    if results:
        print(f"\n=== RESULTS: {len(results)} stocks found ===")

        # Headers
        headers = ['Ticker']
        for i in range(N_DAYS):
            headers.extend([f'Day-{i+1} Price', f'Day-{i+1} Change %', f'Day-{i+1} Volume'])
        headers.extend(['Current Price', 'Current Volume', 'RSI', 'VWAP', 'PremarketFlag'])

        # Build rows
        csv_data = []
        for r in results:
            row = [r['ticker']]
            for i in range(N_DAYS):
                row.append(r['prices'][i])
                row.append(f"{r['changes'][i]}%")
                row.append(r['volumes'][i])
            row.extend([r['current_price'], r['current_volume'], r['RSI'], r['VWAP'], r['PremarketFlag']])
            csv_data.append(','.join(map(str, row)))

        filename = f"stocks_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        with open(filename, 'w') as f:
            f.write(','.join(headers) + '\n')
            f.write('\n'.join(csv_data))

        print(f"Results saved to: {filename}")
        print("\nPreview:")
        print(','.join(headers))
        for row in csv_data[:5]:
            print(row)
    else:
        print("No stocks found meeting criteria")

if __name__ == "__main__":
    main()
