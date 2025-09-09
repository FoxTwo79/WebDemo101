# #!/usr/bin/env python3
# import yfinance as yf
# import pandas as pd
# import numpy as np
# from datetime import datetime
# import concurrent.futures
# import threading

# print_lock = threading.Lock()

# def load_tickers():
#     tickers = []
#     try:
#         with open("nasdaqlisted.txt", 'r') as f:
#             for line in f.readlines()[1:]:
#                 parts = line.split('|')
#                 if len(parts) >= 8:
#                     symbol = parts[0].strip()
#                     if symbol.isalpha() and len(symbol) <= 5 and parts[3] == 'N':
#                         tickers.append(symbol)
#     except FileNotFoundError:
#         pass
#     try:
#         with open("otherlisted.txt", 'r') as f:
#             for line in f.readlines()[1:]:
#                 parts = line.split('|')
#                 if len(parts) >= 3:
#                     symbol = parts[0].strip()
#                     exchange = parts[2].strip()
#                     if symbol.isalpha() and len(symbol) <= 5 and exchange in ['N', 'A']:
#                         tickers.append(symbol)
#     except FileNotFoundError:
#         pass
#     return sorted(list(set(tickers)))

# def compute_RSI(series, period=14):
#     delta = series.diff()
#     gain = delta.where(delta > 0, 0).rolling(window=period).mean()
#     loss = -delta.where(delta < 0, 0).rolling(window=period).mean()
#     RS = gain / loss
#     return 100 - (100 / (1 + RS))

# def compute_VWAP(df):
#     typical_price = (df['High'] + df['Low'] + df['Close']) / 3
#     vwap = (typical_price * df['Volume']).cumsum() / df['Volume'].cumsum()
#     return vwap

# def get_premarket_flag(ticker):
#     try:
#         df = yf.download(ticker, period="1d", interval="1m", prepost=True, progress=False, auto_adjust=False)
#         if df.empty:
#             return "None"
#         df = df.tz_localize(None)
#         premarket = df[df.index.hour < 13]
#         if premarket.empty:
#             return "None"
#         hist = yf.download(ticker, period="2d", interval="1d", progress=False, auto_adjust=False)
#         if len(hist) < 2:
#             return "None"
#         last_close = hist['Close'].iloc[-2]
#         pre_price = premarket['Close'].iloc[-1]
#         if pre_price > last_close:
#             return "PreMarketUp"
#         elif pre_price < last_close:
#             return "PreMarketDown"
#         else:
#             return "None"
#     except Exception:
#         return "None"

# def scan_stock(ticker, n_days=5, threshold=6.0):
#     try:
#         stock = yf.Ticker(ticker)
#         data = stock.history(period="2mo")
#         if len(data) < n_days + 14:
#             return None
#         data['RSI'] = compute_RSI(data['Close'])
#         data['VWAP'] = compute_VWAP(data)
#         recent = data.tail(n_days + 1)
#         closes = recent['Close'].values
#         changes = []
#         for i in range(1, len(closes)):
#             prev_close = closes[i-1]
#             change = ((closes[i] - prev_close) / prev_close) * 100
#             changes.append(round(change, 2))
#             if change < threshold:
#                 return None
#         premarket_flag = get_premarket_flag(ticker)
#         return {
#             'ticker': ticker,
#             'changes': changes,
#             'prices': [round(x, 2) for x in recent['Close'].values[1:]],
#             'volumes': [int(v) for v in recent['Volume'].values[1:]],
#             'current_price': round(recent['Close'].iloc[-1], 2),
#             'current_volume': int(recent['Volume'].iloc[-1]),
#             'RSI': round(recent['RSI'].iloc[-1], 2),
#             'VWAP': round(recent['VWAP'].iloc[-1], 2),
#             'PremarketFlag': premarket_flag
#         }
#     except Exception:
#         return None

# def safe_scan_stock(ticker, n_days, threshold, retries=3):
#     for _ in range(retries):
#         result = scan_stock(ticker, n_days, threshold)
#         if result:
#             return result
#     return None

# def process_ticker(ticker, n_days, threshold, progress_counter, total_tickers):
#     result = safe_scan_stock(ticker, n_days, threshold)
#     with print_lock:
#         progress_counter[0] += 1
#         if progress_counter[0] % 20 == 0 or result is not None:
#             print(f"Progress: {progress_counter[0]}/{total_tickers}")
#         if result is not None:
#             print(f"✓ Found: {ticker}")
#     return result

# def main():
#     N_DAYS = 3
#     THRESHOLD = 5.0
#     MAX_STOCKS = 7728
#     MAX_WORKERS = 20

#     print("Loading tickers...")
#     tickers = load_tickers()
#     print(f"Loaded {len(tickers)} tickers")
#     print(f"Scanning for {N_DAYS} consecutive days with {THRESHOLD}%+ gains using {MAX_WORKERS} threads...")

#     results = []
#     progress_counter = [0]

#     with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         futures = {
#             executor.submit(process_ticker, ticker, N_DAYS, THRESHOLD, progress_counter, min(len(tickers), MAX_STOCKS)): ticker
#             for ticker in tickers[:MAX_STOCKS]
#         }
#         for future in concurrent.futures.as_completed(futures):
#             result = future.result()
#             if result:
#                 results.append(result)

#     if results:
#         headers = ['Ticker']
#         for i in range(N_DAYS):
#             headers.extend([f'Day-{i+1} Price', f'Day-{i+1} Change %', f'Day-{i+1} Volume'])
#         headers.extend(['Current Price', 'Current Volume', 'RSI', 'VWAP', 'PremarketFlag'])

#         csv_data = []
#         for r in results:
#             row = [r['ticker']]
#             for i in range(N_DAYS):
#                 row.append(r['prices'][i])
#                 row.append(f"{r['changes'][i]}%")
#                 row.append(r['volumes'][i])
#             row.extend([r['current_price'], r['current_volume'], r['RSI'], r['VWAP'], r['PremarketFlag']])
#             csv_data.append(','.join(map(str, row)))

#         filename = f"stocks_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
#         with open(filename, 'w') as f:
#             f.write(','.join(headers) + '\n')
#             f.write('\n'.join(csv_data))

#         print(f"Results saved to: {filename}")
#         print(','.join(headers))
#         for row in csv_data[:5]:
#             print(row)
#     else:
#         print("No stocks found meeting criteria")

# if __name__ == "__main__":
#     main()


#!/usr/bin/env python3
"""
Short Stock Consecutive Gains Scanner with RSI, VWAP & PreMarket Flag
Multi-threaded version for improved performance
pip install yfinance pandas numpy
"""
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
import concurrent.futures
import threading

# Lock for thread-safe printing
print_lock = threading.Lock()

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
    """Check if ticker is up or down in pre-market vs yesterday's close"""
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
        # Yesterday's close
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
        # Check consecutive gains
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

def process_ticker(ticker, n_days, threshold, progress_counter, total_tickers):
    """Process a single ticker with progress reporting"""
    result = scan_stock(ticker, n_days, threshold)
    
    # Thread-safe progress reporting
    with print_lock:
        progress_counter[0] += 1
        if progress_counter[0] % 20 == 0 or result:
            print(f"Progress: {progress_counter[0]}/{total_tickers}")
        if result:
            print(f"✓ Found: {ticker}")
    
    return result

def main():
    N_DAYS = 3
    THRESHOLD = 5.0
    MAX_STOCKS = 7728  # adjust as needed
    MAX_WORKERS = 40  # Number of threads to use - adjust based on your system
    
    print("Loading tickers...")
    tickers = load_tickers()
    print(f"Loaded {len(tickers)} tickers")
    print(f"Scanning for {N_DAYS} consecutive days with {THRESHOLD}%+ gains...")
    print(f"Using {MAX_WORKERS} threads for parallel processing...")
    
    results = []
    progress_counter = [0]  # Using a list to make it mutable in threads
    
    # Create a thread pool and process tickers in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks to the executor
        futures = {
            executor.submit(
                process_ticker, 
                ticker, 
                N_DAYS, 
                THRESHOLD, 
                progress_counter, 
                min(len(tickers), MAX_STOCKS)
            ): ticker for ticker in tickers[:MAX_STOCKS]
        }
        
        # Collect results as they complete
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                results.append(result)
    
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