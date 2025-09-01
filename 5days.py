#!/usr/bin/env python3
"""
Short Stock Consecutive Gains Scanner
pip install yfinance pandas
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def load_tickers():
    """Load stock tickers from files"""
    tickers = []
    
    # NASDAQ file
    try:
        with open("nasdaqlisted.txt", 'r') as f:
            for line in f.readlines()[1:]:  # Skip header
                parts = line.split('|')
                if len(parts) >= 8:
                    symbol = parts[0].strip()
                    if (symbol and symbol.isalpha() and len(symbol) <= 5 and 
                        parts[3] == 'N'):  # Not test issue
                        tickers.append(symbol)
    except FileNotFoundError:
        pass
    
    # Other exchanges file
    try:
        with open("otherlisted.txt", 'r') as f:
            for line in f.readlines()[1:]:
                parts = line.split('|')
                if len(parts) >= 3:
                    symbol = parts[0].strip()
                    exchange = parts[2].strip()
                    if (symbol and symbol.isalpha() and len(symbol) <= 5 and 
                        exchange in ['N', 'A']):  # NYSE, AMEX
                        tickers.append(symbol)
    except FileNotFoundError:
        pass
    
    return sorted(list(set(tickers)))

def scan_stock(ticker, n_days=5, threshold=6.0):
    """Check if stock meets consecutive gains criteria"""
    try:
        # Get stock data
        stock = yf.Ticker(ticker)
        data = stock.history(period="1mo")
        
        if len(data) < n_days + 1:
            return None
        
        # Get last n_days + 1 trading days
        closes = data['Close'].tail(n_days + 1).values
        baseline = closes[0]
        
        # Check consecutive gains
        changes = []
        for i in range(1, len(closes)):
            change = ((closes[i] - baseline) / baseline) * 100
            changes.append(round(change, 2))
            if change < threshold:
                return None
        
        return {
            'ticker': ticker,
            'changes': changes,
            'current_price': round(closes[-1], 2),
            'volume': int(data['Volume'].iloc[-1])
        }
    except:
        return None

def main():
    # Configuration
    N_DAYS = 4
    THRESHOLD = 6.0
    MAX_STOCKS = 7728  # Test with first 100 stocks
    
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
    
    # Output results
    if results:
        print(f"\n=== RESULTS: {len(results)} stocks found ===")
        
        # Create CSV
        csv_data = []
        for r in results:
            row = [r['ticker']]
            for i, change in enumerate(r['changes']):
                row.append(f"{change}%")
            row.extend([f"${r['current_price']}", f"{r['volume']:,}"])
            csv_data.append(','.join(map(str, row)))
        
        # Headers
        headers = ['Ticker']
        for i in range(N_DAYS):
            headers.append(f'Day-{N_DAYS-i} Change %')
        headers.extend(['Current Price', 'Volume'])
        
        # Save to file
        filename = f"stocks_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        with open(filename, 'w') as f:
            f.write(','.join(headers) + '\n')
            f.write('\n'.join(csv_data))
        
        print(f"Results saved to: {filename}")
        
        # Show preview
        print("\nPreview:")
        print(','.join(headers))
        for row in csv_data[:5]:
            print(row)
    else:
        print("No stocks found meeting criteria")

if __name__ == "__main__":
    main()