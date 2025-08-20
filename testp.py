import yfinance as yf
import pandas as pd
import asyncio
import pytz
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

NASDAQ_FILE = "nasdaqlisted.txt"
OTHER_FILE = "otherlisted.txt"

class PreMarketScanner:
    def __init__(self):
        self.buy_bucket = []
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.est = pytz.timezone("US/Eastern")

    def read_tickers(self):
        """Read tickers from NASDAQ + other files"""
        tickers = []
        for file in [NASDAQ_FILE, OTHER_FILE]:
            try:
                with open(file, "r") as f:
                    for line in f:
                        if line.startswith("Symbol") or "File Creation" in line:
                            continue
                        parts = line.strip().split("|")
                        if parts and parts[0].isalpha():
                            tickers.append(parts[0])
            except FileNotFoundError:
                continue
        return list(set(tickers))

    def is_bullish_streak(self, ticker, min_pct=3.0):
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="7d", interval="1d")
            closes = hist["Close"].dropna()
            if len(closes) < 4:
                return False

            last4 = closes[-4:]  # last 4 closes (so we can compare 3 changes)
            for i in range(1, len(last4)):
                change_pct = (last4.iloc[i] - last4.iloc[i-1]) / last4.iloc[i-1] * 100
                print(change_pct)
                if change_pct < min_pct:
                    return False
            return True
        except Exception:
            return False


    def check_ticker_sync(self, ticker):
        try:
            if not ticker.isalnum():
                return None

            stock = yf.Ticker(ticker)
            df = stock.history(period="1d", interval="1m", prepost=True)

            if df.empty:
                return None

            df = df.tz_convert(self.est)
            premkt = df.between_time("04:00", "09:30")
            if premkt.empty:
                return None

            first3h = premkt.between_time("04:00", "07:00")
            if first3h.empty:
                return None

            first_open = first3h["Open"].iloc[0]
            last_price = first3h["Close"].iloc[-1]
            change_pct = (last_price - first_open) / first_open * 100

            if change_pct >= 3:
                # now check 3-day bullish streak
                if self.is_bullish_streak(ticker, min_pct=3.0):
                    return {
                        "ticker": ticker,
                        "open": round(first_open, 2),
                        "last": round(last_price, 2),
                        "premkt_change_pct": round(change_pct, 2),
                        "bullish_streak": True
                    }
            return None

        except Exception as e:
            print(f"❌ Error with {ticker}: {e}")
            return None

    async def check_ticker(self, ticker):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.check_ticker_sync, ticker)

    async def run_scan(self):
        tickers = self.read_tickers()
        print(f"Loaded {len(tickers)} tickers for scanning...")

        tasks = [self.check_ticker(t) for t in tickers[:1700]]  # limit
        results = await asyncio.gather(*tasks)

        for res in results:
            if res:
                self.buy_bucket.append(res)

        print("\n=== BUY BUCKET (Pre-market +3% AND 3-day bullish streak) ===")
        for stock in self.buy_bucket:
            print(stock)


async def main():
    scanner = PreMarketScanner()
    await scanner.run_scan()

if __name__ == "__main__":
    asyncio.run(main())
