import yfinance as yf
import pandas as pd
import asyncio
import pytz
from datetime import datetime, time
from concurrent.futures import ThreadPoolExecutor
import csv
import os

NASDAQ_FILE = "nasdaqlisted.txt"
OTHER_FILE = "otherlisted.txt"

class PreMarketScanner:
    def __init__(self):
        self.buy_bucket = []
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.est = pytz.timezone("US/Eastern")
        self.scan_timestamp = datetime.now(self.est).strftime("%Y%m%d_%H%M%S")
        self.csv_filename = f"premarket_scan_{self.scan_timestamp}.csv"

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
                        if parts and len(parts) > 0 and parts[0].strip():
                            ticker = parts[0].strip()
                            # Better ticker validation
                            if ticker.replace(".", "").replace("-", "").isalnum() and len(ticker) <= 5:
                                tickers.append(ticker)
            except FileNotFoundError:
                print(f"⚠️ Warning: {file} not found, skipping...")
                continue
        
        unique_tickers = list(set(tickers))
        print(f"📊 Loaded {len(unique_tickers)} unique tickers")
        return unique_tickers

    def is_bullish_streak(self, ticker, min_pct=3.0):
        """Check if stock has 3+ consecutive bullish days"""
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="10d", interval="1d")  # Get more days for safety
            
            if hist.empty:
                return False, []
            
            closes = hist["Close"].dropna()
            if len(closes) < 4:
                return False, []

            # Get last 4 closes to calculate 3 daily changes
            last4 = closes[-4:]
            changes = []
            
            for i in range(1, len(last4)):
                prev_close = last4.iloc[i-1]
                curr_close = last4.iloc[i]
                change_pct = ((curr_close - prev_close) / prev_close) * 100
                changes.append(round(change_pct, 2))
                
                if change_pct < min_pct:
                    return False, changes
            
            return True, changes
            
        except Exception as e:
            print(f"⚠️ Error checking bullish streak for {ticker}: {e}")
            return False, []

    def get_premarket_data(self, ticker):
        """Get pre-market trading data for a ticker"""
        try:
            stock = yf.Ticker(ticker)
            
            # Get 1-minute data with pre/post market
            df = stock.history(period="1d", interval="1m", prepost=True)
            
            if df.empty:
                return None

            # Convert to EST timezone
            df = df.tz_convert(self.est)
            
            # Filter for pre-market hours (4:00 AM - 9:30 AM EST)
            premkt = df.between_time("04:00", "09:30")
            
            if premkt.empty:
                return None

            # Get first 3 hours of pre-market (4:00 AM - 7:00 AM)
            first3h = premkt.between_time("04:00", "07:00")
            
            if first3h.empty:
                # If no data in first 3 hours, use all available pre-market data
                first3h = premkt

            if first3h.empty:
                return None

            first_open = first3h["Open"].iloc[0]
            last_price = first3h["Close"].iloc[-1]
            volume = first3h["Volume"].sum()
            
            # Calculate percentage change
            change_pct = ((last_price - first_open) / first_open) * 100
            
            return {
                "open": round(first_open, 2),
                "last": round(last_price, 2),
                "change_pct": round(change_pct, 2),
                "volume": int(volume),
                "data_points": len(first3h)
            }
            
        except Exception as e:
            print(f"⚠️ Error getting pre-market data for {ticker}: {e}")
            return None

    def check_ticker_sync(self, ticker):
        """Synchronous function to check a single ticker"""
        try:
            # Get pre-market data
            premkt_data = self.get_premarket_data(ticker)
            
            if not premkt_data or premkt_data["change_pct"] < 3.0:
                return None

            # Check for bullish streak
            is_bullish, daily_changes = self.is_bullish_streak(ticker, min_pct=3.0)
            
            if not is_bullish:
                return None

            # Get additional stock info
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                market_cap = info.get('marketCap', 'N/A')
                sector = info.get('sector', 'N/A')
                industry = info.get('industry', 'N/A')
            except:
                market_cap = sector = industry = 'N/A'

            result = {
                "ticker": ticker,
                "premkt_open": premkt_data["open"],
                "premkt_last": premkt_data["last"],
                "premkt_change_pct": premkt_data["change_pct"],
                "premkt_volume": premkt_data["volume"],
                "daily_changes": daily_changes,
                "market_cap": market_cap,
                "sector": sector,
                "industry": industry,
                "scan_time": datetime.now(self.est).strftime("%H:%M:%S"),
                "bullish_streak": True
            }
            
            print(f"✅ Found: {ticker} (+{premkt_data['change_pct']}%)")
            return result

        except Exception as e:
            print(f"❌ Error with {ticker}: {e}")
            return None

    async def check_ticker(self, ticker):
        """Async wrapper for ticker checking"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.check_ticker_sync, ticker)

    def save_to_csv(self):
        """Save results to CSV file"""
        if not self.buy_bucket:
            print("📝 No stocks found to save to CSV")
            return

        fieldnames = [
            "ticker", "premkt_open", "premkt_last", "premkt_change_pct", 
            "premkt_volume", "daily_changes", "market_cap", "sector", 
            "industry", "scan_time", "bullish_streak"
        ]

        try:
            with open(self.csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for stock in self.buy_bucket:
                    # Convert daily_changes list to string for CSV
                    stock_copy = stock.copy()
                    stock_copy["daily_changes"] = str(stock_copy["daily_changes"])
                    writer.writerow(stock_copy)
            
            print(f"💾 Results saved to: {self.csv_filename}")
            
        except Exception as e:
            print(f"❌ Error saving to CSV: {e}")

    def wait_for_market_open(self):
        """Wait until 9:15 AM EST and then check prices"""
        now = datetime.now(self.est)
        market_check_time = now.replace(hour=9, minute=15, second=0, microsecond=0)
        
        if now > market_check_time:
            # If it's already past 9:15 AM, check immediately
            print("🕘 Market already open, checking current prices...")
            self.check_market_open_prices()
        else:
            # Wait until 9:15 AM
            wait_seconds = (market_check_time - now).total_seconds()
            print(f"⏰ Waiting {wait_seconds/60:.1f} minutes until 9:15 AM EST...")
            
            # This would block - in a real application, you might want to use async sleep
            # asyncio.sleep(wait_seconds) if in async context
            import time
            time.sleep(wait_seconds)
            self.check_market_open_prices()

    def check_market_open_prices(self):
        """Check prices at market open and calculate profit/loss"""
        if not self.buy_bucket:
            print("📊 No stocks in buy bucket to check")
            return

        print("\n=== MARKET OPEN PRICE CHECK (9:15 AM EST) ===")
        updated_results = []
        
        for stock in self.buy_bucket:
            try:
                ticker = stock["ticker"]
                entry_price = stock["premkt_last"]
                
                # Get current market price
                yf_ticker = yf.Ticker(ticker)
                current_data = yf_ticker.history(period="1d", interval="1m")
                
                if not current_data.empty:
                    current_price = current_data["Close"].iloc[-1]
                    profit_loss = ((current_price - entry_price) / entry_price) * 100
                    
                    stock["market_open_price"] = round(current_price, 2)
                    stock["profit_loss_pct"] = round(profit_loss, 2)
                    stock["profit_loss_amount"] = round(current_price - entry_price, 2)
                    
                    status = "🟢 PROFIT" if profit_loss > 0 else "🔴 LOSS"
                    print(f"{status} {ticker}: ${entry_price} → ${current_price} ({profit_loss:+.2f}%)")
                else:
                    stock["market_open_price"] = "N/A"
                    stock["profit_loss_pct"] = "N/A"
                    stock["profit_loss_amount"] = "N/A"
                    print(f"⚠️ {ticker}: Could not get market open price")
                
                updated_results.append(stock)
                
            except Exception as e:
                print(f"❌ Error checking {stock['ticker']}: {e}")
        
        # Save updated results with profit/loss data
        self.buy_bucket = updated_results
        market_csv = f"market_results_{self.scan_timestamp}.csv"
        
        try:
            fieldnames = list(self.buy_bucket[0].keys()) if self.buy_bucket else []
            with open(market_csv, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.buy_bucket)
            print(f"💾 Market results saved to: {market_csv}")
        except Exception as e:
            print(f"❌ Error saving market results: {e}")

    async def run_scan(self):
        """Main scanning function"""
        print(f"🚀 Starting pre-market scan at {datetime.now(self.est).strftime('%Y-%m-%d %H:%M:%S EST')}")
        
        tickers = self.read_tickers()
        
        if not tickers:
            print("❌ No tickers found to scan")
            return

        # Limit to prevent rate limiting (adjust as needed)
        scan_limit = min(len(tickers), 11500)
        print(f"📈 Scanning {scan_limit} tickers...")

        # Create async tasks
        tasks = [self.check_ticker(t) for t in tickers[:scan_limit]]
        
        # Process in batches to avoid overwhelming the API
        batch_size = 50
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            results = await asyncio.gather(*batch, return_exceptions=True)
            
            for res in results:
                if res and not isinstance(res, Exception):
                    self.buy_bucket.append(res)
            
            print(f"📊 Processed batch {i//batch_size + 1}/{(len(tasks)-1)//batch_size + 1}")
            
            # Small delay between batches
            await asyncio.sleep(1)

        print(f"\n=== SCAN COMPLETE ===")
        print(f"🎯 Found {len(self.buy_bucket)} stocks meeting criteria")
        
        if self.buy_bucket:
            print("\n=== BUY BUCKET (Pre-market +3% AND 3-day bullish streak) ===")
            for stock in self.buy_bucket:
                print(f"📈 {stock['ticker']}: ${stock['premkt_open']} → ${stock['premkt_last']} "
                      f"(+{stock['premkt_change_pct']}%) | Daily changes: {stock['daily_changes']}")
            
            # Save to CSV
            self.save_to_csv()
            
            # Ask user if they want to wait for market open
            user_input = input("\n⏰ Wait for 9:15 AM EST to check market prices? (y/n): ")
            if user_input.lower() in ['y', 'yes']:
                self.wait_for_market_open()
        else:
            print("📊 No stocks found meeting the criteria")

    def __del__(self):
        """Cleanup executor on deletion"""
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=True)


async def main():
    """Main function"""
    try:
        scanner = PreMarketScanner()
        await scanner.run_scan()
    except KeyboardInterrupt:
        print("\n⏹️ Scan interrupted by user")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    # Ensure event loop is properly configured
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")