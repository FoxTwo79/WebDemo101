import yfinance as yf
import pandas as pd
import asyncio
import pytz
from datetime import datetime, time
from concurrent.futures import ThreadPoolExecutor
import csv
import os
import json
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional
import threading
import time as time_module

NASDAQ_FILE = "nasdaqlisted.txt"
OTHER_FILE = "otherlisted.txt"

@dataclass
class ScanConfig:
    """Configuration settings for the scanner"""
    min_premarket_gain: float = 3.0
    min_daily_gain: float = 3.0
    scan_limit: int = 11500
    batch_size: int = 50
    max_workers: int = 10
    live_update_interval: int = 3  # seconds
    scan_mode: str = "batch"  # batch, sequential, random, parallel
    
    def save_config(self, filename="scanner_config.json"):
        """Save configuration to JSON file"""
        with open(filename, 'w') as f:
            json.dump(asdict(self), f, indent=2)
    
    @classmethod
    def load_config(cls, filename="scanner_config.json"):
        """Load configuration from JSON file"""
        try:
            with open(filename, 'r') as f:
                config_dict = json.load(f)
                return cls(**config_dict)
        except FileNotFoundError:
            print("📋 Using default configuration")
            return cls()

@dataclass
class StockPosition:
    """Represents a stock position with live tracking"""
    ticker: str
    entry_price: float
    current_price: float = 0.0
    premkt_change_pct: float = 0.0
    profit_loss_pct: float = 0.0
    profit_loss_amount: float = 0.0
    daily_changes: List[float] = None
    volume: int = 0
    sector: str = "N/A"
    industry: str = "N/A"
    market_cap: str = "N/A"
    last_updated: str = ""
    
    def __post_init__(self):
        if self.daily_changes is None:
            self.daily_changes = []

class LiveDashboard:
    """Real-time dashboard for tracking positions"""
    
    def __init__(self, positions: List[StockPosition], config: ScanConfig):
        self.positions = positions
        self.config = config
        self.is_running = False
        self.total_profit_loss = 0.0
        self.win_count = 0
        self.loss_count = 0
        self.update_thread = None
        
    def calculate_stats(self):
        """Calculate win/loss statistics"""
        self.win_count = sum(1 for p in self.positions if p.profit_loss_pct > 0)
        self.loss_count = sum(1 for p in self.positions if p.profit_loss_pct < 0)
        self.total_profit_loss = sum(p.profit_loss_pct for p in self.positions)
        
    def update_position_price(self, position: StockPosition):
        """Update a single position's current price"""
        try:
            stock = yf.Ticker(position.ticker)
            current_data = stock.history(period="1d", interval="1m")
            
            if not current_data.empty:
                position.current_price = round(current_data["Close"].iloc[-1], 2)
                position.profit_loss_amount = round(position.current_price - position.entry_price, 2)
                position.profit_loss_pct = round(((position.current_price - position.entry_price) / position.entry_price) * 100, 2)
                position.last_updated = datetime.now().strftime("%H:%M:%S")
                return True
            return False
        except Exception as e:
            print(f"⚠️ Error updating {position.ticker}: {e}")
            return False
    
    def display_dashboard(self):
        """Display the live dashboard"""
        os.system('cls' if os.name == 'nt' else 'clear')  # Clear screen
        
        print("🔴 LIVE PRE-MARKET TRADING DASHBOARD 🔴")
        print("=" * 80)
        print(f"⏰ Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 Total Positions: {len(self.positions)}")
        print(f"🟢 Wins: {self.win_count} | 🔴 Losses: {self.loss_count}")
        print(f"💰 Total P&L: {self.total_profit_loss:+.2f}%")
        print(f"📈 Win Rate: {(self.win_count/(len(self.positions)) * 100) if self.positions else 0:.1f}%")
        print("=" * 80)
        
        if not self.positions:
            print("📭 No positions to track")
            return
            
        # Sort positions by profit/loss percentage
        sorted_positions = sorted(self.positions, key=lambda x: x.profit_loss_pct, reverse=True)
        
        print(f"{'TICKER':<8} {'ENTRY':<8} {'CURRENT':<8} {'P&L %':<8} {'P&L $':<8} {'UPDATED':<10}")
        print("-" * 70)
        
        for pos in sorted_positions:
            status = "🟢" if pos.profit_loss_pct > 0 else "🔴" if pos.profit_loss_pct < 0 else "⚪"
            print(f"{status} {pos.ticker:<6} ${pos.entry_price:<7.2f} ${pos.current_price:<7.2f} "
                  f"{pos.profit_loss_pct:+7.2f}% ${pos.profit_loss_amount:+7.2f} {pos.last_updated:<10}")
    
    def live_update_loop(self):
        """Background thread for live updates"""
        while self.is_running:
            # Update all positions
            for position in self.positions:
                if self.is_running:  # Check if still running
                    self.update_position_price(position)
            
            # Calculate new stats
            self.calculate_stats()
            
            # Display updated dashboard
            self.display_dashboard()
            
            # Wait for next update
            for _ in range(self.config.live_update_interval):
                if not self.is_running:
                    break
                time_module.sleep(1)
    
    def start_live_tracking(self):
        """Start live tracking in background thread"""
        if self.is_running:
            print("⚠️ Live tracking already running")
            return
            
        self.is_running = True
        self.update_thread = threading.Thread(target=self.live_update_loop, daemon=True)
        self.update_thread.start()
        print(f"🔴 LIVE TRACKING STARTED (Updates every {self.config.live_update_interval}s)")
        print("Press Ctrl+C to stop...")
        
        try:
            # Keep main thread alive
            while self.is_running:
                time_module.sleep(1)
        except KeyboardInterrupt:
            self.stop_live_tracking()
    
    def stop_live_tracking(self):
        """Stop live tracking"""
        self.is_running = False
        if self.update_thread:
            self.update_thread.join(timeout=2)
        print("\n⏹️ Live tracking stopped")
    
    def save_live_results(self):
        """Save current live results to CSV"""
        if not self.positions:
            return
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"live_results_{timestamp}.csv"
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    "ticker", "entry_price", "current_price", "profit_loss_pct", 
                    "profit_loss_amount", "premkt_change_pct", "daily_changes",
                    "volume", "sector", "industry", "market_cap", "last_updated"
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for pos in self.positions:
                    row = {
                        "ticker": pos.ticker,
                        "entry_price": pos.entry_price,
                        "current_price": pos.current_price,
                        "profit_loss_pct": pos.profit_loss_pct,
                        "profit_loss_amount": pos.profit_loss_amount,
                        "premkt_change_pct": pos.premkt_change_pct,
                        "daily_changes": str(pos.daily_changes),
                        "volume": pos.volume,
                        "sector": pos.sector,
                        "industry": pos.industry,
                        "market_cap": pos.market_cap,
                        "last_updated": pos.last_updated
                    }
                    writer.writerow(row)
            
            print(f"💾 Live results saved to: {filename}")
            
        except Exception as e:
            print(f"❌ Error saving live results: {e}")

class PreMarketScanner:
    def __init__(self, config: Optional[ScanConfig] = None):
        self.config = config or ScanConfig.load_config()
        self.buy_bucket = []
        self.positions = []
        self.executor = ThreadPoolExecutor(max_workers=self.config.max_workers)
        self.est = pytz.timezone("US/Eastern")
        self.scan_timestamp = datetime.now(self.est).strftime("%Y%m%d_%H%M%S")
        self.csv_filename = f"premarket_scan_{self.scan_timestamp}.csv"
        self.dashboard = None

    def configure_scanner(self):
        """Interactive configuration setup"""
        print("\n⚙️ SCANNER CONFIGURATION")
        print("=" * 40)
        
        try:
            self.config.min_premarket_gain = float(input(f"Min pre-market gain % (current: {self.config.min_premarket_gain}): ") or self.config.min_premarket_gain)
            self.config.min_daily_gain = float(input(f"Min daily gain % for streak (current: {self.config.min_daily_gain}): ") or self.config.min_daily_gain)
            self.config.scan_limit = int(input(f"Max tickers to scan (current: {self.config.scan_limit}): ") or self.config.scan_limit)
            self.config.live_update_interval = int(input(f"Live update interval in seconds (current: {self.config.live_update_interval}): ") or self.config.live_update_interval)
            
            save_config = input("Save this configuration? (y/n): ").lower() == 'y'
            if save_config:
                self.config.save_config()
                print("✅ Configuration saved!")
                
        except ValueError:
            print("⚠️ Invalid input, using current configuration")

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
                            if ticker.replace(".", "").replace("-", "").isalnum() and len(ticker) <= 5:
                                tickers.append(ticker)
            except FileNotFoundError:
                print(f"⚠️ Warning: {file} not found, skipping...")
                continue
        
        unique_tickers = list(set(tickers))
        print(f"📊 Loaded {len(unique_tickers)} unique tickers")
        return unique_tickers

    def is_bullish_streak(self, ticker, min_pct=None):
        """Check if stock has 3+ consecutive bullish days"""
        min_pct = min_pct or self.config.min_daily_gain
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="10d", interval="1d")
            
            if hist.empty:
                return False, []
            
            closes = hist["Close"].dropna()
            if len(closes) < 4:
                return False, []

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
            return False, []

    def get_premarket_data(self, ticker):
        """Get pre-market trading data for a ticker"""
        try:
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
                first3h = premkt

            if first3h.empty:
                return None

            first_open = first3h["Open"].iloc[0]
            last_price = first3h["Close"].iloc[-1]
            volume = first3h["Volume"].sum()
            
            change_pct = ((last_price - first_open) / first_open) * 100
            
            return {
                "open": round(first_open, 2),
                "last": round(last_price, 2),
                "change_pct": round(change_pct, 2),
                "volume": int(volume),
                "data_points": len(first3h)
            }
            
        except Exception as e:
            return None

    def check_ticker_sync(self, ticker):
        """Synchronous function to check a single ticker"""
        try:
            premkt_data = self.get_premarket_data(ticker)
            
            if not premkt_data or premkt_data["change_pct"] < self.config.min_premarket_gain:
                return None

            is_bullish, daily_changes = self.is_bullish_streak(ticker, self.config.min_daily_gain)
            
            if not is_bullish:
                return None

            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                market_cap = info.get('marketCap', 'N/A')
                sector = info.get('sector', 'N/A')
                industry = info.get('industry', 'N/A')
            except:
                market_cap = sector = industry = 'N/A'

            # Create StockPosition object
            position = StockPosition(
                ticker=ticker,
                entry_price=premkt_data["last"],
                current_price=premkt_data["last"],
                premkt_change_pct=premkt_data["change_pct"],
                daily_changes=daily_changes,
                volume=premkt_data["volume"],
                sector=sector,
                industry=industry,
                market_cap=str(market_cap),
                last_updated=datetime.now().strftime("%H:%M:%S")
            )
            
            print(f"✅ Found: {ticker} (+{premkt_data['change_pct']}%)")
            return position

        except Exception as e:
            print(f"❌ Error with {ticker}: {e}")
            return None

    async def check_ticker(self, ticker):
        """Async wrapper for ticker checking"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.check_ticker_sync, ticker)

    async def run_scan(self, scan_mode=None):
        """Main scanning function with configurable modes"""
        scan_mode = scan_mode or self.config.scan_mode
        
        print(f"🚀 Starting pre-market scan at {datetime.now(self.est).strftime('%Y-%m-%d %H:%M:%S EST')}")
        print(f"📋 Scan mode: {scan_mode.upper()}")
        print(f"📊 Min pre-market gain: {self.config.min_premarket_gain}%")
        print(f"📈 Min daily gain for streak: {self.config.min_daily_gain}%")
        
        tickers = self.read_tickers()
        
        if not tickers:
            print("❌ No tickers found to scan")
            return

        scan_limit = min(len(tickers), self.config.scan_limit)
        
        if scan_mode == "random":
            import random
            random.shuffle(tickers)
            print(f"🎲 Randomized ticker order")
        
        tickers_to_scan = tickers[:scan_limit]
        print(f"📈 Scanning {scan_limit} tickers...")

        # Process based on scan mode
        if scan_mode == "sequential":
            await self._scan_sequential(tickers_to_scan)
        elif scan_mode == "parallel":
            await self._scan_parallel(tickers_to_scan)
        else:  # batch or random
            await self._scan_batch(tickers_to_scan)

        print(f"\n=== SCAN COMPLETE ===")
        print(f"🎯 Found {len(self.positions)} stocks meeting criteria")
        
        if self.positions:
            self.save_to_csv()
            self.show_scan_results()
            
            # Ask if user wants live tracking
            start_live = input("\n🔴 Start live tracking? (y/n): ").lower() == 'y'
            if start_live:
                self.start_live_dashboard()
        else:
            print("📊 No stocks found meeting the criteria")

    async def _scan_sequential(self, tickers):
        """Sequential scanning - one by one"""
        print("⏳ Processing tickers one by one...")
        for i, ticker in enumerate(tickers, 1):
            result = await self.check_ticker(ticker)
            if result:
                self.positions.append(result)
            
            if i % 100 == 0:
                print(f"📊 Processed {i}/{len(tickers)} | Found: {len(self.positions)}")
            
            await asyncio.sleep(0.1)

    async def _scan_parallel(self, tickers):
        """Parallel scanning - all at once"""
        print("🚄 Processing all tickers simultaneously...")
        tasks = [self.check_ticker(t) for t in tickers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for res in results:
            if res and not isinstance(res, Exception):
                self.positions.append(res)

    async def _scan_batch(self, tickers):
        """Batch scanning - default method"""
        tasks = [self.check_ticker(t) for t in tickers]
        
        for i in range(0, len(tasks), self.config.batch_size):
            batch = tasks[i:i + self.config.batch_size]
            results = await asyncio.gather(*batch, return_exceptions=True)
            
            for res in results:
                if res and not isinstance(res, Exception):
                    self.positions.append(res)
            
            batch_num = i//self.config.batch_size + 1
            total_batches = (len(tasks)-1)//self.config.batch_size + 1
            print(f"📊 Processed batch {batch_num}/{total_batches} | Found: {len(self.positions)} stocks")
            
            await asyncio.sleep(1)

    def show_scan_results(self):
        """Display initial scan results"""
        print("\n=== INITIAL SCAN RESULTS ===")
        for pos in self.positions:
            print(f"📈 {pos.ticker}: ${pos.entry_price} (+{pos.premkt_change_pct}%) "
                  f"| Daily changes: {pos.daily_changes}")

    def save_to_csv(self):
        """Save scan results to CSV"""
        if not self.positions:
            print("📝 No stocks found to save to CSV")
            return

        try:
            with open(self.csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    "ticker", "entry_price", "current_price", "premkt_change_pct",
                    "daily_changes", "volume", "sector", "industry", "market_cap"
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for pos in self.positions:
                    row = {
                        "ticker": pos.ticker,
                        "entry_price": pos.entry_price,
                        "current_price": pos.current_price,
                        "premkt_change_pct": pos.premkt_change_pct,
                        "daily_changes": str(pos.daily_changes),
                        "volume": pos.volume,
                        "sector": pos.sector,
                        "industry": pos.industry,
                        "market_cap": pos.market_cap
                    }
                    writer.writerow(row)
            
            print(f"💾 Results saved to: {self.csv_filename}")
            
        except Exception as e:
            print(f"❌ Error saving to CSV: {e}")

    def start_live_dashboard(self):
        """Initialize and start live dashboard"""
        self.dashboard = LiveDashboard(self.positions, self.config)
        self.dashboard.start_live_tracking()
        
        # Save final results after live tracking stops
        self.dashboard.save_live_results()

    def __del__(self):
        """Cleanup executor on deletion"""
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=True)


async def main():
    """Main function with interactive menu"""
    print("🔍 LIVE PRE-MARKET SCANNER")
    print("=" * 50)
    
    # Load or create configuration
    config = ScanConfig.load_config()
    
    print("Choose an option:")
    print("1. Run scan with current settings")
    print("2. Configure scanner settings")
    print("3. View current configuration")
    print("4. Load positions from CSV for live tracking")
    
    try:
        choice = input("\nEnter choice (1-4): ").strip()
        
        scanner = PreMarketScanner(config)
        
        if choice == "2":
            scanner.configure_scanner()
            choice = "1"  # Run scan after configuration
        elif choice == "3":
            print("\n📋 CURRENT CONFIGURATION:")
            for key, value in asdict(config).items():
                print(f"  {key}: {value}")
            return
        elif choice == "4":
            csv_file = input("Enter CSV filename to load positions: ").strip()
            scanner.load_positions_from_csv(csv_file)
            scanner.start_live_dashboard()
            return
        
        if choice == "1" or choice == "":
            # Choose scan mode
            print("\nChoose scanning mode:")
            print("1. BATCH - Process 50 at a time ⚡ (recommended)")
            print("2. SEQUENTIAL - One by one 🐌")
            print("3. RANDOM - Randomized batch order 🎲")
            print("4. PARALLEL - All at once 🚄 (risky)")
            
            mode_choice = input("Enter mode (1-4) or press Enter for batch: ").strip()
            mode_map = {"1": "batch", "2": "sequential", "3": "random", "4": "parallel", "": "batch"}
            scan_mode = mode_map.get(mode_choice, "batch")
            
            await scanner.run_scan(scan_mode)
        
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

    def load_positions_from_csv(self, filename):
        """Load positions from existing CSV file"""
        try:
            df = pd.read_csv(filename)
            self.positions = []
            
            for _, row in df.iterrows():
                position = StockPosition(
                    ticker=row['ticker'],
                    entry_price=float(row['entry_price']),
                    current_price=float(row.get('current_price', row['entry_price'])),
                    premkt_change_pct=float(row['premkt_change_pct']),
                    daily_changes=eval(row['daily_changes']) if isinstance(row['daily_changes'], str) else [],
                    volume=int(row.get('volume', 0)),
                    sector=str(row.get('sector', 'N/A')),
                    industry=str(row.get('industry', 'N/A')),
                    market_cap=str(row.get('market_cap', 'N/A'))
                )
                self.positions.append(position)
            
            print(f"📂 Loaded {len(self.positions)} positions from {filename}")
            
        except Exception as e:
            print(f"❌ Error loading CSV: {e}")

if __name__ == "__main__":
    asyncio.run(main())