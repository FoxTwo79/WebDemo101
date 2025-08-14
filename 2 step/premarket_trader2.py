# Full pipeline (analysis + paper trading)
# python async_scanner.py --mode full

# # Analysis only with higher concurrency
# python async_scanner.py --mode analysis --max-concurrent 150

# # Disable news fetching for faster processing
# python async_scanner.py --mode full --disable-news

# # Custom batch size
# python async_scanner.py --mode analysis --batch-size 100

import os
import re
import time
import asyncio
import aiohttp
import logging
from datetime import datetime, time as dtime, timezone
from dateutil import tz
from typing import List, Optional, Tuple, Dict, Any
import json
from contextlib import asynccontextmanager

import pandas as pd
import numpy as np
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# -------------------------
# Configuration
# -------------------------
class Config:
    # File paths
    NASDAQ_FILE = "nasdaqlisted.txt"
    OTHER_FILE = "otherlisted.txt"
    OUTPUT_CSV = "premarket_log.csv"
    CANDIDATES_FILE = "trade_candidates.csv"
    RESULTS_FILE = "trade_results.csv"
    ERROR_LOG_FILE = "scanner_errors.log"
    
    # Filtering criteria
    MIN_PCT = 5.0
    MAX_PCT = 15.0
    LOOKBACK_DAYS = 5
    
    # Async configuration
    MAX_CONCURRENT_REQUESTS = 100  # Configurable concurrency limit
    REQUEST_TIMEOUT = 30  # seconds
    RATE_LIMIT_DELAY = 0.1  # seconds between batches
    BATCH_SIZE = 50  # Smaller batches for async processing
    
    # Trading configuration
    POSITION_SIZE_USD = 1000
    BUY_TIME_IST = dtime(18, 0)
    SELL_TIME_IST = dtime(18, 55)
    
    # News configuration
    ENABLE_NEWS_FETCH = True  # Toggle for news fetching
    NEWS_LOOKBACK_HOURS = 24
    NEWS_POSITIVE_THRESHOLD = 0.1
    NEWS_MAX_CONCURRENT = 50
    NEWS_TIMEOUT = 15
    
    # Yahoo Finance API endpoints
    PRICE_API = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    NEWS_API = "https://query1.finance.yahoo.com/v1/finance/search"

# -------------------------
# Logging Setup
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Config.ERROR_LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# -------------------------
# Utilities
# -------------------------
class RateLimiter:
    def __init__(self, max_calls: int, time_window: float):
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls = []
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        async with self._lock:
            now = time.time()
            # Remove old calls outside the time window
            self.calls = [call_time for call_time in self.calls if now - call_time < self.time_window]
            
            if len(self.calls) >= self.max_calls:
                sleep_time = self.time_window - (now - self.calls[0])
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                    return await self.acquire()
            
            self.calls.append(now)

def get_current_ist():
    ist = tz.gettz("Asia/Kolkata")
    return datetime.now(ist)

def read_tickers_from_file(path: str) -> List[str]:
    """Read tickers from file with improved parsing"""
    tickers = []
    if not os.path.exists(path):
        logger.warning(f"File not found: {path}")
        return tickers
        
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                tokens = re.split(r'[\s,\t|;]+', line)
                found = None
                for tok in tokens:
                    tok = tok.strip()
                    if not tok:
                        continue
                    if tok.lower() in ("symbol", "ticker", "exchange", "name"):
                        continue
                    if re.match(r"^[A-Za-z0-9\.\-]{1,10}$", tok):
                        found = tok.upper()
                        break
                if found:
                    tickers.append(found)
    except Exception as e:
        logger.error(f"Error reading file {path}: {e}")
    
    return tickers

def compute_close_to_close_pct(prices: List[float], n_changes: int = 3) -> List[float]:
    """Compute percentage changes from price list"""
    if len(prices) < (n_changes + 1):
        return []
    
    pct_changes = []
    for i in range(1, len(prices)):
        if prices[i-1] != 0:
            pct_change = (prices[i] - prices[i-1]) / prices[i-1] * 100.0
            pct_changes.append(pct_change)
    
    return pct_changes[-n_changes:]

# -------------------------
# Async HTTP Client
# -------------------------
class AsyncHTTPClient:
    def __init__(self):
        self.session = None
        self.rate_limiter = RateLimiter(max_calls=200, time_window=60)  # 200 calls per minute
        
    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=Config.MAX_CONCURRENT_REQUESTS)
        timeout = aiohttp.ClientTimeout(total=Config.REQUEST_TIMEOUT)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def get_json(self, url: str, params: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """Make async GET request with rate limiting"""
        await self.rate_limiter.acquire()
        
        try:
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.warning(f"HTTP {response.status} for URL: {url}")
                    return None
        except asyncio.TimeoutError:
            logger.warning(f"Timeout for URL: {url}")
            return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

# -------------------------
# Sentiment Analysis
# -------------------------
class NewsAnalyzer:
    def __init__(self):
        self.analyzer = SentimentIntensityAnalyzer()
    
    def score_headlines_vader(self, titles: List[str]) -> Optional[float]:
        """Score headlines using VADER sentiment analyzer"""
        if not titles:
            return None
        scores = [self.analyzer.polarity_scores(t)["compound"] for t in titles if t]
        return sum(scores) / len(scores) if scores else None

# -------------------------
# Async Stock Data Fetcher
# -------------------------
class AsyncStockScanner:
    def __init__(self):
        self.news_analyzer = NewsAnalyzer()
        self.semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_REQUESTS)
        
    async def fetch_stock_data(self, client: AsyncHTTPClient, symbol: str) -> Dict[str, Any]:
        """Fetch stock data for a single symbol"""
        async with self.semaphore:
            try:
                url = Config.PRICE_API.format(symbol=symbol)
                data = await client.get_json(url)
                
                if not data or 'chart' not in data:
                    return {'ticker': symbol, 'error': 'No chart data'}
                
                chart = data['chart']['result'][0] if data['chart']['result'] else None
                if not chart:
                    return {'ticker': symbol, 'error': 'No chart result'}
                
                meta = chart.get('meta', {})
                timestamps = chart.get('timestamp', [])
                indicators = chart.get('indicators', {})
                
                # Extract current price info
                current_price = meta.get('regularMarketPrice')
                prev_close = meta.get('previousClose')
                pre_market_price = meta.get('preMarketPrice')
                market_state = meta.get('marketState')
                volume = meta.get('regularMarketVolume')
                market_cap = meta.get('marketCap')
                
                # Extract historical closes
                quote_data = indicators.get('quote', [{}])[0] if indicators.get('quote') else {}
                closes = quote_data.get('close', [])
                
                # Clean closes (remove None values)
                clean_closes = [c for c in closes if c is not None]
                
                # Calculate pre-market percentage
                pre_market_pct = None
                if pre_market_price is not None and prev_close not in (None, 0):
                    try:
                        pre_market_pct = (float(pre_market_price) - float(prev_close)) / float(prev_close) * 100.0
                    except (ValueError, ZeroDivisionError):
                        pre_market_pct = None
                
                # Calculate historical percentage changes
                last3_pct_changes = compute_close_to_close_pct(clean_closes, 3)
                
                # Check if meets filter criteria
                meets_filter = (
                    pre_market_pct is not None and
                    Config.MIN_PCT <= pre_market_pct <= Config.MAX_PCT
                )
                
                result = {
                    'ticker': symbol,
                    'market_state': market_state,
                    'current_price': current_price,
                    'pre_market_price': pre_market_price,
                    'pre_market_pct': pre_market_pct,
                    'prev_close': prev_close,
                    'volume': volume,
                    'market_cap': market_cap,
                    'last_3_days_pct_changes': last3_pct_changes,
                    'meets_filter': meets_filter,
                    'timestamp_utc': datetime.now(timezone.utc).isoformat()
                }
                
                return result
                
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {e}")
                return {'ticker': symbol, 'error': str(e)}
    
    async def fetch_news_data(self, client: AsyncHTTPClient, symbol: str) -> Dict[str, Any]:
        """Fetch news data for a single symbol"""
        if not Config.ENABLE_NEWS_FETCH:
            return {
                'ticker': symbol,
                'news_avg_compound': None,
                'positive_news': False,
                'news_articles_used': 0
            }
        
        try:
            # Use Yahoo Finance news search
            params = {
                'q': symbol,
                'quotesCount': 0,
                'newsCount': 10
            }
            
            data = await client.get_json(Config.NEWS_API, params=params)
            
            if not data or 'news' not in data:
                return {
                    'ticker': symbol,
                    'news_avg_compound': None,
                    'positive_news': False,
                    'news_articles_used': 0
                }
            
            # Extract recent headlines
            cutoff_time = datetime.utcnow().timestamp() - Config.NEWS_LOOKBACK_HOURS * 3600
            headlines = []
            
            for article in data['news']:
                pub_time = article.get('providerPublishTime', 0)
                if pub_time >= cutoff_time:
                    title = article.get('title', '')
                    if title:
                        headlines.append(title)
            
            # Analyze sentiment
            avg_sentiment = self.news_analyzer.score_headlines_vader(headlines)
            positive_news = (avg_sentiment is not None) and (avg_sentiment > Config.NEWS_POSITIVE_THRESHOLD)
            
            return {
                'ticker': symbol,
                'news_avg_compound': avg_sentiment,
                'positive_news': positive_news,
                'news_articles_used': len(headlines)
            }
            
        except Exception as e:
            logger.error(f"Error fetching news for {symbol}: {e}")
            return {
                'ticker': symbol,
                'news_avg_compound': None,
                'positive_news': False,
                'news_articles_used': 0
            }
    
    async def scan_tickers_batch(self, client: AsyncHTTPClient, tickers: List[str]) -> List[Dict[str, Any]]:
        """Scan a batch of tickers concurrently"""
        # Fetch stock data for all tickers in batch
        stock_tasks = [self.fetch_stock_data(client, ticker) for ticker in tickers]
        stock_results = await asyncio.gather(*stock_tasks, return_exceptions=True)
        
        # Process results and handle exceptions
        processed_results = []
        for result in stock_results:
            if isinstance(result, Exception):
                logger.error(f"Exception in stock data fetch: {result}")
                continue
            if isinstance(result, dict) and 'ticker' in result:
                processed_results.append(result)
        
        # Fetch news data for tickers that meet filter criteria
        if Config.ENABLE_NEWS_FETCH:
            filtered_tickers = [r['ticker'] for r in processed_results if r.get('meets_filter', False)]
            
            if filtered_tickers:
                news_tasks = [self.fetch_news_data(client, ticker) for ticker in filtered_tickers]
                news_results = await asyncio.gather(*news_tasks, return_exceptions=True)
                
                # Create news lookup
                news_lookup = {}
                for result in news_results:
                    if isinstance(result, dict) and 'ticker' in result:
                        news_lookup[result['ticker']] = result
                
                # Merge news data with stock data
                for stock_result in processed_results:
                    ticker = stock_result['ticker']
                    if ticker in news_lookup:
                        stock_result.update(news_lookup[ticker])
                    else:
                        stock_result.update({
                            'news_avg_compound': None,
                            'positive_news': False,
                            'news_articles_used': 0
                        })
        else:
            # Add empty news fields if news fetching is disabled
            for result in processed_results:
                result.update({
                    'news_avg_compound': None,
                    'positive_news': False,
                    'news_articles_used': 0
                })
        
        return processed_results
    
    async def scan_all_tickers(self, tickers: List[str]) -> pd.DataFrame:
        """Scan all tickers with optimized async processing"""
        logger.info(f"Starting async scan of {len(tickers)} tickers...")
        
        all_results = []
        total_batches = (len(tickers) + Config.BATCH_SIZE - 1) // Config.BATCH_SIZE
        
        async with AsyncHTTPClient() as client:
            for i in range(0, len(tickers), Config.BATCH_SIZE):
                batch = tickers[i:i + Config.BATCH_SIZE]
                batch_num = (i // Config.BATCH_SIZE) + 1
                
                logger.info(f"Processing batch {batch_num}/{total_batches}: {len(batch)} tickers")
                
                try:
                    batch_results = await self.scan_tickers_batch(client, batch)
                    all_results.extend(batch_results)
                    
                    # Rate limiting delay between batches
                    if i + Config.BATCH_SIZE < len(tickers):
                        await asyncio.sleep(Config.RATE_LIMIT_DELAY)
                        
                except Exception as e:
                    logger.error(f"Error processing batch {batch_num}: {e}")
                    continue
        
        logger.info(f"Completed scanning {len(all_results)} tickers successfully")
        return pd.DataFrame(all_results)

# -------------------------
# Paper Trading Simulator
# -------------------------
class PaperTrader:
    def __init__(self):
        self.scanner = AsyncStockScanner()
    
    async def execute_paper_trades(self, candidates_df: pd.DataFrame) -> pd.DataFrame:
        """Execute paper trades on candidates"""
        if candidates_df.empty:
            logger.info("No candidates for paper trading")
            return pd.DataFrame()
        
        logger.info(f"Executing paper trades for {len(candidates_df)} candidates")
        
        trades = []
        async with AsyncHTTPClient() as client:
            for _, row in candidates_df.iterrows():
                ticker = row['ticker']
                
                # Fetch current market data for buy/sell prices
                current_data = await self.scanner.fetch_stock_data(client, ticker)
                
                if 'error' in current_data:
                    continue
                
                buy_price = current_data.get('current_price')
                sell_price = current_data.get('current_price')  # In real scenario, fetch at SELL_TIME_IST
                
                if buy_price and sell_price:
                    quantity = Config.POSITION_SIZE_USD / buy_price
                    pl_usd = (sell_price - buy_price) * quantity
                    
                    trade_result = row.to_dict()
                    trade_result.update({
                        'buy_price': buy_price,
                        'sell_price': sell_price,
                        'quantity': quantity,
                        'pl_usd': pl_usd,
                        'trade_timestamp': datetime.now(timezone.utc).isoformat()
                    })
                    trades.append(trade_result)
        
        return pd.DataFrame(trades)

# -------------------------
# Main Application
# -------------------------
class StockScannerApp:
    def __init__(self):
        self.scanner = AsyncStockScanner()
        self.paper_trader = PaperTrader()
    
    def load_tickers(self) -> List[str]:
        """Load all tickers from files"""
        logger.info("Loading tickers from files...")
        
        nasdaq_tickers = read_tickers_from_file(Config.NASDAQ_FILE)
        other_tickers = read_tickers_from_file(Config.OTHER_FILE)
        
        # Combine and deduplicate
        all_tickers = list(dict.fromkeys([t.upper() for t in nasdaq_tickers + other_tickers if t]))
        
        logger.info(f"Loaded {len(nasdaq_tickers)} NASDAQ tickers")
        logger.info(f"Loaded {len(other_tickers)} other tickers")
        logger.info(f"Total unique tickers: {len(all_tickers)}")
        
        return all_tickers
    
    async def run_analysis(self) -> pd.DataFrame:
        """Run the complete analysis pipeline"""
        # Load tickers
        tickers = self.load_tickers()
        if not tickers:
            logger.error("No tickers found")
            return pd.DataFrame()
        
        # Scan all tickers
        logger.info("Starting async ticker scanning...")
        start_time = time.time()
        
        df = await self.scanner.scan_all_tickers(tickers)
        
        end_time = time.time()
        logger.info(f"Scanning completed in {end_time - start_time:.2f} seconds")
        
        # Save all results
        df.to_csv(Config.OUTPUT_CSV, index=False)
        logger.info(f"All results saved to {Config.OUTPUT_CSV}")
        
        # Filter candidates
        filtered_df = df[df['meets_filter'] == True].copy()
        logger.info(f"Found {len(filtered_df)} candidates meeting filter criteria")
        
        # Further filter by positive news if enabled
        if Config.ENABLE_NEWS_FETCH:
            final_candidates = filtered_df[filtered_df['positive_news'] == True].copy()
            logger.info(f"Found {len(final_candidates)} candidates with positive news")
        else:
            final_candidates = filtered_df.copy()
        
        # Save candidates
        final_candidates.to_csv(Config.CANDIDATES_FILE, index=False)
        logger.info(f"Final candidates saved to {Config.CANDIDATES_FILE}")
        
        return final_candidates
    
    async def run_paper_trades(self, candidates_df: pd.DataFrame = None) -> pd.DataFrame:
        """Run paper trading simulation"""
        if candidates_df is None:
            if not os.path.exists(Config.CANDIDATES_FILE):
                logger.error("No candidates file found. Run analysis first.")
                return pd.DataFrame()
            candidates_df = pd.read_csv(Config.CANDIDATES_FILE)
        
        if candidates_df.empty:
            logger.info("No candidates for paper trading")
            return pd.DataFrame()
        
        # Execute paper trades
        trades_df = await self.paper_trader.execute_paper_trades(candidates_df)
        
        if not trades_df.empty:
            trades_df.to_csv(Config.RESULTS_FILE, index=False)
            logger.info(f"Paper trade results saved to {Config.RESULTS_FILE}")
            
            # Log summary statistics
            total_pl = trades_df['pl_usd'].sum()
            avg_pl = trades_df['pl_usd'].mean()
            win_rate = (trades_df['pl_usd'] > 0).mean() * 100
            
            logger.info(f"Paper Trading Summary:")
            logger.info(f"  Total trades: {len(trades_df)}")
            logger.info(f"  Total P&L: ${total_pl:.2f}")
            logger.info(f"  Average P&L: ${avg_pl:.2f}")
            logger.info(f"  Win rate: {win_rate:.1f}%")
        
        return trades_df
    
    async def run_full_pipeline(self):
        """Run the complete pipeline: analysis + paper trading"""
        logger.info("Starting full pipeline execution...")
        
        # Run analysis
        candidates_df = await self.run_analysis()
        
        # Wait until trading time if needed
        now_ist = get_current_ist()
        target_time = datetime.combine(now_ist.date(), Config.BUY_TIME_IST, tzinfo=tz.gettz("Asia/Kolkata"))
        
        if now_ist < target_time:
            wait_seconds = (target_time - now_ist).total_seconds()
            logger.info(f"Waiting {wait_seconds/60:.1f} minutes until trading time ({Config.BUY_TIME_IST})")
            await asyncio.sleep(wait_seconds)
        
        # Run paper trades
        await self.run_paper_trades(candidates_df)
        
        logger.info("Full pipeline execution completed!")

# -------------------------
# CLI Interface
# -------------------------
async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Async Stock Scanner for 11K+ Tickers")
    parser.add_argument("--mode", choices=["analysis", "trades", "full"], default="full",
                        help="Run mode: analysis only, trades only, or full pipeline")
    parser.add_argument("--max-concurrent", type=int, default=Config.MAX_CONCURRENT_REQUESTS,
                        help="Maximum concurrent requests")
    parser.add_argument("--disable-news", action="store_true", 
                        help="Disable news fetching")
    parser.add_argument("--batch-size", type=int, default=Config.BATCH_SIZE,
                        help="Batch size for processing")
    
    args = parser.parse_args()
    
    # Update configuration based on arguments
    Config.MAX_CONCURRENT_REQUESTS = args.max_concurrent
    Config.ENABLE_NEWS_FETCH = not args.disable_news
    Config.BATCH_SIZE = args.batch_size
    
    # Create and run the application
    app = StockScannerApp()
    
    try:
        if args.mode == "analysis":
            await app.run_analysis()
        elif args.mode == "trades":
            await app.run_paper_trades()
        else:
            await app.run_full_pipeline()
    except KeyboardInterrupt:
        logger.info("Execution interrupted by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())