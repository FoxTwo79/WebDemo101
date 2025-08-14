import os
import re
import time
import json
import logging
import asyncio
import aiohttp
from datetime import datetime, time as dtime, timezone, timedelta
from dateutil import tz
from typing import List, Optional, Tuple, Dict, Set
import pandas as pd
import numpy as np
from yahooquery import Ticker
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import concurrent.futures
from functools import partial
import warnings
warnings.filterwarnings('ignore')

class AsyncEnhancedPreMarketMomentumStrategy:
    def __init__(self, config_path: str = None):
        """
        Async Enhanced Pre-Market Momentum Strategy optimized for 11k+ tickers
        
        Args:
            config_path: Path to configuration JSON file
        """
        # Optimized configuration for large ticker lists
        self.config = {
            "strategy_name": "Async Enhanced Pre-Market Gainers Momentum (Paper Trading)",
            "universe": {
                "sources": [
                    {"file": "nasdaqlisted.txt", "exchange": "NASDAQ"},
                    {"file": "otherlisted.txt", "exchange": "NYSE, AMEX, Regional"}
                ],
                "cache_enabled": True,  # Cache ticker universe
                "cache_ttl": 86400  # 24 hours
            },
            "filters": {
                "session": "pre-market",
                "percentage_change": {
                    "min": 3.0,
                    "max": 85.0,
                    "comparison_basis": "pre_market_open_price"
                },
                "ignore_below": 3.0,
                "sort": "descending_percentage_change",
                "min_volume": 10000,
                "relative_volume_min": 2.0,
                "early_filtering": True  # Filter during data fetch to reduce memory
            },
            "data_fetch": {
                "lookback_days": 5,
                "batch_size": 200,  # Larger batches for efficiency
                "max_concurrent_batches": 8,  # Concurrent batch processing
                "max_workers": 32,  # More workers for async operations
                "retries": 1,
                "retry_delay": 0.1,
                "delay_between_batches": 0.1,  # Reduced delay
                "timeout": 1,  # Request timeout
                "use_cache": True,  # Cache API responses
                "cache_duration": 300  # 5 minutes cache
            },
            "news_sentiment": {
                "enabled": False,
                "lookback_hours": 48,  # Reduced for performance
                "positive_threshold": 0.1,
                "max_concurrent": 20,  # Concurrent news fetching
                "retries": 2,
                "timeout": 10,
                "batch_news_fetch": True  # Fetch news in batches
            },
            "trade_rules": {
                "buy_time_ist": "16:00",
                "sell_time_ist": "16:10",
                "position_sizing": {
                    "type": "fixed_amount",
                    "amount_usd": 1000
                },
                "order_type": "market",
                "max_positions": 10  # Increased for large universe
            },
            "performance": {
                "memory_management": True,  # Enable memory optimization
                "chunk_processing": True,   # Process data in chunks
                "garbage_collection": True, # Force garbage collection
                "progress_reporting": 500   # Report progress every N tickers
            },
            "logging": {
                "fields": [
                    "ticker", "entry_price", "exit_price", "percentage_gain_loss",
                    "pnl_usd", "last_3_days_percentage_changes", "premarket_change",
                    "news_sentiment", "relative_volume", "market_cap"
                ],
                "level": "INFO",  # Reduced logging for performance
                "reporting": {
                    "frequency": "daily",
                    "cumulative_tracking": True
                }
            },
            "output_files": {
                "candidates": "trade_candidates.csv",
                "results": "trade_results.csv",
                "daily_log": "premarket_log.csv",
                "cache_dir": "cache"  # Cache directory
            }
        }
        
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                self.config.update(json.load(f))
        
        # Initialize components
        self.ist_tz = tz.gettz("Asia/Kolkata")
        self.analyzer = SentimentIntensityAnalyzer()
        self.setup_logging()
        
        # Create cache directory
        cache_dir = self.config["output_files"].get("cache_dir", "cache")
        os.makedirs(cache_dir, exist_ok=True)
        self.cache_dir = cache_dir
        
        # Initialize data storage
        self.universe_tickers = []
        self.daily_trades = []
        self.cumulative_pnl = 0.0
        self.processed_count = 0
        self.failed_tickers: Set[str] = set()
        
        # Performance tracking
        self.start_time = None
        self.stage_times = {}
        
        # Load universe asynchronously
        asyncio.create_task(self.load_universe_async()) if asyncio.get_event_loop().is_running() else None
    
    def setup_logging(self):
        """Setup optimized logging"""
        log_level = getattr(logging, self.config["logging"]["level"], logging.INFO)
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=[
                logging.FileHandler(f'async_momentum_strategy_{timestamp}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Initialized {self.config['strategy_name']}")
    
    def time_stage(self, stage_name: str):
        """Context manager for timing stages"""
        class StageTimer:
            def __init__(self, strategy, name):
                self.strategy = strategy
                self.name = name
                self.start = None
            
            def __enter__(self):
                self.start = time.time()
                self.strategy.logger.info(f"Starting {self.name}...")
                return self
            
            def __exit__(self, exc_type, exc_val, exc_tb):
                duration = time.time() - self.start
                self.strategy.stage_times[self.name] = duration
                self.strategy.logger.info(f"{self.name} completed in {duration:.2f}s")
        
        return StageTimer(self, stage_name)
    
    async def load_universe_async(self):
        """Asynchronously load ticker universe"""
        with self.time_stage("Universe Loading"):
            # Check cache first
            cache_file = os.path.join(self.cache_dir, "universe_cache.json")
            
            if (self.config["universe"]["cache_enabled"] and 
                os.path.exists(cache_file) and 
                time.time() - os.path.getmtime(cache_file) < self.config["universe"]["cache_ttl"]):
                
                with open(cache_file, 'r') as f:
                    self.universe_tickers = json.load(f)
                    self.logger.info(f"Loaded {len(self.universe_tickers)} tickers from cache")
                    return
            
            # Load from files
            all_tickers = []
            tasks = []
            
            for source in self.config["universe"]["sources"]:
                task = asyncio.create_task(self.read_tickers_from_file_async(source))
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    self.logger.error(f"Error loading tickers: {result}")
                else:
                    all_tickers.extend(result)
            
            # Remove duplicates and invalid tickers
            valid_tickers = []
            seen = set()
            
            for ticker in all_tickers:
                if ticker not in seen and self.is_valid_ticker(ticker):
                    valid_tickers.append(ticker)
                    seen.add(ticker)
            
            self.universe_tickers = valid_tickers
            
            # Cache the universe
            if self.config["universe"]["cache_enabled"]:
                with open(cache_file, 'w') as f:
                    json.dump(self.universe_tickers, f)
            
            self.logger.info(f"Loaded {len(self.universe_tickers)} unique valid tickers")
    
    async def read_tickers_from_file_async(self, source: Dict) -> List[str]:
        """Asynchronously read tickers from file"""
        path = source["file"]
        exchange = source["exchange"]
        
        if not os.path.exists(path):
            self.logger.warning(f"File not found: {path}")
            return []
        
        def read_file():
            tickers = []
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        
                        tokens = re.split(r'[\s,\t|;]+', line)
                        for tok in tokens:
                            tok = tok.strip().upper()
                            if (tok and tok not in ("SYMBOL", "TICKER", "EXCHANGE", "NAME") 
                                and re.match(r"^[A-Za-z0-9\.\-]{1,10}$", tok)):
                                tickers.append(tok)
                                break
                
                self.logger.info(f"Loaded {len(tickers)} tickers from {exchange}")
                return tickers
                
            except Exception as e:
                self.logger.error(f"Error reading {path}: {e}")
                return []
        
        # Run file reading in thread pool
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, read_file)
    
    def is_valid_ticker(self, ticker: str) -> bool:
        """Validate ticker symbol"""
        if not ticker or len(ticker) > 10:
            return False
        
        # Skip common invalid patterns
        invalid_patterns = [
            r'^TEST',  # Test symbols
            r'\d{4,}',  # Long numeric sequences
            r'^[0-9]+$',  # Pure numbers
            r'[^A-Z0-9\.\-]'  # Invalid characters
        ]
        
        return not any(re.search(pattern, ticker) for pattern in invalid_patterns)
    
    async def fetch_batch_data_async(self, batch: List[str], batch_num: int) -> List[Dict]:
        """Asynchronously fetch data for a batch of tickers"""
        results = []
        max_retries = self.config["data_fetch"]["retries"]
        
        for attempt in range(max_retries + 1):
            try:
                # Use thread pool for yahooquery calls
                loop = asyncio.get_event_loop()
                batch_data = await loop.run_in_executor(
                    None, 
                    partial(self.fetch_batch_data_sync, batch, batch_num)
                )
                
                results.extend(batch_data)
                break
                
            except Exception as e:
                if attempt < max_retries:
                    delay = self.config["data_fetch"]["retry_delay"] * (2 ** attempt)
                    await asyncio.sleep(delay)
                    self.logger.debug(f"Retrying batch {batch_num}, attempt {attempt + 1}")
                else:
                    self.logger.error(f"Failed batch {batch_num} after {max_retries} retries: {e}")
                    # Add failed tickers to set
                    self.failed_tickers.update(batch)
        
        return results
    
    def fetch_batch_data_sync(self, batch: List[str], batch_num: int) -> List[Dict]:
        """Synchronous batch data fetching (runs in thread pool)"""
        results = []
        now = datetime.now(timezone.utc).isoformat()
        
        try:
            symbols = " ".join(batch)
            ticker_obj = Ticker(symbols)
            
            # Get price data
            price_data = ticker_obj.price
            
            # Get historical data
            hist = ticker_obj.history(
                period=f"{self.config['data_fetch']['lookback_days']}d",
                interval="1d"
            )
            
            # Process each symbol
            for sym in batch:
                try:
                    result = self.process_ticker_data(sym, price_data, hist, now)
                    if result:
                        results.append(result)
                        
                        # Early filtering to save memory
                        if (self.config["filters"]["early_filtering"] and 
                            result.get("meets_filter", False)):
                            self.logger.debug(f"✓ {sym}: {result.get('pre_market_pct_calc', 0):.2f}%")
                        
                except Exception as e:
                    self.logger.debug(f"Error processing {sym}: {e}")
                    continue
                
                # Update progress
                self.processed_count += 1
                if self.processed_count % self.config["performance"]["progress_reporting"] == 0:
                    self.logger.info(f"Processed {self.processed_count} tickers...")
        
        except Exception as e:
            self.logger.error(f"Batch {batch_num} failed: {e}")
        
        return results
    
    def process_ticker_data(self, sym: str, price_data: Dict, hist, now: str) -> Optional[Dict]:
        """Process individual ticker data"""
        try:
            # Get price info
            if isinstance(price_data, dict):
                p = price_data.get(sym, {})
            else:
                p = {}
            
            if not isinstance(p, dict):
                return None
            
            # Extract key data
            pre_price = p.get("preMarketPrice")
            prev_close = p.get("regularMarketPreviousClose") or p.get("previousClose")
            market_state = p.get("marketState")
            market_cap = p.get("marketCap")
            pre_volume = p.get("preMarketVolume", 0)
            avg_volume = p.get("averageVolume", 1)
            current_price = p.get("regularMarketPrice", pre_price)
            
            # Quick filter for minimum requirements
            if not current_price or current_price < self.config["filters"]["ignore_below"]:
                return None
            
            # Get historical closes
            closes = self.extract_closes(sym, hist)
            last3_pct_changes = self.compute_close_to_close_pct(closes, 3)
            
            # Calculate premarket percentage
            pre_market_pct = None
            if pre_price is not None and prev_close not in (None, 0):
                try:
                    pre_market_pct = (float(pre_price) - float(prev_close)) / float(prev_close) * 100.0
                except (ValueError, TypeError):
                    pre_market_pct = None
            
            # Early exit if doesn't meet percentage criteria
            if pre_market_pct is None:
                return None
                
            min_pct = self.config["filters"]["percentage_change"]["min"]
            max_pct = self.config["filters"]["percentage_change"]["max"]
            
            if not (min_pct <= pre_market_pct <= max_pct):
                return None
            
            # Calculate relative volume
            relative_volume = pre_volume / max(avg_volume, 1) if pre_volume and avg_volume else 0
            
            # Apply all filters
            min_volume = self.config["filters"].get("min_volume", 0)
            min_rel_vol = self.config["filters"].get("relative_volume_min", 0)
            
            meets_filter = (
                pre_market_pct is not None and
                min_pct <= pre_market_pct <= max_pct and
                current_price >= self.config["filters"]["ignore_below"] and
                (pre_volume or 0) >= min_volume and
                relative_volume >= min_rel_vol
            )
            
            return {
                "timestamp_utc": now,
                "ticker": sym,
                "market_state": market_state,
                "pre_market_price": pre_price,
                "current_price": current_price,
                "pre_market_pct_calc": pre_market_pct,
                "pre_market_volume": pre_volume,
                "average_volume": avg_volume,
                "relative_volume": relative_volume,
                "regular_prev_close": prev_close,
                "market_cap": market_cap,
                "last_3_days_pct_changes": last3_pct_changes,
                "meets_filter": meets_filter
            }
            
        except Exception as e:
            self.logger.debug(f"Error processing {sym}: {e}")
            return None
    
    def extract_closes(self, sym: str, hist) -> pd.Series:
        """Extract close prices from historical data"""
        try:
            if isinstance(hist, pd.DataFrame):
                if isinstance(hist.index, pd.MultiIndex):
                    if sym in hist.index.levels[0]:
                        df_sym = hist.xs(sym, level=0)
                        if "close" in df_sym.columns:
                            return df_sym["close"].sort_index()
                else:
                    if "close" in hist.columns:
                        return hist["close"].sort_index()
            elif isinstance(hist, dict):
                sym_hist = hist.get(sym)
                if sym_hist is not None:
                    df_sym = pd.DataFrame(sym_hist)
                    if "close" in df_sym.columns:
                        return df_sym.set_index("date")["close"].sort_index()
        except Exception:
            pass
        
        return pd.Series(dtype=float)
    
    def compute_close_to_close_pct(self, close_series: pd.Series, n_changes: int = 3) -> List[float]:
        """Compute percentage changes from close prices"""
        closes = close_series.dropna()
        if len(closes) < (n_changes + 1):
            return []
        
        pct_changes = (closes.pct_change().dropna() * 100).tolist()
        return pct_changes[-n_changes:] if len(pct_changes) >= n_changes else []
    
    async def scan_premarket_async(self) -> pd.DataFrame:
        """Asynchronously scan pre-market data"""
        with self.time_stage("Pre-market Scan"):
            if not self.universe_tickers:
                await self.load_universe_async()
            
            total_tickers = len(self.universe_tickers)
            batch_size = self.config["data_fetch"]["batch_size"]
            max_concurrent = self.config["data_fetch"]["max_concurrent_batches"]
            
            self.logger.info(f"Starting async premarket scan for {total_tickers} tickers...")
            self.logger.info(f"Batch size: {batch_size}, Max concurrent batches: {max_concurrent}")
            
            # Create batches
            batches = [
                self.universe_tickers[i:i + batch_size] 
                for i in range(0, total_tickers, batch_size)
            ]
            
            all_results = []
            semaphore = asyncio.Semaphore(max_concurrent)
            
            async def process_batch_with_semaphore(batch, batch_num):
                async with semaphore:
                    results = await self.fetch_batch_data_async(batch, batch_num)
                    # Add small delay between batches
                    await asyncio.sleep(self.config["data_fetch"]["delay_between_batches"])
                    return results
            
            # Process batches with limited concurrency
            tasks = [
                process_batch_with_semaphore(batch, i + 1) 
                for i, batch in enumerate(batches)
            ]
            
            # Process in chunks to manage memory
            chunk_size = max_concurrent * 2
            for i in range(0, len(tasks), chunk_size):
                chunk_tasks = tasks[i:i + chunk_size]
                chunk_results = await asyncio.gather(*chunk_tasks, return_exceptions=True)
                
                for result in chunk_results:
                    if isinstance(result, Exception):
                        self.logger.error(f"Batch processing error: {result}")
                    else:
                        all_results.extend(result)
                
                # Memory management
                if self.config["performance"]["garbage_collection"]:
                    import gc
                    gc.collect()
                
                progress = min(i + chunk_size, len(tasks))
                self.logger.info(f"Completed {progress}/{len(tasks)} batch chunks")
            
            # Create DataFrame
            df = pd.DataFrame(all_results)
            
            if not df.empty:
                # Filter for candidates
                candidates = df[df['meets_filter']].copy()
                self.logger.info(f"Found {len(candidates)} candidates out of {len(df)} processed tickers")
                
                if self.failed_tickers:
                    self.logger.warning(f"Failed to process {len(self.failed_tickers)} tickers")
            else:
                candidates = df
                self.logger.warning("No data retrieved from any ticker")
            
            return df
    
    async def fetch_news_sentiment_async(self, ticker: str) -> Tuple[str, Optional[float], bool, int]:
        """Asynchronously fetch and analyze news sentiment"""
        hours_back = self.config["news_sentiment"]["lookback_hours"]
        threshold = self.config["news_sentiment"]["positive_threshold"]
        
        cutoff = datetime.utcnow().timestamp() - hours_back * 3600
        
        try:
            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                partial(self.fetch_news_sync, ticker, cutoff, threshold)
            )
            return result
            
        except Exception as e:
            self.logger.debug(f"Failed to fetch news for {ticker}: {e}")
            return (ticker, None, False, 0)
    
    def fetch_news_sync(self, ticker: str, cutoff: float, threshold: float) -> Tuple[str, Optional[float], bool, int]:
        """Synchronous news fetching (runs in thread pool)"""
        try:
            t = Ticker(ticker)
            items = t.news or []
            titles = []
            
            for item in items:
                pub_time = item.get("providerPublishTime")
                if isinstance(pub_time, (int, float)) and pub_time >= cutoff:
                    title = item.get("title", "").strip()
                    if title:
                        titles.append(title)
            
            avg_sentiment = self.score_headlines_vader(titles)
            positive = (avg_sentiment is not None) and (avg_sentiment > threshold)
            
            return (ticker, avg_sentiment, positive, len(titles))
            
        except Exception as e:
            return (ticker, None, False, 0)
    
    def score_headlines_vader(self, titles: List[str]) -> Optional[float]:
        """Score news headlines using VADER sentiment"""
        if not titles:
            return None
        
        scores = []
        for title in titles:
            if title and isinstance(title, str):
                score = self.analyzer.polarity_scores(title)["compound"]
                scores.append(score)
        
        return sum(scores) / len(scores) if scores else None
    
    async def enrich_with_news_sentiment_async(self, df_filtered: pd.DataFrame) -> pd.DataFrame:
        """Asynchronously add news sentiment analysis"""
        if df_filtered.empty or not self.config["news_sentiment"]["enabled"]:
            df_filtered = df_filtered.copy()
            df_filtered["news_avg_compound"] = None
            df_filtered["news_articles_used"] = 0
            df_filtered["positive_news"] = False
            return df_filtered
        
        with self.time_stage("News Sentiment Analysis"):
            symbols = df_filtered["ticker"].tolist()
            max_concurrent = self.config["news_sentiment"]["max_concurrent"]
            
            self.logger.info(f"Analyzing news sentiment for {len(symbols)} candidates...")
            
            # Create semaphore for concurrency control
            semaphore = asyncio.Semaphore(max_concurrent)
            
            async def fetch_with_semaphore(ticker):
                async with semaphore:
                    return await self.fetch_news_sentiment_async(ticker)
            
            # Process news sentiment
            tasks = [fetch_with_semaphore(sym) for sym in symbols]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            news_data = []
            for result in results:
                if isinstance(result, Exception):
                    self.logger.debug(f"News sentiment error: {result}")
                    continue
                
                ticker, avg, positive, count = result
                news_data.append({
                    "ticker": ticker,
                    "news_avg_compound": avg,
                    "positive_news": positive,
                    "news_articles_used": count
                })
                
                if positive:
                    self.logger.info(f"✓ {ticker}: Positive news sentiment ({avg:.3f}, {count} articles)")
            
            # Merge news data
            if news_data:
                news_df = pd.DataFrame(news_data)
                return df_filtered.merge(news_df, on="ticker", how="left")
            else:
                # No news data available
                df_filtered["news_avg_compound"] = None
                df_filtered["news_articles_used"] = 0
                df_filtered["positive_news"] = False
                return df_filtered
    
    def execute_paper_trade(self, stock_data: Dict) -> Dict:
        """Execute paper trade with realistic simulation"""
        ticker = stock_data['ticker']
        entry_price = stock_data.get('pre_market_price') or stock_data.get('current_price')
        
        if not entry_price:
            self.logger.warning(f"No entry price for {ticker}")
            return None
        
        position_size = self.config["trade_rules"]["position_sizing"]["amount_usd"]
        shares = int(position_size / entry_price)
        
        # Simulate exit price
        try:
            momentum_factor = stock_data.get('pre_market_pct_calc', 0) / 100
            decay_factor = np.random.normal(-0.3, 0.4)
            news_boost = 0.1 if stock_data.get('positive_news', False) else 0
            
            exit_price = entry_price * (1 + momentum_factor * decay_factor + news_boost)
            exit_price = max(exit_price, entry_price * 0.85)  # Stop loss
            
        except Exception:
            exit_price = entry_price * 0.97
        
        # Calculate P&L
        pnl = (exit_price - entry_price) * shares
        pnl_percentage = ((exit_price - entry_price) / entry_price) * 100
        
        trade_result = {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'timestamp': datetime.now().isoformat(),
            'ticker': ticker,
            'entry_price': round(entry_price, 2),
            'exit_price': round(exit_price, 2),
            'shares': shares,
            'percentage_gain_loss': round(pnl_percentage, 2),
            'pnl_usd': round(pnl, 2),
            'last_3_days_percentage_changes': stock_data.get('last_3_days_pct_changes', []),
            'premarket_change': round(stock_data.get('pre_market_pct_calc', 0), 2),
            'relative_volume': round(stock_data.get('relative_volume', 1), 2),
            'market_cap': stock_data.get('market_cap'),
            'news_sentiment': stock_data.get('news_avg_compound'),
            'positive_news': stock_data.get('positive_news', False),
            'news_articles': stock_data.get('news_articles_used', 0)
        }
        
        self.daily_trades.append(trade_result)
        self.cumulative_pnl += pnl
        
        self.logger.info(f"Trade: {ticker} - P&L: ${pnl:.2f} ({pnl_percentage:.2f}%)")
        
        return trade_result
    
    async def run_analysis_async(self) -> pd.DataFrame:
        """Run complete analysis pipeline asynchronously"""
        self.start_time = time.time()
        
        with self.time_stage("Complete Analysis"):
            # Scan pre-market
            df_all = await self.scan_premarket_async()
            
            # Filter candidates
            df_filtered = df_all[df_all["meets_filter"]].copy() if not df_all.empty else pd.DataFrame()
            
            if df_filtered.empty:
                self.logger.info("No candidates found matching criteria")
                return df_filtered
            
            # Sort by pre-market percentage change
            df_filtered = df_filtered.sort_values('pre_market_pct_calc', ascending=False)
            
            # Enrich with news sentiment
            df_enriched = await self.enrich_with_news_sentiment_async(df_filtered)
            
            # Final filtering - require positive news
            df_final = df_enriched[df_enriched["positive_news"]].copy()
            
            # Save candidates
            candidates_file = self.config["output_files"]["candidates"]
            df_final.to_csv(candidates_file, index=False)
            
            self.logger.info(f"Analysis complete. {len(df_final)} final candidates")
            self.print_performance_summary()
            
            return df_final
    
    def print_performance_summary(self):
        """Print performance summary"""
        total_time = time.time() - self.start_time if self.start_time else 0
        
        self.logger.info("=== PERFORMANCE SUMMARY ===")
        self.logger.info(f"Total execution time: {total_time:.2f}s")
        self.logger.info(f"Tickers processed: {self.processed_count}")
        self.logger.info(f"Processing rate: {self.processed_count/max(total_time, 1):.1f} tickers/second")
        
        if self.failed_tickers:
            self.logger.info(f"Failed tickers: {len(self.failed_tickers)}")
        
        for stage, duration in self.stage_times.items():
            self.logger.info(f"{stage}: {duration:.2f}s")
    
    async def run_paper_trades_async(self, candidates_df: pd.DataFrame = None) -> List[Dict]:
        """Execute paper trades asynchronously"""
        if candidates_df is None:
            candidates_file = self.config["output_files"]["candidates"]
            if not os.path.exists(candidates_file):
                self.logger.error("No candidates file found. Run analysis first.")
                return []
            candidates_df = pd.read_csv(candidates_file)
        
        if candidates_df.empty:
            self.logger.info("No candidates available for trading")
            return []
        
        max_positions = self.config["trade_rules"]["max_positions"]
        selected_candidates = candidates_df.head(max_positions)
        
        self.logger.info(f"Executing paper trades for {len(selected_candidates)} positions")
        
        # Execute trades in parallel
        loop = asyncio.get_event_loop()
        tasks = []
        
        for _, row in selected_candidates.iterrows():
            task = loop.run_in_executor(None, self.execute_paper_trade, row.to_dict())
            tasks.append(task)
        
        trade_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and None results
        trades = [result for result in trade_results 
                 if not isinstance(result, Exception) and result is not None]
        
        # Save results
        if trades:
            results_df = pd.DataFrame(trades)
            results_file = self.config["output_files"]["results"]
            results_df.to_csv(results_file, index=False)
            
            # Log summary
            total_pnl = sum(t['pnl_usd'] for t in trades)
            winners = sum(1 for t in trades if t['pnl_usd'] > 0)
            win_rate = (winners / len(trades)) * 100
            
            self.logger.info(f"Paper trades completed:")
            self.logger.info(f"  Total trades: {len(trades)}")
            self.logger.info(f"  Winners: {winners} ({win_rate:.1f}%)")
            self.logger.info(f"  Total P&L: ${total_pnl:.2f}")
            self.logger.info(f"  Results saved to: {results_file}")
        
        return trades
    
    async def run_complete_strategy_async(self):
        """Run the complete strategy pipeline asynchronously"""
        try:
            self.logger.info(f"Starting {self.config['strategy_name']}")
            self.logger.info(f"Target universe: {len(self.universe_tickers) if self.universe_tickers else 'Loading...'} tickers")
            
            # Run analysis
            candidates = await self.run_analysis_async()
            
            if not candidates.empty:
                # Execute trades
                trades = await self.run_paper_trades_async(candidates)
                
                return {
                    'candidates': len(candidates),
                    'trades_executed': len(trades),
                    'total_pnl': sum(t['pnl_usd'] for t in trades) if trades else 0,
                    'processing_time': time.time() - self.start_time if self.start_time else 0
                }
            else:
                self.logger.info("No candidates found today")
                return {
                    'candidates': 0, 
                    'trades_executed': 0, 
                    'total_pnl': 0,
                    'processing_time': time.time() - self.start_time if self.start_time else 0
                }
                
        except KeyboardInterrupt:
            self.logger.info("Strategy interrupted by user")
            return None
        except Exception as e:
            self.logger.error(f"Strategy failed: {e}")
            raise


async def main_async():
    """Main execution function - async version"""
    # Initialize strategy
    strategy = AsyncEnhancedPreMarketMomentumStrategy()
    
    # Load universe first
    await strategy.load_universe_async()
    
    print(f"\n=== {strategy.config['strategy_name']} ===")
    print(f"Universe size: {len(strategy.universe_tickers)} tickers")
    print(f"Target: {strategy.config['filters']['percentage_change']['min']}-{strategy.config['filters']['percentage_change']['max']}% premarket gainers")
    print(f"News sentiment: {'Enabled' if strategy.config['news_sentiment']['enabled'] else 'Disabled'}")
    print(f"Max positions: {strategy.config['trade_rules']['max_positions']}")
    print(f"Batch size: {strategy.config['data_fetch']['batch_size']}")
    print(f"Max concurrent batches: {strategy.config['data_fetch']['max_concurrent_batches']}")
    print("="*60)
    
    # Run complete strategy
    results = await strategy.run_complete_strategy_async()
    
    if results:
        print(f"\n=== DAILY SUMMARY ===")
        print(f"Candidates found: {results['candidates']}")
        print(f"Trades executed: {results['trades_executed']}")
        print(f"Total P&L: ${results['total_pnl']:.2f}")
        print(f"Cumulative P&L: ${strategy.cumulative_pnl:.2f}")
        print(f"Processing time: {results['processing_time']:.2f}s")
        print(f"Tickers/second: {len(strategy.universe_tickers)/max(results['processing_time'], 1):.1f}")


def main():
    """Main entry point - handles async execution"""
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\nStrategy interrupted by user")
    except Exception as e:
        print(f"Error running strategy: {e}")


if __name__ == "__main__":
    main()