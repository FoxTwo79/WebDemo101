import os
import re
import time
import json
import logging
from datetime import datetime, time as dtime, timezone, timedelta
from dateutil import tz
from typing import List, Optional, Tuple, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np
from yahooquery import Ticker
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

class EnhancedPreMarketMomentumStrategy:
    def __init__(self, config_path: str = None):
        """
        Enhanced Pre-Market Momentum Strategy combining both approaches
        
        Args:
            config_path: Path to configuration JSON file
        """
        # Default configuration
        self.config = {
            "strategy_name": "Enhanced Pre-Market Gainers Momentum (Paper Trading)",
            "universe": {
                "sources": [
                    {"file": "nasdaqlisted.txt", "exchange": "NASDAQ"},
                    {"file": "otherlisted.txt", "exchange": "NYSE, AMEX, Regional"}
                ]
            },
            "filters": {
                "session": "pre-market",
                "percentage_change": {
                    "min": 5.0,
                    "max": 15.0,
                    "comparison_basis": "pre_market_open_price"
                },
                "ignore_below": 5.0,
                "sort": "descending_percentage_change",
                "min_volume": 10000,  # Minimum pre-market volume
                "relative_volume_min": 2.0  # At least 2x average volume
            },
            "data_fetch": {
                "lookback_days": 5,
                "batch_size": 80,
                "max_workers": 16,
                "retries": 2,
                "delay_between_batches": 0.5
            },
            "news_sentiment": {
                "enabled": True,
                "lookback_hours": 108,
                "positive_threshold": 0.1,
                "max_workers": 16,
                "retries": 2
            },
            "trade_rules": {
                "buy_time_ist": "18:00",
                "sell_time_ist": "18:55",
                "position_sizing": {
                    "type": "fixed_amount",
                    "amount_usd": 1000
                },
                "order_type": "market",
                "max_positions": 5
            },
            "logging": {
                "fields": [
                    "ticker", "entry_price", "exit_price", "percentage_gain_loss",
                    "pnl_usd", "last_3_days_percentage_changes", "premarket_change",
                    "news_sentiment", "relative_volume", "market_cap"
                ],
                "reporting": {
                    "frequency": "daily",
                    "cumulative_tracking": True
                }
            },
            "output_files": {
                "candidates": "trade_candidates.csv",
                "results": "trade_results.csv",
                "daily_log": "premarket_log.csv"
            }
        }
        
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                self.config.update(json.load(f))
        
        # Initialize components
        self.ist_tz = tz.gettz("Asia/Kolkata")
        self.analyzer = SentimentIntensityAnalyzer()
        self.setup_logging()
        
        # Initialize data storage
        self.universe_tickers = []
        self.daily_trades = []
        self.cumulative_pnl = 0.0
        
        # Load universe
        self.load_universe()
    
    def setup_logging(self):
        """Setup comprehensive logging"""
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.FileHandler(f'momentum_strategy_{timestamp}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Initialized {self.config['strategy_name']}")
    
    def get_current_ist(self):
        """Get current IST time"""
        return datetime.now(self.ist_tz)
    
    def read_tickers_from_file(self, path: str) -> List[str]:
        """Enhanced ticker reading with regex pattern matching"""
        if not os.path.exists(path):
            self.logger.warning(f"File not found: {path}")
            return []
        
        tickers = []
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    
                    # Handle different delimiters
                    tokens = re.split(r'[\s,\t|;]+', line)
                    found = None
                    
                    for tok in tokens:
                        tok = tok.strip()
                        if not tok:
                            continue
                        # Skip header words
                        if tok.lower() in ("symbol", "ticker", "exchange", "name"):
                            continue
                        # Match valid ticker pattern
                        if re.match(r"^[A-Za-z0-9\.\-]{1,10}$", tok):
                            found = tok.upper()
                            break
                    
                    if found:
                        tickers.append(found)
            
            self.logger.info(f"Loaded {len(tickers)} tickers from {path}")
            
        except Exception as e:
            self.logger.error(f"Error reading {path}: {e}")
        
        return tickers
    
    def load_universe(self):
        """Load ticker universe from multiple sources"""
        all_tickers = []
        
        for source in self.config["universe"]["sources"]:
            file_path = source["file"]
            exchange = source["exchange"]
            tickers = self.read_tickers_from_file(file_path)
            all_tickers.extend(tickers)
            self.logger.info(f"Loaded {len(tickers)} from {exchange}")
        
        # Remove duplicates while preserving order
        self.universe_tickers = list(dict.fromkeys(all_tickers))
        self.logger.info(f"Total universe: {len(self.universe_tickers)} unique tickers")
    
    def chunk_list(self, lst: List, n: int):
        """Split list into chunks"""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]
    
    def compute_close_to_close_pct(self, close_series: pd.Series, n_changes: int = 3) -> List[float]:
        """Compute percentage changes from close prices"""
        closes = close_series.dropna()
        if len(closes) < (n_changes + 1):
            return []
        
        pct_changes = (closes.pct_change().dropna() * 100).tolist()
        return pct_changes[-n_changes:] if len(pct_changes) >= n_changes else []
    
    def scan_premarket(self) -> pd.DataFrame:
        """Enhanced pre-market scanning with better error handling"""
        results = []
        now = datetime.now(timezone.utc).isoformat()
        total_tickers = len(self.universe_tickers)
        batch_size = self.config["data_fetch"]["batch_size"]
        
        self.logger.info(f"Starting premarket scan for {total_tickers} tickers...")
        
        chunk_num = 0
        processed_tickers = 0
        
        for chunk in self.chunk_list(self.universe_tickers, batch_size):
            chunk_num += 1
            remaining = total_tickers - processed_tickers
            
            self.logger.info(f"Processing chunk {chunk_num}: {len(chunk)} tickers. "
                           f"{processed_tickers}/{total_tickers} done, {remaining} remaining")
            
            try:
                symbols = " ".join(chunk)
                ticker_obj = Ticker(symbols)
                
                # Get price data
                price_data = ticker_obj.price
                
                # Get historical data
                hist = ticker_obj.history(
                    period=f"{self.config['data_fetch']['lookback_days']}d",
                    interval="1d"
                )
                
                for sym in chunk:
                    processed_tickers += 1
                    
                    try:
                        # Get price info for this symbol
                        if isinstance(price_data, dict):
                            p = price_data.get(sym, {})
                        else:
                            p = {}
                        
                        if not isinstance(p, dict):
                            continue
                        
                        # Extract key data
                        pre_price = p.get("preMarketPrice")
                        prev_close = p.get("regularMarketPreviousClose") or p.get("previousClose")
                        market_state = p.get("marketState")
                        market_cap = p.get("marketCap")
                        pre_volume = p.get("preMarketVolume", 0)
                        avg_volume = p.get("averageVolume", 1)
                        current_price = p.get("regularMarketPrice", pre_price)
                        
                        # Get historical closes
                        closes = pd.Series(dtype=float)
                        try:
                            if isinstance(hist, pd.DataFrame):
                                if isinstance(hist.index, pd.MultiIndex):
                                    if sym in hist.index.levels[0]:
                                        df_sym = hist.xs(sym, level=0)
                                        if "close" in df_sym.columns:
                                            closes = df_sym["close"].sort_index()
                                else:
                                    if "close" in hist.columns:
                                        closes = hist["close"].sort_index()
                            elif isinstance(hist, dict):
                                sym_hist = hist.get(sym)
                                if sym_hist is not None:
                                    df_sym = pd.DataFrame(sym_hist)
                                    if "close" in df_sym.columns:
                                        closes = df_sym.set_index("date")["close"].sort_index()
                        except Exception as e:
                            self.logger.debug(f"Error processing historical data for {sym}: {e}")
                            closes = pd.Series(dtype=float)
                        
                        # Calculate metrics
                        last3_pct_changes = self.compute_close_to_close_pct(closes, 3)
                        
                        pre_market_pct = None
                        if pre_price is not None and prev_close not in (None, 0):
                            try:
                                pre_market_pct = (float(pre_price) - float(prev_close)) / float(prev_close) * 100.0
                            except (ValueError, TypeError):
                                pre_market_pct = None
                        
                        # Calculate relative volume
                        relative_volume = pre_volume / max(avg_volume, 1) if pre_volume and avg_volume else 0
                        
                        # Apply filters
                        min_pct = self.config["filters"]["percentage_change"]["min"]
                        max_pct = self.config["filters"]["percentage_change"]["max"]
                        min_price = self.config["filters"]["ignore_below"]
                        min_volume = self.config["filters"].get("min_volume", 0)
                        min_rel_vol = self.config["filters"].get("relative_volume_min", 0)
                        
                        meets_filter = (
                            pre_market_pct is not None and
                            min_pct <= pre_market_pct <= max_pct and
                            (current_price or 0) >= min_price and
                            (pre_volume or 0) >= min_volume and
                            relative_volume >= min_rel_vol
                        )
                        
                        results.append({
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
                        })
                        
                        if meets_filter:
                            self.logger.info(f"✓ {sym}: {pre_market_pct:.2f}% premarket gain")
                    
                    except Exception as e:
                        self.logger.debug(f"Error processing {sym}: {e}")
                        continue
                
                # Delay between batches
                time.sleep(self.config["data_fetch"]["delay_between_batches"])
                
            except Exception as e:
                self.logger.error(f"Error processing chunk {chunk_num}: {e}")
                continue
        
        df = pd.DataFrame(results)
        self.logger.info(f"Premarket scan complete. Found {len(df[df['meets_filter']])} candidates.")
        return df
    
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
    
    def fetch_and_score_news(self, ticker: str) -> Tuple[str, Optional[float], bool, int]:
        """Fetch and score news for a ticker"""
        hours_back = self.config["news_sentiment"]["lookback_hours"]
        retries = self.config["news_sentiment"]["retries"]
        threshold = self.config["news_sentiment"]["positive_threshold"]
        
        cutoff = datetime.utcnow().timestamp() - hours_back * 3600
        
        for attempt in range(retries + 1):
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
                if attempt < retries:
                    time.sleep(0.2)
                else:
                    self.logger.debug(f"Failed to fetch news for {ticker}: {e}")
        
        return (ticker, None, False, 0)
    
    def enrich_with_news_sentiment(self, df_filtered: pd.DataFrame) -> pd.DataFrame:
        """Add news sentiment analysis to filtered candidates"""
        if df_filtered.empty or not self.config["news_sentiment"]["enabled"]:
            df_filtered = df_filtered.copy()
            df_filtered["news_avg_compound"] = None
            df_filtered["news_articles_used"] = 0
            df_filtered["positive_news"] = False
            return df_filtered
        
        self.logger.info(f"Analyzing news sentiment for {len(df_filtered)} candidates...")
        
        symbols = df_filtered["ticker"].tolist()
        results = []
        
        max_workers = self.config["news_sentiment"]["max_workers"]
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_ticker = {
                executor.submit(self.fetch_and_score_news, sym): sym 
                for sym in symbols
            }
            
            for future in as_completed(future_to_ticker):
                ticker, avg, positive, count = future.result()
                results.append((ticker, avg, positive, count))
                
                if positive:
                    self.logger.info(f"✓ {ticker}: Positive news sentiment ({avg:.3f}, {count} articles)")
        
        # Merge news data
        news_df = pd.DataFrame(results, columns=[
            "ticker", "news_avg_compound", "positive_news", "news_articles_used"
        ])
        
        return df_filtered.merge(news_df, on="ticker", how="left")
    
    def execute_paper_trade(self, stock_data: Dict) -> Dict:
        """Execute paper trade with realistic simulation"""
        ticker = stock_data['ticker']
        entry_price = stock_data.get('pre_market_price') or stock_data.get('current_price')
        
        if not entry_price:
            self.logger.warning(f"No entry price for {ticker}")
            return None
        
        position_size = self.config["trade_rules"]["position_sizing"]["amount_usd"]
        shares = int(position_size / entry_price)
        
        self.logger.info(f"Paper trading {ticker}: Entry ${entry_price:.2f}, Shares: {shares}")
        
        # Simulate exit price with realistic modeling
        try:
            # Get fresh data for exit simulation
            t = Ticker(ticker)
            current_data = t.price.get(ticker, {})
            current_price = current_data.get("regularMarketPrice", entry_price)
            
            # Model typical momentum decay with some randomness
            momentum_factor = stock_data.get('pre_market_pct_calc', 0) / 100
            
            # Simulate mean reversion with volatility
            # High momentum stocks tend to pull back during regular hours
            decay_factor = np.random.normal(-0.3, 0.4)  # Average pullback with volatility
            news_boost = 0.1 if stock_data.get('positive_news', False) else 0
            
            exit_price = entry_price * (1 + momentum_factor * decay_factor + news_boost)
            exit_price = max(exit_price, entry_price * 0.85)  # Stop loss at -15%
            
        except Exception as e:
            self.logger.debug(f"Error simulating exit for {ticker}: {e}")
            # Conservative fallback
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
        
        self.logger.info(f"Trade completed: {ticker} - P&L: ${pnl:.2f} ({pnl_percentage:.2f}%)")
        
        return trade_result
    
    def run_analysis(self) -> pd.DataFrame:
        """Run complete analysis pipeline"""
        self.logger.info("Starting comprehensive market analysis...")
        
        # Scan pre-market
        df_all = self.scan_premarket()
        
        # Filter candidates
        df_filtered = df_all[df_all["meets_filter"]].copy()
        
        if df_filtered.empty:
            self.logger.info("No candidates found matching criteria")
            return df_filtered
        
        # Sort by pre-market percentage change
        df_filtered = df_filtered.sort_values('pre_market_pct_calc', ascending=False)
        
        # Enrich with news sentiment
        df_enriched = self.enrich_with_news_sentiment(df_filtered)
        
        # Final filtering - require positive news
        df_final = df_enriched[df_enriched["positive_news"]].copy()
        
        # Save candidates
        candidates_file = self.config["output_files"]["candidates"]
        df_final.to_csv(candidates_file, index=False)
        
        self.logger.info(f"Analysis complete. {len(df_final)} final candidates saved to {candidates_file}")
        
        return df_final
    
    def run_paper_trades(self, candidates_df: pd.DataFrame = None) -> List[Dict]:
        """Execute paper trades on candidates"""
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
        
        trades = []
        for _, row in selected_candidates.iterrows():
            trade_result = self.execute_paper_trade(row.to_dict())
            if trade_result:
                trades.append(trade_result)
        
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
    
    def wait_for_trading_time(self):
        """Wait until trading time"""
        now_ist = self.get_current_ist()
        buy_time_str = self.config["trade_rules"]["buy_time_ist"]
        
        # Parse buy time
        hour, minute = map(int, buy_time_str.split(':'))
        buy_time = dtime(hour, minute)
        
        # Calculate wait time
        target_datetime = datetime.combine(now_ist.date(), buy_time, tzinfo=self.ist_tz)
        
        # If time has passed, target next day
        if now_ist.time() > buy_time:
            target_datetime += timedelta(days=1)
        
        wait_seconds = (target_datetime - now_ist).total_seconds()
        
        if wait_seconds > 0:
            self.logger.info(f"Waiting {wait_seconds/60:.1f} minutes until trading time ({buy_time_str} IST)...")
            time.sleep(wait_seconds)
    
    def run_complete_strategy(self):
        """Run the complete strategy pipeline"""
        try:
            self.logger.info(f"Starting {self.config['strategy_name']}")
            
            # Run analysis
            candidates = self.run_analysis()
            
            if not candidates.empty:
                # Wait for trading time if needed
                # self.wait_for_trading_time()  # Uncomment for live trading
                
                # Execute trades
                trades = self.run_paper_trades(candidates)
                
                return {
                    'candidates': len(candidates),
                    'trades_executed': len(trades),
                    'total_pnl': sum(t['pnl_usd'] for t in trades) if trades else 0
                }
            else:
                self.logger.info("No candidates found today")
                return {'candidates': 0, 'trades_executed': 0, 'total_pnl': 0}
                
        except KeyboardInterrupt:
            self.logger.info("Strategy interrupted by user")
            return None
        except Exception as e:
            self.logger.error(f"Strategy failed: {e}")
            raise


def main():
    """Main execution function"""
    # Initialize strategy
    strategy = EnhancedPreMarketMomentumStrategy()
    
    print(f"\n=== {strategy.config['strategy_name']} ===")
    print(f"Universe size: {len(strategy.universe_tickers)} tickers")
    print(f"Target: {strategy.config['filters']['percentage_change']['min']}-{strategy.config['filters']['percentage_change']['max']}% premarket gainers")
    print(f"News sentiment: {'Enabled' if strategy.config['news_sentiment']['enabled'] else 'Disabled'}")
    print(f"Max positions: {strategy.config['trade_rules']['max_positions']}")
    print("="*50)
    
    # Run complete strategy
    results = strategy.run_complete_strategy()
    
    if results:
        print(f"\n=== DAILY SUMMARY ===")
        print(f"Candidates found: {results['candidates']}")
        print(f"Trades executed: {results['trades_executed']}")
        print(f"Total P&L: ${results['total_pnl']:.2f}")
        print(f"Cumulative P&L: ${strategy.cumulative_pnl:.2f}")


if __name__ == "__main__":
    main()