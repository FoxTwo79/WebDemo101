# working_main_trader.py
import os
import re
import asyncio
import pytz
import pandas as pd
from datetime import datetime, time as dtime, timedelta
from typing import List, Dict, Optional
import concurrent.futures
import threading

# --- Third-party libraries ---
from polygon import RESTClient
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------

# --- API Key ---
POLYGON_API_KEY = "TtQDKmwPXtJ7A078n9cwckbHNpEO_eUe"

# --- File Paths ---
NASDAQ_FILE = "nasdaqlisted.txt"
OTHER_FILE = "otherlisted.txt"
CANDIDATES_FILE = "trade_candidates.csv"
RESULTS_FILE = "trade_results.csv"

# --- Strategy Filters ---
MIN_PCT_CHANGE = 5.0
MAX_PCT_CHANGE = 15.0
MIN_PREMARKET_VOLUME = 75000
MIN_MARKET_CAP_USD = 300000000

# --- News Filter Control ---
ENABLE_NEWS_FILTER = False  # Disabled for initial testing
NEWS_POSITIVE_THRESHOLD = 0.1

# --- Trading Simulation Parameters ---
ET_TIMEZONE = pytz.timezone("US/Eastern")
BUY_TIME_ET = dtime(9, 31)
SELL_TIME_ET = dtime(15, 55)
POSITION_SIZE_USD = 1000

# --- Rate Limiting ---
BATCH_SIZE = 20  # Smaller batches for API limits
DELAY_BETWEEN_REQUESTS = 0.2  # Seconds between individual requests
MAX_WORKERS = 10  # Number of concurrent threads

# --- Global Objects ---
VADER_ANALYZER = SentimentIntensityAnalyzer()

# Thread-local storage for Polygon clients
thread_local = threading.local()

def get_client():
    """Get a thread-local Polygon client instance."""
    if not hasattr(thread_local, 'client'):
        thread_local.client = RESTClient(POLYGON_API_KEY)
    return thread_local.client

# -----------------------------------------------------------------------------
# HELPER FUNCTIONS
# -----------------------------------------------------------------------------

def get_current_et_time() -> datetime:
    """Gets the current time in the US/Eastern timezone."""
    return datetime.now(ET_TIMEZONE)

def read_tickers_from_file(path: str) -> List[str]:
    """Reads a list of tickers from a file, cleaning and validating each symbol."""
    tickers = set()
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "Symbol" in line:
                    continue
                tokens = re.split(r'[\s,\t|;]+', line)
                for tok in tokens:
                    if re.match(r"^[A-Z]{1,5}$", tok):
                        tickers.add(tok)
                        break
    except FileNotFoundError:
        print(f"Warning: Ticker file not found at {path}")
    return list(tickers)

def score_headlines_vader(titles: List[str]) -> Optional[float]:
    """Calculates the average VADER compound sentiment score for a list of headlines."""
    if not titles:
        return None
    scores = [VADER_ANALYZER.polarity_scores(t)["compound"] for t in titles if t]
    return sum(scores) / len(scores) if scores else None

# -----------------------------------------------------------------------------
# SYNCHRONOUS DATA FETCHING (for threading)
# -----------------------------------------------------------------------------

def fetch_ticker_data_sync(ticker: str) -> Optional[Dict]:
    """
    Synchronous version of ticker data fetching for use with threading.
    """
    client = get_client()
    
    try:
        # 1. Get previous day's close price
        try:
            prev_close_response = client.get_previous_close_agg(ticker)
            if not prev_close_response or not prev_close_response.results:
                print(f"No previous close data for {ticker}")
                return None
            prev_close = prev_close_response.results[0]["c"]  # 'c' is close price
        except Exception as e:
            print(f"Error getting previous close for {ticker}: {e}")
            return None

        # 2. Get current price using last trade
        current_price = None
        try:
            last_trade_response = client.get_last_trade(ticker)
            if last_trade_response and hasattr(last_trade_response, 'results'):
                current_price = last_trade_response.results["p"]  # 'p' is price
            elif last_trade_response and hasattr(last_trade_response, 'price'):
                current_price = last_trade_response.price
        except Exception as e:
            print(f"Error getting last trade for {ticker}: {e}")

        # 3. If no current price, try getting latest minute bar
        if not current_price:
            try:
                now = datetime.now()
                today_str = now.strftime('%Y-%m-%d')
                
                aggs_response = client.get_aggs(
                    ticker=ticker,
                    multiplier=1,
                    timespan="minute",
                    from_=today_str,
                    to=today_str,
                    adjusted=True,
                    sort="desc",
                    limit=1
                )
                
                if aggs_response and aggs_response.results:
                    current_price = aggs_response.results[0]["c"]  # close price of latest bar
            except Exception as e:
                print(f"Error getting aggregates for {ticker}: {e}")

        # 4. If still no current price, use previous close
        if not current_price:
            current_price = prev_close
            print(f"Using previous close for {ticker} - no current data")

        # 5. Get company details for market cap
        market_cap = 0
        try:
            details_response = client.get_ticker_details(ticker)
            if details_response and details_response.results:
                market_cap = details_response.results.get("market_cap", 0) or 0
        except Exception as e:
            print(f"Error getting details for {ticker}: {e}")

        # 6. Calculate percent change
        pct_change = ((current_price - prev_close) / prev_close) * 100

        # 7. For testing, set a dummy pre-market volume
        pre_market_volume = 100000  # This would need real pre-market data in production

        result = {
            "ticker": ticker,
            "prev_close": prev_close,
            "pre_market_price": current_price,
            "pre_market_pct": pct_change,
            "pre_market_volume": pre_market_volume,
            "market_cap": market_cap
        }
        
        print(f"✓ {ticker}: {pct_change:.2f}% change, ${market_cap:,} market cap")
        return result

    except Exception as e:
        print(f"Critical error processing {ticker}: {e}")
        return None

def fetch_and_score_news_sync(ticker: str) -> Dict:
    """Synchronous version of news fetching."""
    client = get_client()
    
    try:
        news_response = client.get_ticker_news(ticker, limit=10)
        titles = []
        
        if news_response and news_response.results:
            titles = [item["title"] for item in news_response.results if item.get("title")]
        
        avg_score = score_headlines_vader(titles)
        
        return {
            "ticker": ticker,
            "news_score": avg_score,
            "is_positive_news": avg_score is not None and avg_score > NEWS_POSITIVE_THRESHOLD,
            "articles_found": len(titles)
        }
        
    except Exception as e:
        print(f"Error fetching news for {ticker}: {e}")
        return {
            "ticker": ticker,
            "news_score": None,
            "is_positive_news": False,
            "articles_found": 0
        }

# -----------------------------------------------------------------------------
# ASYNC WRAPPERS USING THREADING
# -----------------------------------------------------------------------------

async def fetch_ticker_data_async(ticker: str) -> Optional[Dict]:
    """Async wrapper for synchronous ticker data fetching."""
    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        result = await loop.run_in_executor(executor, fetch_ticker_data_sync, ticker)
    return result

async def fetch_and_score_news_async(ticker: str) -> Dict:
    """Async wrapper for synchronous news fetching."""
    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        result = await loop.run_in_executor(executor, fetch_and_score_news_sync, ticker)
    return result

# -----------------------------------------------------------------------------
# MAIN ANALYSIS FUNCTION
# -----------------------------------------------------------------------------

async def run_premarket_analysis():
    """Main analysis function with proper async handling."""
    print("Starting pre-market analysis...")
    
    # Test API connection first
    try:
        test_client = RESTClient(POLYGON_API_KEY)
        test_response = test_client.get_previous_close_agg("AAPL")
        if not test_response or not test_response.results:
            print("ERROR: Cannot connect to Polygon API or no data available")
            return
        print("✓ API connection verified")
    except Exception as e:
        print(f"ERROR: API connection failed: {e}")
        return

    # Load tickers - limit for testing
    nasdaq_tickers = read_tickers_from_file(NASDAQ_FILE)
    other_tickers = read_tickers_from_file(OTHER_FILE)
    
    # For testing, use only first 50 tickers from each file
    all_tickers = (nasdaq_tickers[:25] + other_tickers[:25])
    all_tickers = sorted(list(set(all_tickers)))
    
    if not all_tickers:
        print("Error: No tickers loaded. Check your ticker files.")
        return
    
    print(f"Testing with {len(all_tickers)} tickers: {all_tickers[:10]}...")

    # Process tickers in batches
    all_results = []
    
    for i in range(0, len(all_tickers), BATCH_SIZE):
        batch = all_tickers[i:i + BATCH_SIZE]
        print(f"\nProcessing batch {i//BATCH_SIZE + 1} ({len(batch)} tickers)...")
        
        # Process batch concurrently
        tasks = [fetch_ticker_data_async(ticker) for ticker in batch]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter valid results
        valid_batch = []
        for j, result in enumerate(batch_results):
            if isinstance(result, Exception):
                print(f"Exception for {batch[j]}: {result}")
            elif result is not None:
                valid_batch.append(result)
        
        all_results.extend(valid_batch)
        print(f"Got {len(valid_batch)} valid results from this batch")
        
        # Small delay between batches
        if i + BATCH_SIZE < len(all_tickers):
            await asyncio.sleep(1.0)

    print(f"\nTotal valid results: {len(all_results)}")

    if not all_results:
        print("No valid data found for any tickers.")
        return

    # Create DataFrame and apply filters
    df = pd.DataFrame(all_results)
    print("\nData summary:")
    print(f"  Price changes range: {df['pre_market_pct'].min():.2f}% to {df['pre_market_pct'].max():.2f}%")
    print(f"  Market caps range: ${df['market_cap'].min():,} to ${df['market_cap'].max():,}")
    
    # Apply strategy filters
    candidates_df = df[
        (df["pre_market_pct"].between(MIN_PCT_CHANGE, MAX_PCT_CHANGE)) &
        (df["pre_market_volume"] >= MIN_PREMARKET_VOLUME) &
        (df["market_cap"] >= MIN_MARKET_CAP_USD)
    ].copy()
    
    print(f"\nFound {len(candidates_df)} candidates after filters:")
    print(f"  - Price change: {MIN_PCT_CHANGE}% to {MAX_PCT_CHANGE}%")
    print(f"  - Min volume: {MIN_PREMARKET_VOLUME:,}")
    print(f"  - Min market cap: ${MIN_MARKET_CAP_USD:,}")

    # Apply news filter if enabled
    if ENABLE_NEWS_FILTER and not candidates_df.empty:
        print("\nApplying news sentiment filter...")
        news_tasks = [fetch_and_score_news_async(row.ticker) for row in candidates_df.itertuples()]
        news_results = await asyncio.gather(*news_tasks)
        news_df = pd.DataFrame(news_results)
        
        candidates_df = candidates_df.merge(news_df, on="ticker")
        candidates_df = candidates_df[candidates_df["is_positive_news"]].copy()
        print(f"Found {len(candidates_df)} candidates after news filter.")

    # Save results
    if not candidates_df.empty:
        candidates_df.sort_values(by="pre_market_pct", ascending=False, inplace=True)
        candidates_df.to_csv(CANDIDATES_FILE, index=False)
        print(f"\nSUCCESS: Saved {len(candidates_df)} candidates to {CANDIDATES_FILE}")
        
        print("\nTop candidates:")
        for _, row in candidates_df.head(10).iterrows():
            print(f"  {row['ticker']}: {row['pre_market_pct']:.2f}% change, "
                  f"${row['market_cap']:,} market cap")
    else:
        print("\nNo candidates met all criteria. Saving empty file.")
        pd.DataFrame().to_csv(CANDIDATES_FILE, index=False)

async def run_simulated_trading_session():
    """Simple trading simulation for testing."""
    if not os.path.exists(CANDIDATES_FILE):
        print("No candidates file found. Please run analysis first.")
        return

    candidates_df = pd.read_csv(CANDIDATES_FILE)
    if candidates_df.empty:
        print("No candidates to trade.")
        return

    tickers = candidates_df['ticker'].tolist()
    print(f"\n--- Simulating trades for {len(tickers)} candidates ---")
    
    # For testing, just simulate immediate buy/sell
    trades = []
    total_pl = 0
    
    for _, row in candidates_df.iterrows():
        ticker = row['ticker']
        buy_price = row['pre_market_price']
        
        # Simulate a small price movement (random for testing)
        import random
        price_change = random.uniform(-0.02, 0.03)  # -2% to +3% change
        sell_price = buy_price * (1 + price_change)
        
        quantity = POSITION_SIZE_USD / buy_price
        pl_usd = (sell_price - buy_price) * quantity
        pl_pct = (sell_price / buy_price - 1) * 100
        total_pl += pl_usd
        
        trade_result = row.to_dict()
        trade_result.update({
            "buy_price": buy_price,
            "sell_price": sell_price,
            "pl_usd": pl_usd,
            "pl_pct": pl_pct
        })
        trades.append(trade_result)
        
        print(f"  {ticker}: ${pl_usd:.2f} ({pl_pct:+.2f}%)")

    # Save results
    if trades:
        results_df = pd.DataFrame(trades)
        results_df.to_csv(RESULTS_FILE, index=False)
        print(f"\n--- SIMULATION COMPLETE ---")
        print(f"Total P/L: ${total_pl:.2f}")
        print(f"Results saved to {RESULTS_FILE}")

# -----------------------------------------------------------------------------
# MAIN EXECUTION
# -----------------------------------------------------------------------------

async def main():
    """Main entry point."""
    print("=== STOCK TRADING ANALYSIS ===")
    print("Testing with limited tickers for debugging...")
    
    # Check files exist
    if not os.path.exists(NASDAQ_FILE):
        print(f"Creating dummy {NASDAQ_FILE} for testing...")
        with open(NASDAQ_FILE, 'w') as f:
            f.write("AAPL\nMSFT\nGOOGL\nTSLA\nAMZN\nMETA\nNVDA\nNFLX\nADBE\nCRM\n")
    
    if not os.path.exists(OTHER_FILE):
        print(f"Creating dummy {OTHER_FILE} for testing...")
        with open(OTHER_FILE, 'w') as f:
            f.write("SPY\nQQQ\nIWM\nVTI\nVOO\n")
    
    try:
        # Run analysis
        await run_premarket_analysis()
        
        # Run simulation
        await run_simulated_trading_session()
        
        print("\n=== ANALYSIS COMPLETE ===")
        
    except Exception as e:
        print(f"Error in main execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if not POLYGON_API_KEY or POLYGON_API_KEY == "YOUR_API_KEY_HERE":
        print("ERROR: Please set your Polygon API key")
    else:
        asyncio.run(main())