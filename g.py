# main_trader.py
import os
import re
import asyncio
import pytz
import pandas as pd
from datetime import datetime, time as dtime
from typing import List, Dict, Optional

# --- Third-party libraries ---
# You need to install these first:
# pip install polygon-api-client pandas vaderSentiment pytz
from polygon import RESTClient
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------

# --- API Key ---
# IMPORTANT: Keep your API key secret. 
# It's better to load this from an environment variable.
POLYGON_API_KEY = "TtQDKmwPXtJ7A078n9cwckbHNpEO_eUe"

# --- File Paths ---
NASDAQ_FILE = "nasdaqlisted.txt"
OTHER_FILE = "otherlisted.txt"
CANDIDATES_FILE = "trade_candidates.csv"
RESULTS_FILE = "trade_results.csv"

# --- Strategy Filters ---
# The core parameters for your pre-market scanning strategy.
MIN_PCT_CHANGE = 5.0          # Minimum pre-market gap up (e.g., 5.0 for 5%)
MAX_PCT_CHANGE = 15.0         # Maximum pre-market gap up (e.g., 15.0 for 15%)
MIN_PREMARKET_VOLUME = 75000  # Minimum pre-market shares traded
MIN_MARKET_CAP_USD = 300000000 # Minimum market capitalization (e.g., $300M)

# --- News Filter Control ---
# Set to True to enable news sentiment analysis, False to disable it.
ENABLE_NEWS_FILTER = True
NEWS_POSITIVE_THRESHOLD = 0.1 # VADER compound score threshold for "positive" news

# --- Trading Simulation Parameters ---
# All times are in US/Eastern Time (ET), the market's native timezone.
# The US market opens at 9:30 AM ET and closes at 4:00 PM ET.
ET_TIMEZONE = pytz.timezone("US/Eastern")
BUY_TIME_ET = dtime(9, 31)   # Buy 1 minute after market open (9:31 AM ET)
SELL_TIME_ET = dtime(15, 55) # Sell 5 minutes before market close (3:55 PM ET)
POSITION_SIZE_USD = 1000     # The dollar amount to simulate for each trade

# --- Global Objects ---
# Initializing these once saves computation.
VADER_ANALYZER = SentimentIntensityAnalyzer()


# -----------------------------------------------------------------------------
# HELPER FUNCTIONS
# -----------------------------------------------------------------------------

def get_current_et_time() -> datetime:
    """Gets the current time in the US/Eastern timezone."""
    return datetime.now(ET_TIMEZONE)

def read_tickers_from_file(path: str) -> List[str]:
    """
    Reads a list of tickers from a file, cleaning and validating each symbol.
    This function is robust against different file formats.
    """
    tickers = set() # Use a set to automatically handle duplicates
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "Symbol" in line:
                    continue
                # Split by common delimiters and find the first valid ticker symbol
                tokens = re.split(r'[\s,\t|;]+', line)
                for tok in tokens:
                    if re.match(r"^[A-Z]{1,5}$", tok):
                        tickers.add(tok)
                        break
    except FileNotFoundError:
        print(f"Warning: Ticker file not found at {path}")
    return list(tickers)

def score_headlines_vader(titles: List[str]) -> Optional[float]:
    """
    Calculates the average VADER compound sentiment score for a list of headlines.
    """
    if not titles:
        return None
    scores = [VADER_ANALYZER.polarity_scores(t)["compound"] for t in titles if t]
    return sum(scores) / len(scores) if scores else None


# -----------------------------------------------------------------------------
# ASYNCHRONOUS DATA FETCHING (with Polygon.io)
# -----------------------------------------------------------------------------

async def fetch_ticker_data(client: RESTClient, ticker: str) -> Optional[Dict]:
    """
    Asynchronously fetches all required pre-market data for a single ticker.
    This function combines multiple API calls into one logical unit.
    """
    try:
        # 1. Get previous day's close price
        prev_close_req = client.get_previous_close_agg_async(ticker)
        
        # 2. Get the latest pre-market quote (price and volume)
        # Using aggregates provides a more stable price than a single last quote.
        now = datetime.utcnow()
        today_str = now.strftime('%Y-%m-%d')
        premarket_aggs_req = client.get_aggs_async(
            ticker, 1, "minute", f"{today_str}T04:00:00Z", now.isoformat(), adjusted=True, limit=5000
        )

        # 3. Get company details for market cap
        details_req = client.get_ticker_details_async(ticker)

        # Await all requests concurrently for maximum speed
        prev_close_data, premarket_aggs, details = await asyncio.gather(
            prev_close_req, premarket_aggs_req, details_req, return_exceptions=True
        )

        # --- Process API responses and handle potential errors ---
        if isinstance(prev_close_data, Exception) or not prev_close_data or not prev_close_data.results:
            return None # Cannot proceed without a previous close price
        prev_close = prev_close_data.results[0].close

        pre_market_price = premarket_aggs[-1].close if not isinstance(premarket_aggs, Exception) and premarket_aggs else None
        pre_market_volume = sum(agg.volume for agg in premarket_aggs) if not isinstance(premarket_aggs, Exception) and premarket_aggs else 0
        
        market_cap = details.market_cap if not isinstance(details, Exception) and details.market_cap else 0

        if not pre_market_price:
            return None # No pre-market activity found

        # Calculate pre-market percent change
        pct_change = ((pre_market_price - prev_close) / prev_close) * 100

        return {
            "ticker": ticker,
            "prev_close": prev_close,
            "pre_market_price": pre_market_price,
            "pre_market_pct": pct_change,
            "pre_market_volume": pre_market_volume,
            "market_cap": market_cap
        }

    except Exception:
        # Broad exception to catch any other issues with a specific ticker
        return None

async def fetch_and_score_news(client: RESTClient, ticker: str) -> Dict:
    """
    Asynchronously fetches recent news and calculates a sentiment score.
    """
    try:
        news_items = await client.get_ticker_news_async(ticker, limit=20)
        titles = [item.title for item in news_items]
        avg_score = score_headlines_vader(titles)
        return {
            "ticker": ticker,
            "news_score": avg_score,
            "is_positive_news": avg_score is not None and avg_score > NEWS_POSITIVE_THRESHOLD,
            "articles_found": len(titles)
        }
    except Exception:
        return {
            "ticker": ticker,
            "news_score": None,
            "is_positive_news": False,
            "articles_found": 0
        }

# -----------------------------------------------------------------------------
# CORE LOGIC: ANALYSIS & TRADING
# -----------------------------------------------------------------------------

async def run_premarket_analysis():
    """
    Main analysis function. Scans all tickers, applies filters, and saves
    the final candidates to a CSV file.
    """
    print("Starting pre-market analysis...")
    client = RESTClient(POLYGON_API_KEY)

    # 1. Load tickers
    tickers = read_tickers_from_file(NASDAQ_FILE) + read_tickers_from_file(OTHER_FILE)
    tickers = sorted(list(set(tickers))) # Remove duplicates and sort
    if not tickers:
        print("Error: No tickers loaded. Exiting.")
        await client.close()
        return
    
    print(f"Loaded {len(tickers)} unique tickers for scanning.")

    # 2. Asynchronously fetch data for all tickers
    tasks = [fetch_ticker_data(client, ticker) for ticker in tickers]
    results = await asyncio.gather(*tasks)
    
    # Filter out tickers that failed to fetch or have no data
    valid_results = [r for r in results if r is not None]
    if not valid_results:
        print("No valid pre-market data found for any tickers.")
        await client.close()
        return

    df = pd.DataFrame(valid_results)
    
    # 3. Apply strategy filters
    candidates_df = df[
        (df["pre_market_pct"].between(MIN_PCT_CHANGE, MAX_PCT_CHANGE)) &
        (df["pre_market_volume"] >= MIN_PREMARKET_VOLUME) &
        (df["market_cap"] >= MIN_MARKET_CAP_USD)
    ].copy()
    
    print(f"Found {len(candidates_df)} candidates after initial price/volume/market cap filters.")

    # 4. (Optional) Apply news sentiment filter
    if ENABLE_NEWS_FILTER and not candidates_df.empty:
        print("Enriching candidates with news sentiment analysis...")
        news_tasks = [fetch_and_score_news(client, row.ticker) for row in candidates_df.itertuples()]
        news_results = await asyncio.gather(*news_tasks)
        news_df = pd.DataFrame(news_results)
        
        # Merge news data and filter for positive news
        candidates_df = candidates_df.merge(news_df, on="ticker")
        candidates_df = candidates_df[candidates_df["is_positive_news"]].copy()
        print(f"Found {len(candidates_df)} candidates after positive news filter.")

    # 5. Save final candidates
    if not candidates_df.empty:
        candidates_df.sort_values(by="pre_market_pct", ascending=False, inplace=True)
        candidates_df.to_csv(CANDIDATES_FILE, index=False)
        print(f"Successfully saved {len(candidates_df)} final candidates to {CANDIDATES_FILE}.")
    else:
        print("No candidates met all strategy criteria.")

    await client.close()

async def run_simulated_trading_session():
    """
    Executes the paper trading logic based on the candidates file.
    This function handles waiting for buy/sell times and fetching live prices.
    """
    if not os.path.exists(CANDIDATES_FILE):
        print("No candidates file found. Please run the analysis first.")
        return

    candidates_df = pd.read_csv(CANDIDATES_FILE)
    if candidates_df.empty:
        print("Candidates file is empty. No trades to simulate.")
        return

    tickers_to_trade = candidates_df['ticker'].tolist()
    print(f"\n--- Starting Simulated Trading Session for {len(tickers_to_trade)} tickers ---")
    
    client = RESTClient(POLYGON_API_KEY)
    
    # --- Wait for Buy Time ---
    now_et = get_current_et_time()
    buy_datetime = now_et.replace(hour=BUY_TIME_ET.hour, minute=BUY_TIME_ET.minute, second=0, microsecond=0)
    
    wait_seconds = (buy_datetime - now_et).total_seconds()
    if wait_seconds > 0:
        print(f"Waiting {wait_seconds / 60:.1f} minutes until buy time ({BUY_TIME_ET.strftime('%H:%M')} ET)...")
        await asyncio.sleep(wait_seconds)

    # --- Execute Buys (Fetch Buy Prices) ---
    print(f"It's {BUY_TIME_ET.strftime('%H:%M')} ET. Executing simulated buys...")
    buy_price_tasks = [client.get_last_trade_async(t) for t in tickers_to_trade]
    buy_results = await asyncio.gather(*buy_price_tasks, return_exceptions=True)
    
    trades = {}
    for i, res in enumerate(buy_results):
        ticker = tickers_to_trade[i]
        if not isinstance(res, Exception) and res.price:
            trades[ticker] = {"buy_price": res.price}
            print(f"  BOUGHT {ticker} @ ${res.price:.2f}")
        else:
            trades[ticker] = {"buy_price": None}
            print(f"  FAILED to buy {ticker}. Could not fetch price.")

    # --- Wait for Sell Time ---
    now_et = get_current_et_time()
    sell_datetime = now_et.replace(hour=SELL_TIME_ET.hour, minute=SELL_TIME_ET.minute, second=0, microsecond=0)
    
    wait_seconds = (sell_datetime - now_et).total_seconds()
    if wait_seconds > 0:
        print(f"All positions opened. Waiting {wait_seconds / 3600:.2f} hours until sell time ({SELL_TIME_ET.strftime('%H:%M')} ET)...")
        await asyncio.sleep(wait_seconds)

    # --- Execute Sells (Fetch Sell Prices) ---
    print(f"It's {SELL_TIME_ET.strftime('%H:%M')} ET. Executing simulated sells...")
    sell_price_tasks = [client.get_last_trade_async(t) for t in tickers_to_trade]
    sell_results = await asyncio.gather(*sell_price_tasks, return_exceptions=True)

    for i, res in enumerate(sell_results):
        ticker = tickers_to_trade[i]
        if ticker in trades:
            if not isinstance(res, Exception) and res.price:
                trades[ticker]["sell_price"] = res.price
                print(f"  SOLD {ticker} @ ${res.price:.2f}")
            else:
                trades[ticker]["sell_price"] = None
                print(f"  FAILED to sell {ticker}. Could not fetch price.")
    
    # --- Calculate and Save Results ---
    final_results = []
    for ticker, data in trades.items():
        buy_price = data.get("buy_price")
        sell_price = data.get("sell_price")
        
        if buy_price and sell_price:
            quantity = POSITION_SIZE_USD / buy_price
            pl_usd = (sell_price - buy_price) * quantity
            pl_pct = (sell_price / buy_price - 1) * 100
        else:
            pl_usd, pl_pct = 0.0, 0.0
            
        # Get original candidate info to merge with results
        original_data = candidates_df[candidates_df['ticker'] == ticker].iloc[0].to_dict()
        original_data.update({
            "buy_price": buy_price,
            "sell_price": sell_price,
            "pl_usd": pl_usd,
            "pl_pct": pl_pct,
        })
        final_results.append(original_data)

    if final_results:
        results_df = pd.DataFrame(final_results)
        results_df.to_csv(RESULTS_FILE, index=False)
        total_pl = results_df['pl_usd'].sum()
        print("\n--- Trading Session Complete ---")
        print(f"Results saved to {RESULTS_FILE}")
        print(f"Total Simulated P/L: ${total_pl:.2f}")

    await client.close()


# -----------------------------------------------------------------------------
# MAIN EXECUTION BLOCK
# -----------------------------------------------------------------------------
async def main():
    """The main entry point for the application."""
    # This script should be run on a trading day before the market opens,
    # for example, around 8:00 AM ET.
    
    # Step 1: Find candidates in the pre-market.
    await run_premarket_analysis()

    # Step 2: Execute the buy/sell simulation during market hours.
    await run_simulated_trading_session()


if __name__ == "__main__":
    # Ensure you have nasdaqlisted.txt and otherlisted.txt in the same directory.
    # The script will run the analysis, then wait for the market to open to trade.
    
    # Check if API key is set
    if POLYGON_API_KEY == "YOUR_API_KEY_HERE" or not POLYGON_API_KEY:
        print("ERROR: Please set your Polygon API key in the configuration section.")
    else:
        asyncio.run(main())

