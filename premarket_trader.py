"""
Premarket trading analysis script (Agent Mode).
Loads tickers, scans premarket data, applies filters, enriches with news sentiment, and simulates trades.
"""
import os
import re
import time
from datetime import datetime, time as dtime, timezone
from dateutil import tz
from typing import List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from yahooquery import Ticker
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# -------------------------
# Config
# -------------------------
############################################################
# --- Trading and analysis configuration ---
# All configurable parameters for the strategy are set here.
# To enable auto-modification, simply change values below or
# load from external sources if needed. This section controls
# filtering, batch sizes, trading times, file outputs, and more.
############################################################
MIN_PCT = 5.0  # Minimum % change for filter
MAX_PCT = 15.0 # Maximum % change for filter
LOOKBACK_DAYS = 5  # Days of historical data to fetch
BATCH_SIZE = 80     # Number of tickers per batch for API calls
NASDAQ_FILE = "nasdaqlisted.txt"
OTHER_FILE = "otherlisted.txt"

MIN_PCT = 5.0
MAX_PCT = 15.0
LOOKBACK_DAYS = 5
BATCH_SIZE = 80

POSITION_SIZE_USD = 1000
BUY_TIME_IST = dtime(18, 0)
SELL_TIME_IST = dtime(18, 55)

OUTPUT_CSV = "premarket_log.csv"
CANDIDATES_FILE = "trade_candidates.csv"
RESULTS_FILE = "trade_results.csv"

NEWS_LOOKBACK_HOURS = 108
NEWS_POSITIVE_THRESHOLD = 0.1
NEWS_MAX_WORKERS = 16
NEWS_RETRIES = 2

# Sentiment analyzer
_analyzer = SentimentIntensityAnalyzer()

# -------------------------
# Helpers
# -------------------------
## Get current time in IST timezone
def get_current_ist():
    ist = tz.gettz("Asia/Kolkata")
    return datetime.now(ist)

def read_tickers_from_file(path: str) -> List[str]:
    """Read tickers from a file, ignoring comments and extracting valid symbols."""
    tickers = []
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
    return tickers

def chunk_list(lst, n):
    """Yield successive n-sized chunks from list."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def compute_close_to_close_pct(close_series: pd.Series, n_changes: int = 3) -> List[float]:
    """Compute last n close-to-close percent changes from a price series."""
    closes = close_series.dropna()
    if len(closes) < (n_changes + 1):
        return []
    pct_changes = (closes.pct_change().dropna() * 100).tolist()
    return pct_changes[-n_changes:]

# -------------------------
# Pre-market scanner
# -------------------------
## Scan tickers for premarket price and filter by % change
def scan_premarket(ticker_list: List[str]) -> pd.DataFrame:
    results = []
    now = datetime.now(timezone.utc).isoformat()
    # Progress logging
    total_tickers = len(ticker_list)
    print(f"Starting premarket scan for {total_tickers} tickers...")
    chunk_num = 0
    processed_tickers = 0

    for chunk in chunk_list(ticker_list, BATCH_SIZE):
        chunk_num += 1
        print(f"Processing chunk {chunk_num}: {len(chunk)} tickers. {processed_tickers}/{total_tickers} done, {total_tickers-processed_tickers} pending.")
        symbols = " ".join(chunk)
        t = Ticker(symbols)
        price_data = t.price
        hist = t.history(period=f"{LOOKBACK_DAYS}d", interval="1d")

        for sym in chunk:
            print(f"  Processing ticker: {sym} ({processed_tickers+1}/{total_tickers})")
            try:
                p = price_data.get(sym) if isinstance(price_data, dict) else price_data
            except Exception:
                p = None
            if not p or not isinstance(p, dict):
                continue
            processed_tickers += 1

            pre_price = p.get("preMarketPrice")
            prev_close = p.get("regularMarketPreviousClose") or p.get("previousClose")
            market_state = p.get("marketState")
            market_cap = p.get("marketCap")
            pre_volume = p.get("preMarketVolume")
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
            except Exception:
                closes = pd.Series(dtype=float)

            last3_pct_changes = compute_close_to_close_pct(closes, 3)

            pre_market_pct = None
            if pre_price is not None and prev_close not in (None, 0):
                try:
                    pre_market_pct = (float(pre_price) - float(prev_close)) / float(prev_close) * 100.0
                except Exception:
                    pre_market_pct = None

            meets_filter = (
                pre_market_pct is not None and
                MIN_PCT <= pre_market_pct <= MAX_PCT
            )

            results.append({
                "timestamp_utc": now,
                "ticker": sym,
                "market_state": market_state,
                "pre_market_price": pre_price,
                "pre_market_pct_calc": pre_market_pct,
                "pre_market_volume": pre_volume,
                "regular_prev_close": prev_close,
                "market_cap": market_cap,
                "last_3_days_pct_changes": last3_pct_changes,
                "meets_filter": meets_filter
            })

        time.sleep(0.5)

    return pd.DataFrame(results)

# -------------------------
# News Sentiment
# -------------------------
## Score news headlines for sentiment using VADER
def score_headlines_vader(titles: List[str]) -> Optional[float]:
    if not titles:
        return None
    scores = [_analyzer.polarity_scores(t)["compound"] for t in titles if t]
    return sum(scores) / len(scores) if scores else None

def fetch_and_score_news(ticker: str,
    """Fetch news for ticker, score sentiment, and return summary."""
                         hours_back: int = NEWS_LOOKBACK_HOURS,
                         retries: int = NEWS_RETRIES) -> Tuple[str, Optional[float], bool, int]:
    cutoff = datetime.utcnow().timestamp() - hours_back * 3600
    last_err = None
    for _ in range(retries + 1):
        try:
            t = Ticker(ticker)
            items = t.news or []
            titles = []
            for it in items:
                pub = it.get("providerPublishTime")
                if isinstance(pub, (int, float)) and pub >= cutoff:
                    title = it.get("title") or ""
                    if title:
                        titles.append(title)
            avg = score_headlines_vader(titles)
            positive = (avg is not None) and (avg > NEWS_POSITIVE_THRESHOLD)
            return (ticker, avg, positive, len(titles))
        except Exception as e:
            last_err = e
            time.sleep(0.2)
    return (ticker, None, False, 0)

def enrich_with_news_parallel(df_filtered: pd.DataFrame) -> pd.DataFrame:
    """Enrich filtered DataFrame with news sentiment scores in parallel."""
    if df_filtered.empty:
        df_filtered["news_avg_compound"] = None
        df_filtered["news_articles_used"] = 0
        df_filtered["positive_news"] = False
        return df_filtered

    symbols = df_filtered["ticker"].tolist()
    results = []
    with ThreadPoolExecutor(max_workers=NEWS_MAX_WORKERS) as ex:
        future_map = {ex.submit(fetch_and_score_news, sym): sym for sym in symbols}
        for fut in as_completed(future_map):
            ticker, avg, positive, count = fut.result()
            results.append((ticker, avg, positive, count))

    news_df = pd.DataFrame(results, columns=["ticker", "news_avg_compound", "positive_news", "news_articles_used"])
    return df_filtered.merge(news_df, on="ticker", how="left")

# -------------------------
# Trading simulation
# -------------------------
## Main analysis pipeline: load, scan, filter, enrich, save
def run_analysis():
    print("Loading tickers from files...")
    tickers = read_tickers_from_file(NASDAQ_FILE) + read_tickers_from_file(OTHER_FILE)
    tickers = list(dict.fromkeys([t.upper() for t in tickers if t]))
    if not tickers:
        print("No tickers found.")
        return
    print(f"Loaded {len(tickers)} tickers for analysis.")

    print("Running premarket scan...")
    df = scan_premarket(tickers)
    print("Filtering candidates...")
    filtered = df[df["meets_filter"]].copy()
    print(f"{len(filtered)} candidates after filter.")
    print("Enriching with news in parallel...")
    filtered = enrich_with_news_parallel(filtered)
    final = filtered[filtered["positive_news"]].copy()
    final.to_csv(CANDIDATES_FILE, index=False)
    print(f"Analysis complete. {len(final)} candidates saved to {CANDIDATES_FILE}.")

def run_paper_trades():
    """Simulate paper trades for filtered candidates and save results."""
    if not os.path.exists(CANDIDATES_FILE):
        print("No candidates file found. Run analysis first.")
        return
    candidates = pd.read_csv(CANDIDATES_FILE)
    results = []
    for _, row in candidates.iterrows():
        ticker = row["ticker"]
        t = Ticker(ticker)
        p = t.price.get(ticker, {})
        buy_price = p.get("regularMarketPrice")
        sell_price = p.get("regularMarketPrice")  # placeholder — re-fetch in real setup at SELL_TIME_IST
        if buy_price and sell_price:
            qty = POSITION_SIZE_USD / buy_price
            pl = (sell_price - buy_price) * qty
            row_dict = row.to_dict()
            row_dict.update({
                "buy_price": buy_price,
                "sell_price": sell_price,
                "pl_usd": pl
            })
            results.append(row_dict)

    if results:
        df_res = pd.DataFrame(results)
        df_res.to_csv(RESULTS_FILE, index=False)
        print(f"Paper trades complete. Results saved to {RESULTS_FILE}.")
    else:
        print("No trades executed.")

# -------------------------
# Main
# -------------------------
## Entry point: run analysis, wait for trading time, then simulate trades
if __name__ == "__main__":
    now_ist = get_current_ist()
    run_analysis()
    wait_seconds = (datetime.combine(now_ist.date(), BUY_TIME_IST, tzinfo=tz.gettz("Asia/Kolkata")) - now_ist).total_seconds()
    if wait_seconds > 0:
        print(f"Waiting {wait_seconds/60:.1f} minutes until trading time...")
        time.sleep(wait_seconds)
    run_paper_trades()
