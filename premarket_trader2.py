#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Real-time pre-market scanner + 30-day backtest for U.S. stocks.

Outputs:
  - all_tickers.csv                     (cached universe)
  - analysis_full.csv                   (real-time pass/fail with reasons)
  - analysis_passed.csv                 (real-time passes only)
  - backtest_picks.csv                  (daily picks during backtest)
  - backtest_results.csv                (per-day summary & overall stats)

Notes:
  * Real-time uses yahooquery.price pre-market fields.
  * Backtest proxies pre-market with open-gap vs prior close (historical pre-market not available).

python premarket_trader.py --no-news
python premarket_trader.py --backtest --days 30
python premarket_trader.py


"""

import argparse
import os
import re
import sys
import time
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional

import pandas as pd
from dateutil import tz
from yahooquery import Ticker
from concurrent.futures import ThreadPoolExecutor, as_completed
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# -------------------------
# Config (tune as needed)
# -------------------------
NASDAQ_FILE = "nasdaqlisted.txt"   # pipe-delimited; first column = Symbol
OTHER_FILE  = "otherlisted.txt"    # pipe-delimited; first column = ACT Symbol
UNIVERSE_CACHE = "all_tickers.csv"

BATCH_SIZE = 80

# Real-time strategy rules
MIN_PCT = 3.0
MAX_PCT = 20.0
MIN_PRE_VOL = 50_000

# Optional news sentiment
USE_NEWS = True
NEWS_LOOKBACK_HOURS = 24
NEWS_POSITIVE_THRESHOLD = 0.10
NEWS_MAX_WORKERS = 12
NEWS_RETRIES = 1

# Backtest rules (proxy for premarket)
BACKTEST_DAYS = 30
BACKTEST_MIN_PCT = MIN_PCT
BACKTEST_MAX_PCT = MAX_PCT
BACKTEST_MIN_DAILY_VOL = 1_000_000  # liquidity proxy
# -------------------------

_analyzer = SentimentIntensityAnalyzer()


# ---------- Utils ----------
def get_current_ist() -> datetime:
    return datetime.now(tz.gettz("Asia/Kolkata"))

def ensure_universe_cache() -> List[str]:
    """Read tickers from the two pipe files and cache to CSV."""
    if os.path.exists(UNIVERSE_CACHE):
        df = pd.read_csv(UNIVERSE_CACHE)
        symbols = df["symbol"].dropna().astype(str).str.upper().tolist()
        return symbols

    # autodetect pipe with pandas
    nas = pd.read_csv(NASDAQ_FILE, sep="|")
    oth = pd.read_csv(OTHER_FILE,  sep="|")

    # first column for each file is the symbol
    nas_sym_col = nas.columns[0]   # "Symbol"
    oth_sym_col = oth.columns[0]   # "ACT Symbol"

    # remove test issues if flagged (when such column exists)
    if "Test Issue" in nas.columns:
        nas = nas[nas["Test Issue"].astype(str).str.upper() != "Y"]
    if "Test Issue" in oth.columns:
        oth = oth[oth["Test Issue"].astype(str).str.upper() != "Y"]

    nas_symbols = nas[nas_sym_col].dropna().astype(str).str.upper()
    oth_symbols = oth[oth_sym_col].dropna().astype(str).str.upper()

    symbols = pd.Series(pd.concat([nas_symbols, oth_symbols], ignore_index=True)).drop_duplicates().tolist()
    pd.DataFrame({"symbol": symbols}).to_csv(UNIVERSE_CACHE, index=False)
    return symbols


def chunked(lst: List[str], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


# ---------- News sentiment ----------
def _score_titles_vader(titles: List[str]) -> Optional[float]:
    if not titles:
        return None
    scores = [_analyzer.polarity_scores(t)["compound"] for t in titles if t]
    return (sum(scores) / len(scores)) if scores else None

def _fetch_news_sentiment(symbol: str, hours_back: int) -> Tuple[str, Optional[float], bool, int]:
    cutoff = datetime.now(tz.UTC).timestamp() - hours_back * 3600
    try:
        t = Ticker(symbol)
        items = t.news or []
        titles = []
        for it in items:
            pub = it.get("providerPublishTime")
            if isinstance(pub, (int, float)) and pub >= cutoff:
                title = it.get("title") or ""
                if title:
                    titles.append(title)
        avg = _score_titles_vader(titles)
        positive = (avg is not None) and (avg > NEWS_POSITIVE_THRESHOLD)
        return symbol, avg, positive, len(titles)
    except Exception:
        return symbol, None, False, 0

def enrich_with_news(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        for c in ("news_avg_compound", "news_articles_used", "positive_news"):
            df[c] = [] if c == "news_articles_used" else None
        return df
    symbols = df["ticker"].tolist()
    results = []
    with ThreadPoolExecutor(max_workers=NEWS_MAX_WORKERS) as ex:
        futs = [ex.submit(_fetch_news_sentiment, s, NEWS_LOOKBACK_HOURS) for s in symbols]
        for f in as_completed(futs):
            results.append(f.result())
    news_df = pd.DataFrame(results, columns=["ticker", "news_avg_compound", "positive_news", "news_articles_used"])
    return df.merge(news_df, on="ticker", how="left")


# ---------- Real-time scan ----------
def realtime_scan(symbols: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Query yahooquery.price, apply filters, and return:
      (full_df_with_reasons, passed_df)
    """
    rows = []
    for batch in chunked(symbols, BATCH_SIZE):
        tk = Ticker(" ".join(batch))
        price = tk.price  # dict keyed by symbol OR a dict-like
        for s in batch:
            p = {}
            try:
                p = price.get(s, {}) if isinstance(price, dict) else {}
            except Exception:
                p = {}
            if not isinstance(p, dict):
                rows.append({"ticker": s, "reason": "Bad price payload", "pre_change_pct": None,
                             "pre_volume": None, "meets_filter": False})
                continue

            pre_pct = p.get("preMarketChangePercent")
            pre_vol = p.get("preMarketVolume")
            pre_price = p.get("preMarketPrice")
            prev_close = p.get("regularMarketPreviousClose") or p.get("previousClose")

            reason = None
            meets = True
            if pre_pct is None:
                meets = False; reason = "No pre-market data"
            elif not (MIN_PCT <= pre_pct <= MAX_PCT):
                meets = False; reason = f"Change {pre_pct:.2f}% out of range"
            elif pre_vol is None or pre_vol < MIN_PRE_VOL:
                meets = False; reason = f"Volume too low ({pre_vol})"

            rows.append({
                "timestamp_utc": datetime.now(tz.UTC).isoformat(),
                "ticker": s,
                "pre_market_price": pre_price,
                "regular_prev_close": prev_close,
                "pre_change_pct": pre_pct,
                "pre_volume": pre_vol,
                "reason": "Pass" if meets else reason,
                "meets_filter": bool(meets),
            })

        time.sleep(0.2)  # polite

    full = pd.DataFrame(rows)

    passed = full[full["meets_filter"] == True].copy()
    if USE_NEWS and not passed.empty:
        passed = enrich_with_news(passed)
        passed = passed[(passed["positive_news"] == True) | (passed["news_avg_compound"].isna())]  # keep if positive or no news found

    return full, passed


# ---------- Backtest (proxy) ----------
def backtest(symbols: List[str], days: int = BACKTEST_DAYS) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Proxy pre-market gap with open-gap vs prior close.
    Picks: BACKTEST_MIN_PCT <= gap% <= BACKTEST_MAX_PCT and daily volume >= BACKTEST_MIN_DAILY_VOL.
    P/L: buy at open, sell at close (same day).
    """
    # Pull once for all symbols over (days + 5) buffer
    period_days = max(days + 5, 40)
    tk = Ticker(" ".join(symbols))
    hist = tk.history(period=f"{period_days}d", interval="1d")
    if not isinstance(hist, pd.DataFrame) or hist.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Normalize to MultiIndex(symbol, date)
    if not isinstance(hist.index, pd.MultiIndex):
        # single symbol path
        sym = symbols[0]
        hist = hist.copy()
        hist["symbol"] = sym
        hist = hist.set_index(["symbol", hist.index])
    hist = hist.sort_index()

    picks_rows = []
    day_rows = []

    # build per-symbol daily rows
    # For each day per symbol, compute prior close, gap%, and open->close return
    hist = hist.reset_index()
    hist.rename(columns={"symbol": "ticker"}, inplace=True)

    hist["date"] = pd.to_datetime(hist["date"]).dt.tz_localize("UTC")
    hist = hist.sort_values(["ticker", "date"]).reset_index(drop=True)

    for ticker, g in hist.groupby("ticker"):
        g = g.sort_values("date").reset_index(drop=True)
        g["prior_close"] = g["close"].shift(1)
        g["gap_pct"] = (g["open"] - g["prior_close"]) / g["prior_close"] * 100.0
        g["day_ret_pct"] = (g["close"] - g["open"]) / g["open"] * 100.0
        g["passes"] = (
            (g["gap_pct"].between(BACKTEST_MIN_PCT, BACKTEST_MAX_PCT))
            & (g["volume"] >= BACKTEST_MIN_DAILY_VOL)
            & g["prior_close"].notna()
        )
        if g["passes"].any():
            sel = g[g["passes"]].copy()
            for _, r in sel.iterrows():
                picks_rows.append({
                    "date_utc": r["date"].isoformat(),
                    "ticker": ticker,
                    "gap_pct": round(r["gap_pct"], 3),
                    "volume": int(r["volume"]),
                    "open": float(r["open"]),
                    "close": float(r["close"]),
                    "day_ret_pct": round(r["day_ret_pct"], 3),
                })

        # build per-day summary later

    picks = pd.DataFrame(picks_rows)
    if picks.empty:
        return picks, pd.DataFrame()

    # Per-day summary
    picks["date_only_utc"] = pd.to_datetime(picks["date_utc"]).dt.date
    for day, grp in picks.groupby("date_only_utc"):
        n = len(grp)
        avg_gap = grp["gap_pct"].mean()
        avg_ret = grp["day_ret_pct"].mean()
        win_rate = (grp["day_ret_pct"] > 0).mean() if n else 0.0
        day_rows.append({
            "date_utc": str(day),
            "num_picks": int(n),
            "avg_gap_pct": round(avg_gap, 3),
            "avg_day_ret_pct": round(avg_ret, 3),
            "win_rate": round(100.0 * win_rate, 2)
        })
    day_summary = pd.DataFrame(day_rows).sort_values("date_utc").tail(days)

    return picks, day_summary


# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="Real-time pre-market scanner + backtest")
    ap.add_argument("--backtest", action="store_true", help="Run 30-day backtest (proxy open-gap)")
    ap.add_argument("--days", type=int, default=BACKTEST_DAYS, help="Backtest lookback days (default 30)")
    ap.add_argument("--no-news", action="store_true", help="Disable news sentiment filter in real-time")
    args = ap.parse_args()

    symbols = ensure_universe_cache()
    if not symbols:
        print("No symbols found. Check your input files.")
        sys.exit(1)

    if args.backtest:
        print(f"[Backtest] Running proxy backtest for last {args.days} trading days on {len(symbols)} symbols...")
        picks, day_summary = backtest(symbols, days=args.days)
        picks.to_csv("backtest_picks.csv", index=False)
        day_summary.to_csv("backtest_results.csv", index=False)
        print(f"[Backtest] Picks rows: {len(picks)} -> backtest_picks.csv")
        print(f"[Backtest] Day summary rows: {len(day_summary)} -> backtest_results.csv")
        if day_summary.empty:
            print("[Backtest] WARNING: No picks with the proxy rules; consider widening thresholds.")
        return

    # Real-time analysis
    global USE_NEWS
    if args.no_news:
        USE_NEWS = False

    ist_now = get_current_ist().strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[Real-time] IST now: {ist_now}. Scanning {len(symbols)} symbols...")
    full, passed = realtime_scan(symbols)
    full.to_csv("analysis_full.csv", index=False)
    passed.to_csv("analysis_passed.csv", index=False)
    print(f"[Real-time] Saved analysis_full.csv ({len(full)} rows).")
    print(f"[Real-time] Saved analysis_passed.csv ({len(passed)} rows, after news={USE_NEWS}).")
    if passed.empty:
        print("[Real-time] No passes. See analysis_full.csv 'reason' column; consider widening thresholds or lowering MIN_PRE_VOL.")

if __name__ == "__main__":
    main()
