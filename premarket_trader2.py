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

Usage:
  python premarket_trader.py --no-news
  python premarket_trader.py --backtest --days 30
  python premarket_trader.py
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import List, Tuple, Optional

import pandas as pd
from dateutil import tz
from dateutil.tz import tzutc, gettz
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from yahooquery import Ticker

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# -------------------------
# Config (tune as needed)
# -------------------------
NASDAQ_FILE = "nasdaqlisted.txt"   # pipe-delimited; first column = Symbol
OTHER_FILE = "otherlisted.txt"     # pipe-delimited; first column = ACT Symbol
UNIVERSE_CACHE = "all_tickers.csv"

BATCH_SIZE = 80  # yahooquery multi-symbol batch

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
# Globals
# -------------------------
_analyzer = SentimentIntensityAnalyzer()


# ---------- Utils ----------
def now_utc_iso() -> str:
    return datetime.now(tzutc()).isoformat()


def get_current_ist() -> datetime:
    return datetime.now(gettz("Asia/Kolkata"))


def ensure_universe_cache() -> List[str]:
    """Read tickers from the two pipe files and cache to CSV (all_tickers.csv)."""
    if os.path.exists(UNIVERSE_CACHE):
        logging.info(f"Loading universe from cache: {UNIVERSE_CACHE}")
        df = pd.read_csv(UNIVERSE_CACHE)
        if "symbol" in df.columns:
            syms = df["symbol"].dropna().astype(str).str.upper().tolist()
            logging.info(f"Loaded {len(syms)} symbols from cache.")
            return syms
        else:
            logging.warning("Universe cache exists but missing 'symbol' column; rebuilding.")

    logging.info(f"Reading input files: {NASDAQ_FILE} and {OTHER_FILE}")
    if not os.path.exists(NASDAQ_FILE) or not os.path.exists(OTHER_FILE):
        logging.error("Input files not found. Ensure nasdaqlisted.txt and otherlisted.txt exist.")
        return []

    # Read pipe-delimited files (headers expected)
    nas = pd.read_csv(NASDAQ_FILE, sep="|", dtype=str, encoding="utf-8", errors="ignore")
    oth = pd.read_csv(OTHER_FILE, sep="|", dtype=str, encoding="utf-8", errors="ignore")

    # Remove test issues if present
    if "Test Issue" in nas.columns:
        nas = nas[nas["Test Issue"].astype(str).str.upper() != "Y"]
    if "Test Issue" in oth.columns:
        oth = oth[oth["Test Issue"].astype(str).str.upper() != "Y"]

    nas_sym_col = nas.columns[0]
    oth_sym_col = oth.columns[0]

    nas_symbols = nas[nas_sym_col].dropna().astype(str).str.upper()
    oth_symbols = oth[oth_sym_col].dropna().astype(str).str.upper()

    symbols = pd.Series(pd.concat([nas_symbols, oth_symbols], ignore_index=True)).drop_duplicates().tolist()
    logging.info(f"Total unique symbols collected: {len(symbols)}")
    pd.DataFrame({"symbol": symbols}).to_csv(UNIVERSE_CACHE, index=False)
    logging.info(f"Saved universe cache to {UNIVERSE_CACHE}")
    return symbols


def chunked(lst: List[str], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# ---------- News sentiment ----------
def _score_titles_vader(titles: List[str]) -> Optional[float]:
    if not titles:
        return None
    scores = [_analyzer.polarity_scores(t)["compound"] for t in titles if t]
    return (sum(scores) / len(scores)) if scores else None


def _fetch_news_sentiment(symbol: str, hours_back: int) -> Tuple[str, Optional[float], bool, int]:
    cutoff = datetime.now(tzutc()).timestamp() - hours_back * 3600
    tries = 0
    while tries <= NEWS_RETRIES:
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
        except Exception as exc:
            tries += 1
            time.sleep(0.2)
    return symbol, None, False, 0


def enrich_with_news(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(f"Enriching {len(df)} tickers with news sentiment (threads={NEWS_MAX_WORKERS})...")
    if df.empty:
        df["news_avg_compound"] = None
        df["news_articles_used"] = 0
        df["positive_news"] = False
        return df
    symbols = df["ticker"].tolist()
    results = []
    with ThreadPoolExecutor(max_workers=min(NEWS_MAX_WORKERS, max(1, len(symbols)))) as ex:
        futures = [ex.submit(_fetch_news_sentiment, s, NEWS_LOOKBACK_HOURS) for s in symbols]
        for f in as_completed(futures):
            results.append(f.result())
    news_df = pd.DataFrame(results, columns=["ticker", "news_avg_compound", "positive_news", "news_articles_used"])
    merged = df.merge(news_df, on="ticker", how="left")
    return merged


# ---------- Real-time scan ----------
def realtime_scan(symbols: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Query yahooquery.price, apply filters, and return:
      (full_df_with_reasons, passed_df)
    """
    logging.info(f"Starting real-time scan for {len(symbols)} symbols...")
    rows = []
    for batch in chunked(symbols, BATCH_SIZE):
        logging.info(
            f"Processing batch of {len(batch)} symbols: "
            f"(batch {symbols.index(batch[0]) // BATCH_SIZE + 1} of {((len(symbols)-1)//BATCH_SIZE)+1})"
            f"{', '.join(batch[:5])}{'...' if len(batch) > 5 else ''} "
        )
        try:
            tk = Ticker(" ".join(batch))
            price_map = tk.price
        except Exception as exc:
            logging.warning(f"Batch request failed: {exc}; sleeping briefly and continuing.")
            time.sleep(1.0)
            # attempt one-by-one fallback
            price_map = {}

        for s in batch:
            p = {}
            try:
                if isinstance(price_map, dict):
                    p = price_map.get(s, {}) or {}
                else:
                    # sometimes price_map is single-dict when batch size == 1
                    p = {}
            except Exception:
                p = {}

            if not isinstance(p, dict):
                rows.append({
                    "timestamp_utc": now_utc_iso(),
                    "ticker": s,
                    "pre_market_price": None,
                    "regular_prev_close": None,
                    "pre_change_pct": None,
                    "pre_volume": None,
                    "reason": "Bad price payload",
                    "meets_filter": False
                })
                continue

            pre_pct = p.get("preMarketChangePercent")
            pre_vol = p.get("preMarketVolume")
            pre_price = p.get("preMarketPrice")
            prev_close = p.get("regularMarketPreviousClose") or p.get("previousClose")

            reason = None
            meets = True
            if pre_pct is None:
                meets = False
                reason = "No pre-market data"
            elif not (MIN_PCT <= pre_pct <= MAX_PCT):
                meets = False
                reason = f"Change {pre_pct:.2f}% out of range"
            elif pre_vol is None or (isinstance(pre_vol, (int, float)) and pre_vol < MIN_PRE_VOL):
                meets = False
                reason = f"Volume too low ({pre_vol})"

            rows.append({
                "timestamp_utc": now_utc_iso(),
                "ticker": s,
                "pre_market_price": pre_price,
                "regular_prev_close": prev_close,
                "pre_change_pct": pre_pct,
                "pre_volume": pre_vol,
                "reason": "Pass" if meets else reason,
                "meets_filter": bool(meets),
            })

        # polite pause between batches
        time.sleep(0.25)

    full = pd.DataFrame(rows)
    passed = full[full["meets_filter"] == True].copy()

    if USE_NEWS and not passed.empty:
        passed = enrich_with_news(passed)
        # keep if positive or no-news (if no articles found we keep by default)
        passed = passed[(passed["positive_news"] == True) | (passed["news_articles_used"].fillna(0) == 0)]

    return full, passed


# ---------- Backtest (proxy) ----------
def backtest(symbols: List[str], days: int = BACKTEST_DAYS) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Proxy pre-market gap with open-gap vs prior close.
    Picks: BACKTEST_MIN_PCT <= gap% <= BACKTEST_MAX_PCT and daily volume >= BACKTEST_MIN_DAILY_VOL.
    P/L: buy at open, sell at close (same day).
    """
    logging.info(f"Starting backtest for {len(symbols)} symbols, lookback {days} days...")
    period_days = max(days + 10, 60)
    # fetch history - note: large symbol lists may cause failures; consider sampling if rate-limited
    try:
        tk = Ticker(" ".join(symbols))
        hist = tk.history(period=f"{period_days}d", interval="1d")
    except Exception as exc:
        logging.error(f"Failed to fetch historical data for backtest: {exc}")
        return pd.DataFrame(), pd.DataFrame()

    if hist is None or (isinstance(hist, pd.DataFrame) and hist.empty):
        logging.warning("No historical data returned for backtest.")
        return pd.DataFrame(), pd.DataFrame()

    # Normalize historical DataFrame to columns: ticker, date, open, close, volume
    if isinstance(hist, dict):
        # convert dict-of-lists to DataFrame
        rows = []
        for sym, recs in hist.items():
            df_sym = pd.DataFrame(recs)
            if df_sym.empty:
                continue
            df_sym["ticker"] = sym
            rows.append(df_sym)
        hist_df = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    elif isinstance(hist, pd.DataFrame):
        hist_df = hist.reset_index()
        # if Yahoo returned MultiIndex (symbol, date), ensure 'symbol' col exists
        if "symbol" not in hist_df.columns and isinstance(hist.index, pd.MultiIndex):
            # attempt to extract from index
            hist_df = hist.reset_index()
    else:
        hist_df = pd.DataFrame()

    if hist_df.empty:
        logging.warning("Historical DataFrame empty after normalization.")
        return pd.DataFrame(), pd.DataFrame()

    # Ensure date column is present
    if "date" not in hist_df.columns:
        # some responses use 'indexed' or DateTime index; try to infer
        if hist_df.index.name == "date":
            hist_df = hist_df.reset_index()
        else:
            hist_df["date"] = pd.to_datetime(hist_df.get("formatted_date") or hist_df.get("datetime") or pd.NaT)

    # Normalize column names
    for col in ("open", "close", "volume"):
        if col not in hist_df.columns:
            # try uppercase variants
            if col.upper() in hist_df.columns:
                hist_df[col] = hist_df[col.upper()]

    hist_df = hist_df.rename(columns={"symbol": "ticker"})
    # Ensure date is datetime with UTC
    hist_df["date"] = pd.to_datetime(hist_df["date"], errors="coerce")
    hist_df = hist_df.dropna(subset=["date", "ticker", "open", "close", "volume"])
    hist_df["date"] = hist_df["date"].dt.tz_localize(tzutc(), ambiguous='NaT', nonexistent='NaT')

    picks_rows = []
    day_rows = []

    # compute per-ticker gaps and returns
    for ticker, g in hist_df.groupby("ticker"):
        g = g.sort_values("date").reset_index(drop=True)
        if len(g) < 2:
            continue
        g["prior_close"] = g["close"].shift(1)
        g["gap_pct"] = (g["open"] - g["prior_close"]) / g["prior_close"] * 100.0
        g["day_ret_pct"] = (g["close"] - g["open"]) / g["open"] * 100.0
        g["passes"] = (
            g["prior_close"].notna() &
            g["gap_pct"].between(BACKTEST_MIN_PCT, BACKTEST_MAX_PCT) &
            (g["volume"] >= BACKTEST_MIN_DAILY_VOL)
        )
        sel = g[g["passes"]].copy()
        for _, r in sel.iterrows():
            picks_rows.append({
                "date_utc": r["date"].isoformat(),
                "ticker": ticker,
                "gap_pct": round(float(r["gap_pct"]), 3),
                "volume": int(r["volume"]),
                "open": float(r["open"]),
                "close": float(r["close"]),
                "day_ret_pct": round(float(r["day_ret_pct"]), 3),
            })

    picks = pd.DataFrame(picks_rows)
    if picks.empty:
        logging.info("Backtest generated no picks. Consider widening thresholds.")
        return picks, pd.DataFrame()

    picks["date_only_utc"] = pd.to_datetime(picks["date_utc"]).dt.date
    for day, grp in picks.groupby("date_only_utc"):
        n = len(grp)
        avg_gap = grp["gap_pct"].mean()
        avg_ret = grp["day_ret_pct"].mean()
        win_rate = (grp["day_ret_pct"] > 0).mean() if n else 0.0
        day_rows.append({
            "date_utc": str(day),
            "num_picks": int(n),
            "avg_gap_pct": round(float(avg_gap), 3),
            "avg_day_ret_pct": round(float(avg_ret), 3),
            "win_rate": round(100.0 * win_rate, 2)
        })

    day_summary = pd.DataFrame(day_rows).sort_values("date_utc").tail(days)
    logging.info(f"Backtest complete. Picks: {len(picks)}, Day summaries: {len(day_summary)}")
    return picks, day_summary


# ---------- CLI Entrypoint ----------
def main():
    ap = argparse.ArgumentParser(description="Real-time pre-market scanner + backtest")
    ap.add_argument("--backtest", action="store_true", help="Run 30-day backtest (proxy open-gap)")
    ap.add_argument("--days", type=int, default=BACKTEST_DAYS, help="Backtest lookback days (default 30)")
    ap.add_argument("--no-news", action="store_true", help="Disable news sentiment filter in real-time")
    args = ap.parse_args()

    global USE_NEWS
    if args.no_news:
        USE_NEWS = False
        logging.info("News sentiment disabled (--no-news).")

    symbols = ensure_universe_cache()
    if not symbols:
        logging.error("No symbols found. Check your input files and paths.")
        sys.exit(1)

    if args.backtest:
        logging.info(f"[Backtest] Running proxy backtest for last {args.days} trading days on {len(symbols)} symbols...")
        picks, day_summary = backtest(symbols, days=args.days)
        picks.to_csv("backtest_picks.csv", index=False)
        day_summary.to_csv("backtest_results.csv", index=False)
        logging.info(f"[Backtest] Picks rows: {len(picks)} -> backtest_picks.csv")
        logging.info(f"[Backtest] Day summary rows: {len(day_summary)} -> backtest_results.csv")
        if day_summary.empty:
            logging.warning("[Backtest] No picks found; consider widening thresholds.")
        return

    # Real-time analysis
    ist_now = get_current_ist().strftime("%Y-%m-%d %H:%M:%S %Z")
    logging.info(f"[Real-time] IST now: {ist_now}. Scanning {len(symbols)} symbols...")
    full, passed = realtime_scan(symbols)
    full.to_csv("analysis_full.csv", index=False)
    passed.to_csv("analysis_passed.csv", index=False)
    logging.info(f"[Real-time] Saved analysis_full.csv ({len(full)} rows).")
    logging.info(f"[Real-time] Saved analysis_passed.csv ({len(passed)} rows, after news={USE_NEWS}).")
    if passed.empty:
        logging.info("[Real-time] No passes. See analysis_full.csv 'reason' column; consider widening thresholds or lowering MIN_PRE_VOL.")


if __name__ == "__main__":
    main()
