#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Web scrape Yahoo Finance for one ticker:
- Pre-market, regular, post-market prices
- Volume, previous close
"""

import requests
from bs4 import BeautifulSoup

def scrape_yahoo_finance(ticker):
    url = f"https://finance.yahoo.com/quote/{ticker}?p={ticker}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/114.0.0.0 Safari/537.36"
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch page: {response.status_code}")

    soup = BeautifulSoup(response.text, "html.parser")

    def find_span(text_label):
        tag = soup.find("td", {"data-test": text_label})
        if tag and tag.find("span"):
            return tag.find("span").text.strip()
        return None

    # Regular price
    regular_price_tag = soup.find("fin-streamer", {"data-field": "regularMarketPrice"})
    regular_price = regular_price_tag.text if regular_price_tag else None

    # Pre-market price (may not exist outside pre-market hours)
    pre_market_tag = soup.find("fin-streamer", {"data-field": "preMarketPrice"})
    pre_market = pre_market_tag.text if pre_market_tag else None

    # Post-market price
    post_market_tag = soup.find("fin-streamer", {"data-field": "postMarketPrice"})
    post_market = post_market_tag.text if post_market_tag else None

    previous_close = find_span("PREV_CLOSE-value")
    volume = find_span("TD_VOLUME-value")

    return {
        "symbol": ticker,
        "regular_price": regular_price,
        "pre_market_price": pre_market,
        "post_market_price": post_market,
        "previous_close": previous_close,
        "volume": volume
    }

if __name__ == "__main__":
    ticker = "AAPL"
    data = scrape_yahoo_finance(ticker)
    for k, v in data.items():
        print(f"{k}: {v}")



# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-

# """
# Compare Yahooquery and yfinance for one ticker:
# - Pre-market, regular, post-market prices
# - Volume, previous close, market state
# """

# import json
# from yahooquery import Ticker as YQ_Ticker
# import yfinance as yf

# TICKER_SYMBOL = "AUDC"

# print(f"\n=== Testing Ticker: {TICKER_SYMBOL} ===\n")

# # ---------------- Yahooquery ---------------- #
# print(">>> Yahooquery Data")
# yq_tkr = YQ_Ticker(TICKER_SYMBOL)
# yq_price_data = yq_tkr.price

# print(json.dumps(yq_price_data, indent=2))  # full raw JSON

# yq_pdata = yq_price_data.get(TICKER_SYMBOL, {})
# print("\n--- Extracted Yahooquery Fields ---")
# print("Symbol:           ", yq_pdata.get("symbol"))
# print("Currency:         ", yq_pdata.get("currency"))
# print("Market State:     ", yq_pdata.get("marketState"))
# print("Pre-Market Price: ", yq_pdata.get("preMarketPrice"))
# print("Regular Price:    ", yq_pdata.get("regularMarketPrice"))
# print("Post-Market Price:", yq_pdata.get("postMarketPrice"))
# print("Previous Close:   ", yq_pdata.get("regularMarketPreviousClose"))
# print("Volume:           ", yq_pdata.get("regularMarketVolume"))

# # ---------------- yfinance ---------------- #
# print("\n>>> yfinance Data")
# yf_tkr = yf.Ticker(TICKER_SYMBOL)
# yf_info = yf_tkr.info  # dictionary with all fields

# # Optional: print all keys & values
# # for k, v in yf_info.items():
# #     print(f"{k}: {v}")

# print("\n--- Extracted yfinance Fields ---")
# print("Symbol:           ", yf_info.get("symbol"))
# print("Currency:         ", yf_info.get("currency"))
# print("Market State:     ", yf_info.get("marketState"))
# print("Pre-Market Price: ", yf_info.get("preMarketPrice"))
# print("Regular Price:    ", yf_info.get("regularMarketPrice"))
# print("Post-Market Price:", yf_info.get("postMarketPrice"))
# print("Previous Close:   ", yf_info.get("regularMarketPreviousClose"))
# print("Volume:           ", yf_info.get("regularMarketVolume"))

# # ---------------- Intraday data from yfinance (optional) ---------------- #
# print("\n>>> yfinance Intraday Prices (last 5 minutes)")
# hist = yf_tkr.history(period="1d", interval="1m", prepost=True)
# print(hist.tail())
