import yfinance as yf
import pandas as pd
import argparse
import os

NASDAQ_FILE = "nasdaqlisted.txt"
OTHER_FILE = "otherlisted.txt"

def load_tickers():
    tickers = []
    for file in [NASDAQ_FILE, OTHER_FILE]:
        if os.path.exists(file):
            with open(file) as f:
                next(f)  # skip header line
                for line in f:
                    parts = line.strip().split("|")
                    if parts and parts[0] and parts[0] != "Symbol":
                        tickers.append(parts[0].strip())
    return list(set(tickers))

def analyze_tickers(tickers, N=5, threshold=6.0):
    results = []
    for ticker in tickers:
        try:
            df = yf.download(
                                ticker,
                                period=f"{N+5}d",
                                interval="1d",
                                auto_adjust=False,   # explicit to avoid warning
                                progress=False
                            )

            if df.empty or len(df) < N+1:
                continue

            # Take last N+1 trading days
            df = df.tail(N+1)
            base_price = df.iloc[0]["Close"]

            changes = []
            valid = True
            for i in range(1, len(df)):
                pct_change = ((df.iloc[i]["Close"] - base_price) / base_price) * 100
                changes.append(round(pct_change, 2))
                if pct_change < threshold:
                    valid = False
                    break

            if valid and len(changes) == N:
                latest = df.iloc[-1]
                results.append({
                    "Ticker": ticker,
                    **{f"Day-{N-i} Change %": changes[i] for i in range(N)},
                    "Current Price": round(latest["Close"], 2),
                    "Volume": int(latest["Volume"])
                })
        except Exception:
            continue
    return results

def save_results(results, output="csv"):
    df = pd.DataFrame(results)
    if df.empty:
        print("No stocks matched criteria.")
        return

    if output == "csv":
        df.to_csv("filtered_stocks.csv", index=False)
        print("Results saved to filtered_stocks.csv")
    elif output == "json":
        df.to_json("filtered_stocks.json", orient="records", indent=2)
        print("Results saved to filtered_stocks.json")
    else:
        print(df.to_string(index=False))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=5, help="Number of consecutive days (N)")
    parser.add_argument("--threshold", type=float, default=6.0, help="Percentage threshold")
    parser.add_argument("--format", type=str, default="csv", choices=["csv", "json", "table"], help="Output format")
    args = parser.parse_args()

    tickers = load_tickers()
    results = analyze_tickers(tickers, N=args.days, threshold=args.threshold)
    save_results(results, output=args.format)
