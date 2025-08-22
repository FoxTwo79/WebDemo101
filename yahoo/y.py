import requests

url = "https://query1.finance.yahoo.com/v1/test/getcrumb"

headers = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "text/plain",
    "priority": "u=1, i",
    "sec-ch-ua": "\"Google Chrome\";v=\"135\", \"Not-A.Brand\";v=\"8\", \"Chromium\";v=\"135\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Linux\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "cookie": "A1=d=AQABBGQmqGgCEFCiRlPR6wXMrdcJupD5Tv8FEgEBAQF3qWiyaK9E8HgB_eMCAA&S=AQAAAtbfF62_iqKuT-GDInKXckw; A3=d=AQABBGQmqGgCEFCiRlPR6wXMrdcJupD5Tv8FEgEBAQF3qWiyaK9E8HgB_eMCAA&S=AQAAAtbfF62_iqKuT-GDInKXckw; A1S=d=AQABBGQmqGgCEFCiRlPR6wXMrdcJupD5Tv8FEgEBAQF3qWiyaK9E8HgB_eMCAA&S=AQAAAtbfF62_iqKuT-GDInKXckw",
    "Referer": "https://finance.yahoo.com/quote/AAPL/",
    "Referrer-Policy": "no-referrer-when-downgrade"
}

response = requests.get(url, headers=headers)

# Print the response
print(f"Status Code: {response.status_code}")
print(f"Response Text: {response.text}")

# import requests
# import pandas as pd

# def get_crumb():
#     """Fetch crumb from Yahoo Finance crumb endpoint."""
#     url = "https://query1.finance.yahoo.com/v1/test/getcrumb"
#     headers = {
#         "content-type": "text/plain",
#         "sec-ch-ua": "\"Google Chrome\";v=\"135\", \"Not-A.Brand\";v=\"8\", \"Chromium\";v=\"135\"",
#         "sec-ch-ua-mobile": "?0",
#         "sec-ch-ua-platform": "\"Linux\"",
#         "Referer": "https://finance.yahoo.com/quote/HPKEW/",
#         "Referrer-Policy": "no-referrer-when-downgrade"
#     }
#     resp = requests.get(url, headers=headers, timeout=20)
#     resp.raise_for_status()
#     return resp.text.strip()

# def fetch_quotes(symbols, crumb):
#     """Fetch live quotes for given symbols with crumb."""
#     base_url = "https://query1.finance.yahoo.com/v7/finance/quote"
#     params = {
#         "symbols": ",".join(symbols),
#         "crumb": crumb,
#         "formatted": "false",
#         "region": "US",
#         "lang": "en-US",
#         "fields": (
#             "currency,fromCurrency,toCurrency,exchangeTimezoneName,exchangeTimezoneShortName,"
#             "gmtOffSetMilliseconds,regularMarketChange,regularMarketChangePercent,regularMarketPrice,"
#             "regularMarketTime,preMarketChange,preMarketChangePercent,preMarketPrice,preMarketTime,"
#             "priceHint,postMarketChange,postMarketChangePercent,postMarketPrice,postMarketTime,"
#             "extendedMarketChange,extendedMarketChangePercent,extendedMarketPrice,extendedMarketTime,"
#             "overnightMarketChange,overnightMarketChangePercent,overnightMarketPrice,overnightMarketTime"
#         )
#     }
#     resp = requests.get(base_url, params=params, timeout=30)
#     resp.raise_for_status()
#     data = resp.json()
#     return data.get("quoteResponse", {}).get("result", [])

# def save_to_csv(quotes, filename="yahoo_quotes.csv"):
#     """Save quotes to CSV."""
#     if not quotes:
#         print("No data to save.")
#         return
#     df = pd.DataFrame(quotes)
#     df.to_csv(filename, index=False)
#     print(f"✅ Saved {len(df)} symbols to {filename}")
#     print(df[["symbol", "regularMarketPrice", "preMarketPrice", "postMarketPrice"]])

# if __name__ == "__main__":
#     symbols = ["GN.CO", "HPKEW", "TGT"]
#     crumb = get_crumb()
#     quotes = fetch_quotes(symbols, crumb)
#     save_to_csv(quotes)
