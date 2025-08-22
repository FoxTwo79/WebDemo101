import requests

def get_yahoo_crumb():
    session = requests.Session()
    
    # Hit Yahoo Finance homepage first to set cookies
    session.get("https://finance.yahoo.com")
    
    # Now request the crumb endpoint
    url = "https://query1.finance.yahoo.com/v1/test/getcrumb"
    resp = session.get(url)
    
    if resp.status_code == 200:
        crumb = resp.text.strip()
        return crumb, session.cookies.get_dict()
    else:
        raise Exception(f"Failed to fetch crumb: {resp.status_code} {resp.text}")

if __name__ == "__main__":
    crumb, cookies = get_yahoo_crumb()
    print("Crumb:", crumb)
    print("Cookies:", cookies)

    # Example usage: download historical data
    symbol = "AAPL"
    url = f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1=1609459200&period2=1692748800&interval=1d&events=history&crumb={crumb}"
    r = requests.get(url, cookies=cookies)
    print("Data sample:\n", r.text[:500])
