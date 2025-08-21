import requests
import pandas as pd
import numpy as np
import json
import time
import csv
from datetime import datetime, timedelta
import os
import urllib.parse
import warnings
warnings.filterwarnings('ignore')

class TradingAnalyzer:
    def __init__(self):
        self.base_url = "https://marketchameleon.com/EquityScreener/EquityScreenerData"
        self.session = requests.Session()
        
        # You need to update these with your actual session cookies
        self.headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "priority": "u=1, i",
            "sec-ch-ua": "\"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"138\", \"Google Chrome\";v=\"138\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "x-mobdisp": "false",
            "x-requested-with": "XMLHttpRequest",
            "cookie": "_ga=GA1.1.752058074.1755606665; PAPVisitorId=T2pnDCV3vwahlRmZ0ZrY5CYI9Rq9CTqZ; ASP.NET_SessionId=ch2acoct2lkjrjxxm0enwibl; RT=\"z=1&dm=marketchameleon.com&si=0nrfwwhxjiol&ss=meiivnwu&sl=0&tt=0\"; __RequestVerificationToken=GZ5SZVwUzaOYdEYc05mB_3qm2W8egAymBDKNJ4cPrBCY8G9pf77W_JTwu4nJ4ZGWXAwQEERetLRF1nkt71WXo8UDUrK_mO_5CKNNkNca6eI1; _ga_CXRDD1LJF1=GS2.1.s1755666634$o2$g1$t1755667552$j60$l0$h0; __stripe_mid=084a0ead-77af-42d6-be75-1f841b44ae7b714f95; v1=79481629; .AspNet.ApplicationCookie=11kCooOd54R9yMYDZH3U7PSFXuDVxMbHqRkELFx-jZPaukg9qPbIJmW5rWtPLwwZQEcI6sc35TCji_AJgLe4yvtGpLyJV1YttlvSwNJXUT2JRrFsk70h4aRHJdeN_gpDz4vbbCwJxqcFU-79vgvAMop0DSz8jX6502s4ZWWxlSVFyPm3W11ZMDmrh2DMrdm4v0wTJE5USiKOGc7w4_Mz2dQKBzgCHeIylcxDcL9TEgSe2KQuYwX2KdbB73UUWU-s9mYK1QFbvWH0s3TGIRMuCqBtVWjLbrHFyjDUrt9hsLuxsBYpWMTzWS-UDUYKRfGV4fDGZs8We5EVmMzVINWRaoXhlxu5jj3kMjVVmO2VFs2UF-4QQCLrWWm5Nd6L5F3wxk5vJur6gSX0BfOBg9b8hmYpysXokK55_E3e7DXgZ_50eYzMKhGetn7AQVn-CkKzpZbtMSwgYZZzojmmc0h6hEFQAhvN5lY31gOS1M-fLNI; ak_bmsc=4EE111B45CC95D71843A8251BDA867DC~000000000000000000000000000000~YAAQp5YRYLuDM6qYAQAActVQxhwMvMGGD1Wfh31Qu76N0DHBnthzwANFuV1PI54mAJQpXEsjjFcUNy5ieDT6VociZIMRM3iMNgYuidxggstPrAgTnEsHpiVvUgb71oBGkWf5sz3PLS1otScQ1VZ0M42fmPXipvm94xisHcudGFjDMyKninuuDc0JypF52Qz48A1jByviOTjn52yQiuki9AoppRQpf2essVi874ewp5K5gB7DI8X3ffBxFRQ2p3dro1J15AIsbe/c/NMUqa7gDnSU7VI9MJoVK9Mir2u7iPa/Skia/2RySdYzBY8VoEjN451cukH/0eK3PG3k2qHB6zK381R6gtC/RkjphHM+G/nezkVrfMkpHFDrt8rdJux5U4tSiJzoXPvxWft/ABgTaDpLFg==; bm_sv=57DF9ED0C05B4C2D85254727541A03~YAAQHYpRaE1x/7OYAQAAnhpYxhwscliElNc6KvYN4ez39IOeA/a6zTPL+Zx9q7e5xTpeCVv9/o45Ljr9ZSEveqYO5PeJ58/j+SwTcVgFojAE+jjnYhn344N/Uu+NaFhbqK9AVWzOyKMi6F5oiv4QlVhX8i8u26HtVrUC+t7i5edvGZCrNk6O97KI51zlVLI/sbiV1k0yjnrLnYOwyyMdGB7QyZqqrRk/dhfJnfiqFeyNbLRWgYrHlzfMJWGeEaCDlmlxJ56V798g~1",  # Update this with your actual cookies
            "Referer": "https://marketchameleon.com/Screeners/Stocks"
        }
        
        # Alert thresholds
        self.alert_config = {
            'price_change_threshold': 0.1,  # 10%
            'volume_spike_threshold': 5.0,  # 5x average volume
            'rsi_overbought': 70,
            'rsi_oversold': 30,
            'high_volatility_threshold': 100  # 100% annualized volatility
        }
        
        # Initialize data storage
        self.historical_data = []
        self.alerts_log = []
        
    def build_request_payload(self, length=5, start=0):
        """Build the request payload for the API"""
        return {
            "draw": "1",
            "start": str(start),
            "length": str(length),
            "search[value]": "",
            "search[regex]": "false",
            # Column definitions (simplified - add more as needed)
            "columns[0][data]": "Symbol",
            "columns[0][name]": "c0",
            "columns[0][searchable]": "true",
            "columns[0][orderable]": "true",
            "columns[1][data]": "BD.Name",
            "columns[1][name]": "c1",
            "columns[2][data]": "BD.Px",
            "columns[2][name]": "c2",
            "columns[3][data]": "BD.PxChgPct",
            "columns[3][name]": "c3",
            "columns[4][data]": "BD.Volume",
            "columns[4][name]": "c6",
            "columns[5][data]": "BD.RelVolume",
            "columns[5][name]": "c7",
            "columns[6][data]": "BD.MarketCap",
            "columns[6][name]": "c8",
            "order[0][column]": "3",
            "order[0][dir]": "desc"
        }

    def fetch_market_data(self, length=5):
        """Fetch market data from the API"""
        try:
            payload = self.build_request_payload(length=length)

            # Properly encode the payload
            encoded_payload = urllib.parse.urlencode(payload, doseq=True)
            encoded_payload = 'draw=4&columns%5B0%5D%5Bdata%5D=Symbol&columns%5B0%5D%5Bname%5D=c0&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=true&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=BD.Name&columns%5B1%5D%5Bname%5D=c1&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=true&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=BD.Px&columns%5B2%5D%5Bname%5D=c2&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=true&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=BD.PxChgPct&columns%5B3%5D%5Bname%5D=c3&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=true&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=BD.Volume&columns%5B4%5D%5Bname%5D=c6&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=true&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=BD.AvgVolume&columns%5B5%5D%5Bname%5D=c51&columns%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=true&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B6%5D%5Bdata%5D=BD.RelVolume&columns%5B6%5D%5Bname%5D=c7&columns%5B6%5D%5Bsearchable%5D=true&columns%5B6%5D%5Borderable%5D=true&columns%5B6%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B6%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B7%5D%5Bdata%5D=BD.MarketCap&columns%5B7%5D%5Bname%5D=c8&columns%5B7%5D%5Bsearchable%5D=true&columns%5B7%5D%5Borderable%5D=true&columns%5B7%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B7%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B8%5D%5Bdata%5D=BD.DivYield&columns%5B8%5D%5Bname%5D=c9&columns%5B8%5D%5Bsearchable%5D=true&columns%5B8%5D%5Borderable%5D=true&columns%5B8%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B8%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B9%5D%5Bdata%5D=Fm.pe_ratio&columns%5B9%5D%5Bname%5D=c10&columns%5B9%5D%5Bsearchable%5D=true&columns%5B9%5D%5Borderable%5D=true&columns%5B9%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B9%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B10%5D%5Bdata%5D=BD.Chg2D&columns%5B10%5D%5Bname%5D=c53&columns%5B10%5D%5Bsearchable%5D=true&columns%5B10%5D%5Borderable%5D=true&columns%5B10%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B10%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B11%5D%5Bdata%5D=BD.Chg3D&columns%5B11%5D%5Bname%5D=c54&columns%5B11%5D%5Bsearchable%5D=true&columns%5B11%5D%5Borderable%5D=true&columns%5B11%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B11%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B12%5D%5Bdata%5D=BD.Chg4D&columns%5B12%5D%5Bname%5D=c55&columns%5B12%5D%5Bsearchable%5D=true&columns%5B12%5D%5Borderable%5D=true&columns%5B12%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B12%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B13%5D%5Bdata%5D=BD.Chg5D&columns%5B13%5D%5Bname%5D=c56&columns%5B13%5D%5Bsearchable%5D=true&columns%5B13%5D%5Borderable%5D=true&columns%5B13%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B13%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B14%5D%5Bdata%5D=BD.Chg6D&columns%5B14%5D%5Bname%5D=c57&columns%5B14%5D%5Bsearchable%5D=true&columns%5B14%5D%5Borderable%5D=true&columns%5B14%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B14%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B15%5D%5Bdata%5D=BD.Chg7D&columns%5B15%5D%5Bname%5D=c58&columns%5B15%5D%5Bsearchable%5D=true&columns%5B15%5D%5Borderable%5D=true&columns%5B15%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B15%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B16%5D%5Bdata%5D=BD.Chg2Wk&columns%5B16%5D%5Bname%5D=c11&columns%5B16%5D%5Bsearchable%5D=true&columns%5B16%5D%5Borderable%5D=true&columns%5B16%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B16%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B17%5D%5Bdata%5D=BD.Chg3M&columns%5B17%5D%5Bname%5D=c12&columns%5B17%5D%5Bsearchable%5D=true&columns%5B17%5D%5Borderable%5D=true&columns%5B17%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B17%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B18%5D%5Bdata%5D=BD.Chg6M&columns%5B18%5D%5Bname%5D=c13&columns%5B18%5D%5Bsearchable%5D=true&columns%5B18%5D%5Borderable%5D=true&columns%5B18%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B18%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B19%5D%5Bdata%5D=BD.Chg1Y&columns%5B19%5D%5Bname%5D=c14&columns%5B19%5D%5Bsearchable%5D=true&columns%5B19%5D%5Borderable%5D=true&columns%5B19%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B19%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B20%5D%5Bdata%5D=BD.ChgYTD&columns%5B20%5D%5Bname%5D=c15&columns%5B20%5D%5Bsearchable%5D=true&columns%5B20%5D%5Borderable%5D=true&columns%5B20%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B20%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B21%5D%5Bdata%5D=BD.Chg3Y&columns%5B21%5D%5Bname%5D=c200&columns%5B21%5D%5Bsearchable%5D=true&columns%5B21%5D%5Borderable%5D=true&columns%5B21%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B21%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B22%5D%5Bdata%5D=BD.Chg5Y&columns%5B22%5D%5Bname%5D=c201&columns%5B22%5D%5Bsearchable%5D=true&columns%5B22%5D%5Borderable%5D=true&columns%5B22%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B22%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B23%5D%5Bdata%5D=BD.ChgCloseToOpen&columns%5B23%5D%5Bname%5D=c4&columns%5B23%5D%5Bsearchable%5D=true&columns%5B23%5D%5Borderable%5D=true&columns%5B23%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B23%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B24%5D%5Bdata%5D=BD.ChgOpenPx&columns%5B24%5D%5Bname%5D=c5&columns%5B24%5D%5Bsearchable%5D=true&columns%5B24%5D%5Borderable%5D=true&columns%5B24%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B24%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B25%5D%5Bdata%5D=BD.Chg52WLo&columns%5B25%5D%5Bname%5D=c16&columns%5B25%5D%5Bsearchable%5D=true&columns%5B25%5D%5Borderable%5D=true&columns%5B25%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B25%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B26%5D%5Bdata%5D=BD.Chg52WHi&columns%5B26%5D%5Bname%5D=c17&columns%5B26%5D%5Bsearchable%5D=true&columns%5B26%5D%5Borderable%5D=true&columns%5B26%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B26%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B27%5D%5Bdata%5D=BD.ChgMA20&columns%5B27%5D%5Bname%5D=c18&columns%5B27%5D%5Bsearchable%5D=true&columns%5B27%5D%5Borderable%5D=true&columns%5B27%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B27%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B28%5D%5Bdata%5D=BD.ChgMA50&columns%5B28%5D%5Bname%5D=c19&columns%5B28%5D%5Bsearchable%5D=true&columns%5B28%5D%5Borderable%5D=true&columns%5B28%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B28%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B29%5D%5Bdata%5D=BD.ChgMA250&columns%5B29%5D%5Bname%5D=c20&columns%5B29%5D%5Bsearchable%5D=true&columns%5B29%5D%5Borderable%5D=true&columns%5B29%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B29%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B30%5D%5Bdata%5D=BD.IV30&columns%5B30%5D%5Bname%5D=c21&columns%5B30%5D%5Bsearchable%5D=true&columns%5B30%5D%5Borderable%5D=true&columns%5B30%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B30%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B31%5D%5Bdata%5D=BD.IV30Chg&columns%5B31%5D%5Bname%5D=c22&columns%5B31%5D%5Bsearchable%5D=true&columns%5B31%5D%5Borderable%5D=true&columns%5B31%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B31%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B32%5D%5Bdata%5D=BD.OptVolume&columns%5B32%5D%5Bname%5D=c23&columns%5B32%5D%5Bsearchable%5D=true&columns%5B32%5D%5Borderable%5D=true&columns%5B32%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B32%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B33%5D%5Bdata%5D=BD.RelOptVolume&columns%5B33%5D%5Bname%5D=c24&columns%5B33%5D%5Bsearchable%5D=true&columns%5B33%5D%5Borderable%5D=true&columns%5B33%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B33%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B34%5D%5Bdata%5D=BD.IVRank&columns%5B34%5D%5Bname%5D=c25&columns%5B34%5D%5Bsearchable%5D=true&columns%5B34%5D%5Borderable%5D=true&columns%5B34%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B34%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B35%5D%5Bdata%5D=BD.Sector&columns%5B35%5D%5Bname%5D=c26&columns%5B35%5D%5Bsearchable%5D=true&columns%5B35%5D%5Borderable%5D=true&columns%5B35%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B35%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B36%5D%5Bdata%5D=BD.Industry&columns%5B36%5D%5Bname%5D=c27&columns%5B36%5D%5Bsearchable%5D=true&columns%5B36%5D%5Borderable%5D=true&columns%5B36%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B36%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B37%5D%5Bdata%5D=BD.EquityType&columns%5B37%5D%5Bname%5D=c28&columns%5B37%5D%5Bsearchable%5D=true&columns%5B37%5D%5Borderable%5D=true&columns%5B37%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B37%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B38%5D%5Bdata%5D=BD.DivGrowth1Yr&columns%5B38%5D%5Bname%5D=c32&columns%5B38%5D%5Bsearchable%5D=true&columns%5B38%5D%5Borderable%5D=true&columns%5B38%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B38%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B39%5D%5Bdata%5D=BD.DivGrowth3Yr&columns%5B39%5D%5Bname%5D=c33&columns%5B39%5D%5Bsearchable%5D=true&columns%5B39%5D%5Borderable%5D=true&columns%5B39%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B39%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B40%5D%5Bdata%5D=BD.PayoutRatio&columns%5B40%5D%5Bname%5D=c34&columns%5B40%5D%5Bsearchable%5D=true&columns%5B40%5D%5Borderable%5D=true&columns%5B40%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B40%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B41%5D%5Bdata%5D=BD.RSI14&columns%5B41%5D%5Bname%5D=c45&columns%5B41%5D%5Bsearchable%5D=true&columns%5B41%5D%5Borderable%5D=true&columns%5B41%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B41%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B42%5D%5Bdata%5D=BD.DivIncreases3Yr&columns%5B42%5D%5Bname%5D=c46&columns%5B42%5D%5Bsearchable%5D=true&columns%5B42%5D%5Borderable%5D=true&columns%5B42%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B42%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B43%5D%5Bdata%5D=BD.DivDecreases3Yr&columns%5B43%5D%5Bname%5D=c47&columns%5B43%5D%5Bsearchable%5D=true&columns%5B43%5D%5Borderable%5D=true&columns%5B43%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B43%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B44%5D%5Bdata%5D=BD.Vol1Day&columns%5B44%5D%5Bname%5D=c48&columns%5B44%5D%5Bsearchable%5D=true&columns%5B44%5D%5Borderable%5D=true&columns%5B44%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B44%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B45%5D%5Bdata%5D=BD.Vol20Day&columns%5B45%5D%5Bname%5D=c49&columns%5B45%5D%5Bsearchable%5D=true&columns%5B45%5D%5Borderable%5D=true&columns%5B45%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B45%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B46%5D%5Bdata%5D=BD.Vol1Year&columns%5B46%5D%5Bname%5D=c50&columns%5B46%5D%5Bsearchable%5D=true&columns%5B46%5D%5Borderable%5D=true&columns%5B46%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B46%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B47%5D%5Bdata%5D=BD.Skew25DSort&columns%5B47%5D%5Bname%5D=c52&columns%5B47%5D%5Bsearchable%5D=true&columns%5B47%5D%5Borderable%5D=true&columns%5B47%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B47%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B48%5D%5Bdata%5D=BD.Country&columns%5B48%5D%5Bname%5D=c80&columns%5B48%5D%5Bsearchable%5D=true&columns%5B48%5D%5Borderable%5D=true&columns%5B48%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B48%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B49%5D%5Bdata%5D=BD.ShrOut&columns%5B49%5D%5Bname%5D=c90&columns%5B49%5D%5Bsearchable%5D=true&columns%5B49%5D%5Borderable%5D=true&columns%5B49%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B49%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B50%5D%5Bdata%5D=BD.ShrFloat&columns%5B50%5D%5Bname%5D=c91&columns%5B50%5D%5Bsearchable%5D=true&columns%5B50%5D%5Borderable%5D=true&columns%5B50%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B50%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B51%5D%5Bdata%5D=BD.MA_20D&columns%5B51%5D%5Bname%5D=MA_20D&columns%5B51%5D%5Bsearchable%5D=true&columns%5B51%5D%5Borderable%5D=true&columns%5B51%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B51%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B52%5D%5Bdata%5D=BD.MA_50D&columns%5B52%5D%5Bname%5D=MA_50D&columns%5B52%5D%5Bsearchable%5D=true&columns%5B52%5D%5Borderable%5D=true&columns%5B52%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B52%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B53%5D%5Bdata%5D=BD.MA_250D&columns%5B53%5D%5Bname%5D=MA_250D&columns%5B53%5D%5Bsearchable%5D=true&columns%5B53%5D%5Borderable%5D=true&columns%5B53%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B53%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B54%5D%5Bdata%5D=BD.MA_20v50&columns%5B54%5D%5Bname%5D=_20MA_vs_50MA&columns%5B54%5D%5Bsearchable%5D=true&columns%5B54%5D%5Borderable%5D=true&columns%5B54%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B54%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B55%5D%5Bdata%5D=BD.MA_20v250&columns%5B55%5D%5Bname%5D=_20MA_vs_250MA&columns%5B55%5D%5Bsearchable%5D=true&columns%5B55%5D%5Borderable%5D=true&columns%5B55%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B55%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B56%5D%5Bdata%5D=BD.MA_50v250&columns%5B56%5D%5Bname%5D=_50MA_vs_250MA&columns%5B56%5D%5Bsearchable%5D=true&columns%5B56%5D%5Borderable%5D=true&columns%5B56%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B56%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B57%5D%5Bdata%5D=BD.MA_Name&columns%5B57%5D%5Bname%5D=c59&columns%5B57%5D%5Bsearchable%5D=true&columns%5B57%5D%5Borderable%5D=true&columns%5B57%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B57%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B58%5D%5Bdata%5D=BD.StockSD_Out&columns%5B58%5D%5Bname%5D=c60&columns%5B58%5D%5Bsearchable%5D=true&columns%5B58%5D%5Borderable%5D=true&columns%5B58%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B58%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B59%5D%5Bdata%5D=BD.DayVWMinSD&columns%5B59%5D%5Bname%5D=c81&columns%5B59%5D%5Bsearchable%5D=true&columns%5B59%5D%5Borderable%5D=true&columns%5B59%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B59%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B60%5D%5Bdata%5D=BD.DayVWAP&columns%5B60%5D%5Bname%5D=c82&columns%5B60%5D%5Bsearchable%5D=true&columns%5B60%5D%5Borderable%5D=true&columns%5B60%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B60%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B61%5D%5Bdata%5D=BD.DayVWPlusSD&columns%5B61%5D%5Bname%5D=c83&columns%5B61%5D%5Bsearchable%5D=true&columns%5B61%5D%5Borderable%5D=true&columns%5B61%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B61%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B62%5D%5Bdata%5D=BD.PvVWStd&columns%5B62%5D%5Bname%5D=c84&columns%5B62%5D%5Bsearchable%5D=true&columns%5B62%5D%5Borderable%5D=true&columns%5B62%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B62%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B63%5D%5Bdata%5D=BD.PvVWPct&columns%5B63%5D%5Bname%5D=c85&columns%5B63%5D%5Bsearchable%5D=true&columns%5B63%5D%5Borderable%5D=true&columns%5B63%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B63%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B64%5D%5Bdata%5D=BD.VwSD&columns%5B64%5D%5Bname%5D=c86&columns%5B64%5D%5Bsearchable%5D=true&columns%5B64%5D%5Borderable%5D=true&columns%5B64%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B64%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B65%5D%5Bdata%5D=Fm.pe_ratio&columns%5B65%5D%5Bname%5D=c101&columns%5B65%5D%5Bsearchable%5D=true&columns%5B65%5D%5Borderable%5D=true&columns%5B65%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B65%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B66%5D%5Bdata%5D=Fm.pe_normalized_eps&columns%5B66%5D%5Bname%5D=c102&columns%5B66%5D%5Bsearchable%5D=true&columns%5B66%5D%5Borderable%5D=true&columns%5B66%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B66%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B67%5D%5Bdata%5D=Fm.px_to_sales&columns%5B67%5D%5Bname%5D=c103&columns%5B67%5D%5Bsearchable%5D=true&columns%5B67%5D%5Borderable%5D=true&columns%5B67%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B67%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B68%5D%5Bdata%5D=Fm.px_to_bookval&columns%5B68%5D%5Bname%5D=c104&columns%5B68%5D%5Bsearchable%5D=true&columns%5B68%5D%5Borderable%5D=true&columns%5B68%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B68%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B69%5D%5Bdata%5D=Fm.px_to_tangiblebookval&columns%5B69%5D%5Bname%5D=c105&columns%5B69%5D%5Bsearchable%5D=true&columns%5B69%5D%5Borderable%5D=true&columns%5B69%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B69%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B70%5D%5Bdata%5D=Fm.peg_ratio&columns%5B70%5D%5Bname%5D=c106&columns%5B70%5D%5Bsearchable%5D=true&columns%5B70%5D%5Borderable%5D=true&columns%5B70%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B70%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B71%5D%5Bdata%5D=Fm.px_to_cash&columns%5B71%5D%5Bname%5D=c107&columns%5B71%5D%5Bsearchable%5D=true&columns%5B71%5D%5Borderable%5D=true&columns%5B71%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B71%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B72%5D%5Bdata%5D=Fm.px_to_freecashflow&columns%5B72%5D%5Bname%5D=c108&columns%5B72%5D%5Bsearchable%5D=true&columns%5B72%5D%5Borderable%5D=true&columns%5B72%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B72%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B73%5D%5Bdata%5D=Fm.px_to_ebitda&columns%5B73%5D%5Bname%5D=c109&columns%5B73%5D%5Bsearchable%5D=true&columns%5B73%5D%5Borderable%5D=true&columns%5B73%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B73%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B74%5D%5Bdata%5D=Fm.forward_pe_ratio&columns%5B74%5D%5Bname%5D=c110&columns%5B74%5D%5Bsearchable%5D=true&columns%5B74%5D%5Borderable%5D=true&columns%5B74%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B74%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B75%5D%5Bdata%5D=Fm.peg_pay_back&columns%5B75%5D%5Bname%5D=c111&columns%5B75%5D%5Bsearchable%5D=true&columns%5B75%5D%5Borderable%5D=true&columns%5B75%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B75%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B76%5D%5Bdata%5D=Fm.price_to_cfo&columns%5B76%5D%5Bname%5D=c112&columns%5B76%5D%5Bsearchable%5D=true&columns%5B76%5D%5Borderable%5D=true&columns%5B76%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B76%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B77%5D%5Bdata%5D=Fm.ev_to_ebitda&columns%5B77%5D%5Bname%5D=c113&columns%5B77%5D%5Bsearchable%5D=true&columns%5B77%5D%5Borderable%5D=true&columns%5B77%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B77%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B78%5D%5Bdata%5D=Fm.gross_margin&columns%5B78%5D%5Bname%5D=c121&columns%5B78%5D%5Bsearchable%5D=true&columns%5B78%5D%5Borderable%5D=true&columns%5B78%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B78%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B79%5D%5Bdata%5D=Fm.ebitda_margin&columns%5B79%5D%5Bname%5D=c122&columns%5B79%5D%5Bsearchable%5D=true&columns%5B79%5D%5Borderable%5D=true&columns%5B79%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B79%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B80%5D%5Bdata%5D=Fm.ebit_margin&columns%5B80%5D%5Bname%5D=c123&columns%5B80%5D%5Bsearchable%5D=true&columns%5B80%5D%5Borderable%5D=true&columns%5B80%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B80%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B81%5D%5Bdata%5D=Fm.net_profit_marg&columns%5B81%5D%5Bname%5D=c124&columns%5B81%5D%5Bsearchable%5D=true&columns%5B81%5D%5Borderable%5D=true&columns%5B81%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B81%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B82%5D%5Bdata%5D=Fm.norm_net_profit_marg&columns%5B82%5D%5Bname%5D=c125&columns%5B82%5D%5Bsearchable%5D=true&columns%5B82%5D%5Borderable%5D=true&columns%5B82%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B82%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B83%5D%5Bdata%5D=Fm.tax_rate&columns%5B83%5D%5Bname%5D=c126&columns%5B83%5D%5Bsearchable%5D=true&columns%5B83%5D%5Borderable%5D=true&columns%5B83%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B83%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B84%5D%5Bdata%5D=Fm.return_on_equity&columns%5B84%5D%5Bname%5D=c127&columns%5B84%5D%5Bsearchable%5D=true&columns%5B84%5D%5Borderable%5D=true&columns%5B84%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B84%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B85%5D%5Bdata%5D=Fm.sales_per_employee&columns%5B85%5D%5Bname%5D=c128&columns%5B85%5D%5Bsearchable%5D=true&columns%5B85%5D%5Borderable%5D=true&columns%5B85%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B85%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B86%5D%5Bdata%5D=Fm.pretax_marg&columns%5B86%5D%5Bname%5D=c129&columns%5B86%5D%5Bsearchable%5D=true&columns%5B86%5D%5Borderable%5D=true&columns%5B86%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B86%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B87%5D%5Bdata%5D=Fm.roic&columns%5B87%5D%5Bname%5D=c130&columns%5B87%5D%5Bsearchable%5D=true&columns%5B87%5D%5Borderable%5D=true&columns%5B87%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B87%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B88%5D%5Bdata%5D=Fm.ops_marg&columns%5B88%5D%5Bname%5D=c131&columns%5B88%5D%5Bsearchable%5D=true&columns%5B88%5D%5Borderable%5D=true&columns%5B88%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B88%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B89%5D%5Bdata%5D=Fm.roa&columns%5B89%5D%5Bname%5D=c132&columns%5B89%5D%5Bsearchable%5D=true&columns%5B89%5D%5Borderable%5D=true&columns%5B89%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B89%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B90%5D%5Bdata%5D=Fm.cash_return&columns%5B90%5D%5Bname%5D=c133&columns%5B90%5D%5Bsearchable%5D=true&columns%5B90%5D%5Borderable%5D=true&columns%5B90%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B90%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B91%5D%5Bdata%5D=Fm.debt_to_equity&columns%5B91%5D%5Bname%5D=c141&columns%5B91%5D%5Bsearchable%5D=true&columns%5B91%5D%5Borderable%5D=true&columns%5B91%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B91%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B92%5D%5Bdata%5D=Fm.debt_to_assets&columns%5B92%5D%5Bname%5D=c142&columns%5B92%5D%5Bsearchable%5D=true&columns%5B92%5D%5Borderable%5D=true&columns%5B92%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B92%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B93%5D%5Bdata%5D=Fm.debt_to_ebitda&columns%5B93%5D%5Bname%5D=c143&columns%5B93%5D%5Bsearchable%5D=true&columns%5B93%5D%5Borderable%5D=true&columns%5B93%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B93%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B94%5D%5Bdata%5D=Fm.debt_to_cash&columns%5B94%5D%5Bname%5D=c144&columns%5B94%5D%5Bsearchable%5D=true&columns%5B94%5D%5Borderable%5D=true&columns%5B94%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B94%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B95%5D%5Bdata%5D=Fm.interest_coverage&columns%5B95%5D%5Bname%5D=c145&columns%5B95%5D%5Bsearchable%5D=true&columns%5B95%5D%5Borderable%5D=true&columns%5B95%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B95%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B96%5D%5Bdata%5D=Fm.quick_ratio&columns%5B96%5D%5Bname%5D=c146&columns%5B96%5D%5Bsearchable%5D=true&columns%5B96%5D%5Borderable%5D=true&columns%5B96%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B96%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B97%5D%5Bdata%5D=Fm.current_ratio&columns%5B97%5D%5Bname%5D=c147&columns%5B97%5D%5Bsearchable%5D=true&columns%5B97%5D%5Borderable%5D=true&columns%5B97%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B97%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B98%5D%5Bdata%5D=Fm.debt_to_capital&columns%5B98%5D%5Bname%5D=c148&columns%5B98%5D%5Bsearchable%5D=true&columns%5B98%5D%5Borderable%5D=true&columns%5B98%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B98%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B99%5D%5Bdata%5D=Fm.financial_leverage&columns%5B99%5D%5Bname%5D=c149&columns%5B99%5D%5Bsearchable%5D=true&columns%5B99%5D%5Borderable%5D=true&columns%5B99%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B99%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B100%5D%5Bdata%5D=Fm.diluted_eps_growth&columns%5B100%5D%5Bname%5D=c161&columns%5B100%5D%5Bsearchable%5D=true&columns%5B100%5D%5Borderable%5D=true&columns%5B100%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B100%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B101%5D%5Bdata%5D=Fm.sust_growth_rate&columns%5B101%5D%5Bname%5D=c162&columns%5B101%5D%5Bsearchable%5D=true&columns%5B101%5D%5Borderable%5D=true&columns%5B101%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B101%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B102%5D%5Bdata%5D=Fm.revenue_growth&columns%5B102%5D%5Bname%5D=c163&columns%5B102%5D%5Bsearchable%5D=true&columns%5B102%5D%5Borderable%5D=true&columns%5B102%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B102%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B103%5D%5Bdata%5D=Fm.ops_income_growth&columns%5B103%5D%5Bname%5D=c164&columns%5B103%5D%5Bsearchable%5D=true&columns%5B103%5D%5Borderable%5D=true&columns%5B103%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B103%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B104%5D%5Bdata%5D=Fm.gross_profit_ann_5y_growth&columns%5B104%5D%5Bname%5D=c165&columns%5B104%5D%5Bsearchable%5D=true&columns%5B104%5D%5Borderable%5D=true&columns%5B104%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B104%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B105%5D%5Bdata%5D=Fm.cap_expend_ann_5y_growth&columns%5B105%5D%5Bname%5D=c166&columns%5B105%5D%5Bsearchable%5D=true&columns%5B105%5D%5Borderable%5D=true&columns%5B105%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B105%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B106%5D%5Bdata%5D=Fm.net_income_growth&columns%5B106%5D%5Bname%5D=c167&columns%5B106%5D%5Bsearchable%5D=true&columns%5B106%5D%5Borderable%5D=true&columns%5B106%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B106%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B107%5D%5Bdata%5D=DailyPerf15.PctPos&columns%5B107%5D%5Bname%5D=c170&columns%5B107%5D%5Bsearchable%5D=true&columns%5B107%5D%5Borderable%5D=true&columns%5B107%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B107%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B108%5D%5Bdata%5D=DailyPerf15.AvgRet&columns%5B108%5D%5Bname%5D=c171&columns%5B108%5D%5Bsearchable%5D=true&columns%5B108%5D%5Borderable%5D=true&columns%5B108%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B108%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B109%5D%5Bdata%5D=DailyPerf15.StdDev&columns%5B109%5D%5Bname%5D=c172&columns%5B109%5D%5Bsearchable%5D=true&columns%5B109%5D%5Borderable%5D=true&columns%5B109%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B109%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B110%5D%5Bdata%5D=DailyPerf15.Sharpe&columns%5B110%5D%5Bname%5D=c173&columns%5B110%5D%5Bsearchable%5D=true&columns%5B110%5D%5Borderable%5D=true&columns%5B110%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B110%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B111%5D%5Bdata%5D=DailyPerf30.PctPos&columns%5B111%5D%5Bname%5D=c174&columns%5B111%5D%5Bsearchable%5D=true&columns%5B111%5D%5Borderable%5D=true&columns%5B111%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B111%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B112%5D%5Bdata%5D=DailyPerf30.AvgRet&columns%5B112%5D%5Bname%5D=c175&columns%5B112%5D%5Bsearchable%5D=true&columns%5B112%5D%5Borderable%5D=true&columns%5B112%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B112%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B113%5D%5Bdata%5D=DailyPerf30.StdDev&columns%5B113%5D%5Bname%5D=c176&columns%5B113%5D%5Bsearchable%5D=true&columns%5B113%5D%5Borderable%5D=true&columns%5B113%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B113%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B114%5D%5Bdata%5D=DailyPerf30.Sharpe&columns%5B114%5D%5Bname%5D=c177&columns%5B114%5D%5Bsearchable%5D=true&columns%5B114%5D%5Borderable%5D=true&columns%5B114%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B114%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B115%5D%5Bdata%5D=DailyPerf90.PctPos&columns%5B115%5D%5Bname%5D=c178&columns%5B115%5D%5Bsearchable%5D=true&columns%5B115%5D%5Borderable%5D=true&columns%5B115%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B115%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B116%5D%5Bdata%5D=DailyPerf90.AvgRet&columns%5B116%5D%5Bname%5D=c179&columns%5B116%5D%5Bsearchable%5D=true&columns%5B116%5D%5Borderable%5D=true&columns%5B116%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B116%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B117%5D%5Bdata%5D=DailyPerf90.StdDev&columns%5B117%5D%5Bname%5D=c180&columns%5B117%5D%5Bsearchable%5D=true&columns%5B117%5D%5Borderable%5D=true&columns%5B117%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B117%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B118%5D%5Bdata%5D=DailyPerf90.Sharpe&columns%5B118%5D%5Bname%5D=c181&columns%5B118%5D%5Bsearchable%5D=true&columns%5B118%5D%5Borderable%5D=true&columns%5B118%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B118%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B119%5D%5Bdata%5D=InWatchlist&columns%5B119%5D%5Bname%5D=InWatchlist&columns%5B119%5D%5Bsearchable%5D=true&columns%5B119%5D%5Borderable%5D=true&columns%5B119%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B119%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=BD.EtfHoldingsList&columns%5B5%5D%5Bname%5D=c29&columns%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=true&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B121%5D%5Bdata%5D=BD.EarningsType&columns%5B121%5D%5Bname%5D=c30&columns%5B121%5D%5Bsearchable%5D=true&columns%5B121%5D%5Borderable%5D=true&columns%5B121%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B121%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B122%5D%5Bdata%5D=BD.HasOptions&columns%5B122%5D%5Bname%5D=c31&columns%5B122%5D%5Bsearchable%5D=true&columns%5B122%5D%5Borderable%5D=true&columns%5B122%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B122%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B123%5D%5Bdata%5D=BD.StockIdeas&columns%5B123%5D%5Bname%5D=StockIdeas&columns%5B123%5D%5Bsearchable%5D=true&columns%5B123%5D%5Borderable%5D=true&columns%5B123%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B123%5D%5Bsearch%5D%5Bregex%5D=false&order%5B0%5D%5Bcolumn%5D=3&order%5B0%5D%5Bdir%5D=desc&start=0&length='+str(length)+'&search%5Bvalue%5D=&search%5Bregex%5D=false'
            print(self.headers)
            response = self.session.post(
                self.base_url,
                headers=self.headers,
                data=encoded_payload,
                timeout=30
            )

            if response.status_code == 200:
                return response.json()
            else:
                print(f"API Error: Status Code {response.status_code}")
                return None

        except Exception as e:
            print(f"Error fetching data: {str(e)}")
            return None

    def process_stock_data(self, raw_data):
        """Process and enrich the raw stock data"""
        if not raw_data or 'data' not in raw_data:
            return pd.DataFrame()
        
        processed_stocks = []
        
        for stock in raw_data['data']:
            try:
                stock_data = {
                    # Basic Info
                    'symbol': stock.get('Symbol', ''),
                    'company_name': stock.get('BD', {}).get('Name', ''),
                    'sector': stock.get('BD', {}).get('Sector', ''),
                    'industry': stock.get('BD', {}).get('Industry', ''),
                    'country': stock.get('BD', {}).get('Country', ''),
                    'equity_type': stock.get('BD', {}).get('EquityType', ''),
                    
                    # Price Data
                    'current_price': stock.get('BD', {}).get('Px', 0),
                    'price_change': stock.get('BD', {}).get('PxChg', 0),
                    'price_change_pct': stock.get('BD', {}).get('PxChgPct', 0),
                    'open_change_pct': stock.get('BD', {}).get('ChgOpenPx', 0),
                    'close_to_open_change': stock.get('BD', {}).get('ChgCloseToOpen', 0),
                    
                    # Volume Analysis
                    'volume': stock.get('BD', {}).get('Volume', 0),
                    'avg_volume': stock.get('BD', {}).get('AvgVolume', 0),
                    'relative_volume': stock.get('BD', {}).get('RelVolume', 0),
                    
                    # Market Data
                    'market_cap': stock.get('BD', {}).get('MarketCap', 0),
                    'market_cap_str': stock.get('BD', {}).get('MarketCapStr', ''),
                    'dividend_yield': stock.get('BD', {}).get('DivYield', 0),
                    
                    # Performance Metrics
                    'change_2d': stock.get('BD', {}).get('Chg2D', 0),
                    'change_3d': stock.get('BD', {}).get('Chg3D', 0),
                    'change_1w': stock.get('BD', {}).get('Chg7D', 0),
                    'change_2w': stock.get('BD', {}).get('Chg2Wk', 0),
                    'change_1m': stock.get('BD', {}).get('Chg3M', 0),
                    'change_3m': stock.get('BD', {}).get('Chg3M', 0),
                    'change_6m': stock.get('BD', {}).get('Chg6M', 0),
                    'change_1y': stock.get('BD', {}).get('Chg1Y', 0),
                    'change_ytd': stock.get('BD', {}).get('ChgYTD', 0),
                    'change_3y': stock.get('BD', {}).get('Chg3Y', 0),
                    'change_5y': stock.get('BD', {}).get('Chg5Y', 0),
                    
                    # Technical Indicators
                    'rsi_14': stock.get('BD', {}).get('RSI14', 0),
                    'ma_20d': stock.get('BD', {}).get('MA_20D', 0),
                    'ma_50d': stock.get('BD', {}).get('MA_50D', 0),
                    'ma_250d': stock.get('BD', {}).get('MA_250D', 0),
                    'change_vs_ma20': stock.get('BD', {}).get('ChgMA20', 0),
                    'change_vs_ma50': stock.get('BD', {}).get('ChgMA50', 0),
                    'change_vs_ma250': stock.get('BD', {}).get('ChgMA250', 0),
                    'ma_20v50': stock.get('BD', {}).get('MA_20v50', 0),
                    'ma_20v250': stock.get('BD', {}).get('MA_20v250', 0),
                    'ma_50v250': stock.get('BD', {}).get('MA_50v250', 0),
                    'ma_pattern': stock.get('BD', {}).get('MA_Name', ''),
                    
                    # 52-Week Range
                    'change_vs_52w_high': stock.get('BD', {}).get('Chg52WHi', 0),
                    'change_vs_52w_low': stock.get('BD', {}).get('Chg52WLo', 0),
                    
                    # Volatility
                    'iv_30d': stock.get('BD', {}).get('IV30', 0),
                    'iv_change': stock.get('BD', {}).get('IV30Chg', 0),
                    'iv_rank': stock.get('BD', {}).get('IVRank', 0),
                    'vol_1day': stock.get('BD', {}).get('Vol1Day', 0),
                    'vol_20day': stock.get('BD', {}).get('Vol20Day', 0),
                    'vol_1year': stock.get('BD', {}).get('Vol1Year', 0),
                    
                    # Options Data
                    'options_volume': stock.get('BD', {}).get('OptVolume', 0),
                    'relative_options_volume': stock.get('BD', {}).get('RelOptVolume', 0),
                    'has_options': stock.get('BD', {}).get('HasOptions', False),
                    
                    # VWAP Data
                    'vwap': stock.get('BD', {}).get('DayVWAP', 0),
                    'vwap_minus_sd': stock.get('BD', {}).get('DayVWMinSD', 0),
                    'vwap_plus_sd': stock.get('BD', {}).get('DayVWPlusSD', 0),
                    'price_vs_vwap_std': stock.get('BD', {}).get('PvVWStd', 0),
                    'price_vs_vwap_pct': stock.get('BD', {}).get('PvVWPct', 0),
                    
                    # Financial Ratios
                    'pe_ratio': stock.get('Fm', {}).get('pe_ratio', 0),
                    'pe_normalized': stock.get('Fm', {}).get('pe_normalized_eps', 0),
                    'forward_pe': stock.get('Fm', {}).get('forward_pe_ratio', 0),
                    'price_to_sales': stock.get('Fm', {}).get('px_to_sales', 0),
                    'price_to_book': stock.get('Fm', {}).get('px_to_bookval', 0),
                    'price_to_cash': stock.get('Fm', {}).get('px_to_cash', 0),
                    'price_to_fcf': stock.get('Fm', {}).get('px_to_freecashflow', 0),
                    'ev_to_ebitda': stock.get('Fm', {}).get('ev_to_ebitda', 0),
                    'peg_ratio': stock.get('Fm', {}).get('peg_ratio', 0),
                    
                    # Profitability Margins
                    'gross_margin': stock.get('Fm', {}).get('gross_margin', 0),
                    'ebitda_margin': stock.get('Fm', {}).get('ebitda_margin', 0),
                    'net_margin': stock.get('Fm', {}).get('net_profit_marg', 0),
                    'operating_margin': stock.get('Fm', {}).get('ops_marg', 0),
                    
                    # Returns
                    'roe': stock.get('Fm', {}).get('return_on_equity', 0),
                    'roa': stock.get('Fm', {}).get('roa', 0),
                    'roic': stock.get('Fm', {}).get('roic', 0),
                    
                    # Debt Ratios
                    'debt_to_equity': stock.get('Fm', {}).get('debt_to_equity', 0),
                    'debt_to_assets': stock.get('Fm', {}).get('debt_to_assets', 0),
                    'current_ratio': stock.get('Fm', {}).get('current_ratio', 0),
                    'quick_ratio': stock.get('Fm', {}).get('quick_ratio', 0),
                    
                    # Growth Rates
                    'revenue_growth': stock.get('Fm', {}).get('revenue_growth', 0),
                    'eps_growth': stock.get('Fm', {}).get('diluted_eps_growth', 0),
                    'net_income_growth': stock.get('Fm', {}).get('net_income_growth', 0),
                    
                    # Dividend Data
                    'dividend_growth_1y': stock.get('BD', {}).get('DivGrowth1Yr', 0),
                    'dividend_growth_3y': stock.get('BD', {}).get('DivGrowth3Yr', 0),
                    'payout_ratio': stock.get('BD', {}).get('PayoutRatio', 0),
                    'div_increases_3y': stock.get('BD', {}).get('DivIncreases3Yr', 0),
                    'div_decreases_3y': stock.get('BD', {}).get('DivDecreases3Yr', 0),
                    
                    # Shares Data
                    'shares_outstanding': stock.get('BD', {}).get('ShrOut', 0),
                    'shares_float': stock.get('BD', {}).get('ShrFloat', 0),
                    
                    # Timestamp
                    'timestamp': datetime.now().isoformat(),
                }
                
                # Calculate additional metrics
                stock_data.update(self.calculate_additional_metrics(stock_data))
                processed_stocks.append(stock_data)
                
            except Exception as e:
                print(f"Error processing stock {stock.get('Symbol', 'Unknown')}: {str(e)}")
                continue
        
        return pd.DataFrame(processed_stocks)
    
    def calculate_additional_metrics(self, stock_data):
        """Calculate additional trading metrics"""
        additional_metrics = {}
        
        # Momentum Score (0-100)
        momentum_factors = [
            stock_data.get('price_change_pct', 0),
            stock_data.get('change_1w', 0),
            stock_data.get('change_1m', 0),
            stock_data.get('relative_volume', 0) - 1  # Above 1 is positive momentum
        ]
        momentum_score = sum([f for f in momentum_factors if f is not None]) * 10
        additional_metrics['momentum_score'] = max(0, min(100, momentum_score + 50))
        
        # Volatility Score
        vol_1day = stock_data.get('vol_1day', 0) or 0
        additional_metrics['volatility_score'] = min(100, vol_1day)
        
        # Volume Score (relative to average)
        rel_vol = stock_data.get('relative_volume', 0) or 0
        additional_metrics['volume_score'] = min(100, rel_vol * 20)
        
        # Technical Score (based on MA relationships and RSI)
        rsi = stock_data.get('rsi_14', 50) or 50
        ma_score = 0
        if stock_data.get('ma_20v50', 0) > 0:
            ma_score += 25
        if stock_data.get('ma_50v250', 0) > 0:
            ma_score += 25
        if stock_data.get('change_vs_ma20', 0) > 0:
            ma_score += 25
        if 30 < rsi < 70:
            ma_score += 25
        additional_metrics['technical_score'] = ma_score
        
        # Overall Score
        scores = [
            additional_metrics.get('momentum_score', 0),
            additional_metrics.get('technical_score', 0),
            additional_metrics.get('volume_score', 0)
        ]
        additional_metrics['overall_score'] = sum(scores) / len(scores)
        
        return additional_metrics
    
    def check_alerts(self, df):
        """Check for trading alerts based on thresholds"""
        alerts = []
        current_time = datetime.now()
        
        for idx, row in df.iterrows():
            symbol = row.get('symbol', '')
            
            # Price movement alerts
            price_change = abs(row.get('price_change_pct', 0) or 0)
            if price_change >= self.alert_config['price_change_threshold']:
                direction = "UP" if (row.get('price_change_pct', 0) or 0) > 0 else "DOWN"
                alerts.append({
                    'timestamp': current_time,
                    'symbol': symbol,
                    'alert_type': 'PRICE_MOVE',
                    'message': f"{symbol} moved {direction} {price_change:.2%}",
                    'value': price_change,
                    'current_price': row.get('current_price', 0)
                })
            
            # Volume spike alerts
            rel_volume = row.get('relative_volume', 0) or 0
            if rel_volume >= self.alert_config['volume_spike_threshold']:
                alerts.append({
                    'timestamp': current_time,
                    'symbol': symbol,
                    'alert_type': 'VOLUME_SPIKE',
                    'message': f"{symbol} volume spike: {rel_volume:.1f}x average",
                    'value': rel_volume,
                    'current_price': row.get('current_price', 0)
                })
            
            # RSI alerts
            rsi = row.get('rsi_14', 0) or 0
            if rsi >= self.alert_config['rsi_overbought']:
                alerts.append({
                    'timestamp': current_time,
                    'symbol': symbol,
                    'alert_type': 'RSI_OVERBOUGHT',
                    'message': f"{symbol} RSI overbought: {rsi:.1f}",
                    'value': rsi,
                    'current_price': row.get('current_price', 0)
                })
            elif rsi <= self.alert_config['rsi_oversold'] and rsi > 0:
                alerts.append({
                    'timestamp': current_time,
                    'symbol': symbol,
                    'alert_type': 'RSI_OVERSOLD',
                    'message': f"{symbol} RSI oversold: {rsi:.1f}",
                    'value': rsi,
                    'current_price': row.get('current_price', 0)
                })
            
            # High volatility alert
            vol_score = row.get('volatility_score', 0) or 0
            if vol_score >= self.alert_config['high_volatility_threshold']:
                alerts.append({
                    'timestamp': current_time,
                    'symbol': symbol,
                    'alert_type': 'HIGH_VOLATILITY',
                    'message': f"{symbol} high volatility detected: {vol_score:.1f}",
                    'value': vol_score,
                    'current_price': row.get('current_price', 0)
                })
        
        return alerts
    
    def export_to_csv(self, df, filename=None):
        """Export data to CSV file"""
        if filename is None:
            filename = f"trading_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        try:
            df.to_csv(filename, index=False)
            print(f"Data exported to {filename} - {len(df)} records")
            return filename
        except Exception as e:
            print(f"Error exporting to CSV: {str(e)}")
            return None
    
    def export_alerts_to_csv(self, alerts, filename=None):
        """Export alerts to CSV file"""
        if not alerts:
            return None
            
        if filename is None:
            filename = f"trading_alerts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        try:
            alerts_df = pd.DataFrame(alerts)
            alerts_df.to_csv(filename, index=False)
            print(f"Alerts exported to {filename} - {len(alerts)} alerts")
            return filename
        except Exception as e:
            print(f"Error exporting alerts: {str(e)}")
            return None
    
    def generate_summary_report(self, df):
        """Generate a summary report of market conditions"""
        if df.empty:
            return "No data available for summary"
        
        summary = {
            'total_stocks': len(df),
            'timestamp': datetime.now().isoformat(),
            'market_stats': {
                'avg_price_change': df['price_change_pct'].mean(),
                'positive_movers': len(df[df['price_change_pct'] > 0]),
                'negative_movers': len(df[df['price_change_pct'] < 0]),
                'high_volume_stocks': len(df[df['relative_volume'] > 2]),
                'overbought_stocks': len(df[df['rsi_14'] > 70]),
                'oversold_stocks': len(df[df['rsi_14'] < 30])
            },
            'top_performers': {
                'biggest_gainers': df.nlargest(10, 'price_change_pct')[['symbol', 'price_change_pct', 'current_price']].to_dict('records'),
                'biggest_losers': df.nsmallest(10, 'price_change_pct')[['symbol', 'price_change_pct', 'current_price']].to_dict('records'),
                'highest_volume': df.nlargest(10, 'relative_volume')[['symbol', 'relative_volume', 'current_price']].to_dict('records'),
                'highest_momentum': df.nlargest(10, 'momentum_score')[['symbol', 'momentum_score', 'current_price']].to_dict('records')
            }
        }
        
        return summary
    
    def run_analysis(self, length=5, export_csv=True):
        """Run the complete analysis cycle"""
        print(f"\n{'='*60}")
        print(f"Running Trading Analysis - {datetime.now()}")
        print(f"{'='*60}")
        
        # Fetch data
        print("Fetching market data...")
        raw_data = self.fetch_market_data(length=length)
        
        if not raw_data:
            print("Failed to fetch market data")
            return None
        
        # Process data
        print("Processing stock data...")
        df = self.process_stock_data(raw_data)
        
        if df.empty:
            print("No data to process")
            return None
        
        print(f"Processed {len(df)} stocks")
        
        # Check alerts
        print("Checking for alerts...")
        alerts = self.check_alerts(df)
        
        if alerts:
            print(f"\n🚨 ALERTS ({len(alerts)}):")
            for alert in alerts[-10:]:  # Show last 10 alerts
                print(f"  {alert['alert_type']}: {alert['message']}")
        
        # Generate summary
        summary = self.generate_summary_report(df)
        print(f"\n📊 MARKET SUMMARY:")
        print(f"  Total Stocks: {summary['total_stocks']}")
        print(f"  Avg Price Change: {summary['market_stats']['avg_price_change']:.2%}")
        print(f"  Positive Movers: {summary['market_stats']['positive_movers']}")
        print(f"  High Volume: {summary['market_stats']['high_volume_stocks']}")
        
        # Export data
        if export_csv:
            csv_file = self.export_to_csv(df)
            if alerts:
                self.export_alerts_to_csv(alerts)
        
        # Store historical data
        self.historical_data.append({
            'timestamp': datetime.now(),
            'data': df,
            'alerts': alerts,
            'summary': summary
        })
        
        # Keep only last 100 snapshots to manage memory
        if len(self.historical_data) > 100:
            self.historical_data = self.historical_data[-100:]
        
        return {
            'data': df,
            'alerts': alerts,
            'summary': summary,
            'csv_file': csv_file if export_csv else None
        }
    
    def run_continuous_monitoring(self, interval_seconds=3, max_iterations=None):
        """Run continuous monitoring with specified interval"""
        print(f"Starting continuous monitoring (every {interval_seconds} seconds)")
        print("Press Ctrl+C to stop")
        
        iteration = 0
        try:
            while True:
                if max_iterations and iteration >= max_iterations:
                    break
                
                self.run_analysis()
                iteration += 1
                
                print(f"\nWaiting {interval_seconds} seconds until next update...")
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print("\n\nStopping continuous monitoring...")
        except Exception as e:
            print(f"Error in continuous monitoring: {str(e)}")

def main():
    """Main function to run the trading analyzer"""
    
    print("🔥 Advanced Trading Analyzer")
    print("=" * 50)
    
    # Initialize analyzer
    analyzer = TradingAnalyzer()
    
    # Configuration options
    print("\nConfiguration Options:")
    print("1. Single analysis run")
    print("2. Continuous monitoring (every 3 seconds) - NOT RECOMMENDED")
    print("3. Continuous monitoring (every 30 seconds)")
    print("4. Continuous monitoring (every 60 seconds)")
    
    try:
        choice = input("\nSelect option (1-4): ").strip()
        
        if choice == "1":
            # Single run
            result = analyzer.run_analysis(length=5)
            if result:
                print("\n✅ Analysis complete!")
                
        elif choice == "2":
            print("\n⚠️  WARNING: 3-second intervals may exceed API rate limits!")
            confirm = input("Continue anyway? (y/n): ").strip().lower()
            if confirm == 'y':
                analyzer.run_continuous_monitoring(interval_seconds=3)
                
        elif choice == "3":
            analyzer.run_continuous_monitoring(interval_seconds=30)
            
        elif choice == "4":
            analyzer.run_continuous_monitoring(interval_seconds=60)
            
        else:
            print("Invalid option selected")
            
    except KeyboardInterrupt:
        print("\n\nProgram interrupted by user")
    except Exception as e:
        print(f"\nError: {str(e)}")

if __name__ == "__main__":
    main()