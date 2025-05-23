import requests
import pandas as pd
import time
from datetime import datetime, timedelta

def fetch_binance_klines(symbol, interval, start_str, end_str):
    url = "https://api.binance.com/api/v3/klines"
    
    start_ts = int(pd.to_datetime(start_str).timestamp() * 1000)
    end_ts = int(pd.to_datetime(end_str).timestamp() * 1000)

    all_klines = []

    while start_ts < end_ts:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_ts,
            "endTime": end_ts,
            "limit": 1000  # max limit per call
        }
        response = requests.get(url, params=params)
        data = response.json()

        if not data:
            break
        
        all_klines.extend(data)
        last_open_time = data[-1][0]
        start_ts = last_open_time + 1  # move to next millisecond

        time.sleep(0.3)  # to avoid hitting rate limits

    df = pd.DataFrame(all_klines, columns=[
        "Open Time", "Open", "High", "Low", "Close", "Volume",
        "Close Time", "Quote Asset Volume", "Number of Trades",
        "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume", "Ignore"
    ])

    df["Open Time"] = pd.to_datetime(df["Open Time"], unit="ms")
    df["Close Time"] = pd.to_datetime(df["Close Time"], unit="ms")
    
    numeric_cols = ["Open", "High", "Low", "Close", "Volume",
                    "Quote Asset Volume", "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume"]
    df[numeric_cols] = df[numeric_cols].astype(float)
    df["Number of Trades"] = df["Number of Trades"].astype(int)
    
    df.set_index("Open Time", inplace=True)
    df.drop(columns=["Close Time", "Ignore"], inplace=True)
    
    return df

# Calculate start date 5 years ago from today
end_date = datetime.now().date()
start_date = end_date - timedelta(days=5*365)

start_str = start_date.strftime("%Y-%m-%d")
end_str = end_date.strftime("%Y-%m-%d")

print(f"Fetching data from {start_str} to {end_str}...")

eth_data_5years = fetch_binance_klines("ETHUSDT", "1d", start_str, end_str)

eth_data_5years.to_csv("eth_usdt_5years.csv")
print(f"Data fetched: {len(eth_data_5years)} rows")
print(eth_data_5years.head())
