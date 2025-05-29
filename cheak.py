import requests
import pandas as pd
from datetime import datetime, timedelta
import time
# Constants
ACCESS_TOKEN = 


CLIENT_ID = "1100153746"
BASE_URL = "https://api.dhan.co/v2/charts/intraday"

def fetch_market_quote(security_id: str, from_date: str, to_date: str):
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "access-token": ACCESS_TOKEN,
        "client-id": CLIENT_ID
    }

    payload = {
        "securityId": security_id,
        "exchangeSegment": "NSE_EQ",
        "instrument": "EQUITY",
        "interval": 5, # 5-minute interval
        "fromDate": from_date,
        "toDate": to_date
    }

    response = requests.post(BASE_URL, headers=headers, json=payload)
    data = response.json()
    df = pd.DataFrame(data)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
    df['timestamp'] = df['timestamp'].dt.tz_convert('Asia/Kolkata')
    return df

def fetch_multiple_intervals(security_id: str, days: int):
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=days)
    all_data = []

    while start_dt < end_dt:
        # time.sleep(1)  # Sleep to avoid hitting API limits
        next_interval = start_dt + timedelta(days=85)  
        to_date = min(next_interval, end_dt).strftime('%Y-%m-%d')
        from_date = start_dt.strftime('%Y-%m-%d')

        print(f"Fetching data from {from_date} to {to_date}...")
        df = fetch_market_quote(security_id, from_date, to_date)
        all_data.append(df)

        start_dt = next_interval

    # Combine all dataframes into one
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df.to_csv(f"historical_data_{security_id}_last_{days}_days.csv", index=False)
    return combined_df

def main():
    security_id = "15355"
    days = 500  # Fetch data for the last 200 days
    df = fetch_multiple_intervals(security_id, days)
    if df is not None:
        print(df.head())

if __name__ == "__main__":
    main()
    
