import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
import pytz
import os

def process_existing_data():
    """
    Process the existing compressed data file and generate resampled files for backtesting.
    """
    # File paths
    input_file = '../data/raw/glbx-mdp3-20250430-20250729.ohlcv-1s.csv.zst'
    output_dir = '../data/raw/'
    
    print(f"Loading data from: {input_file}")
    
    # Load the compressed data
    df = pd.read_csv(input_file, compression='zstd')
    
    print(f"Loaded {len(df)} rows of data")
    print(f"Columns: {df.columns.tolist()}")
    print(f"Date range: {df['ts_event'].min()} to {df['ts_event'].max()}")
    
    # Convert timestamp to datetime
    df['ts_event'] = pd.to_datetime(df['ts_event'])
    
    # Filter for MYM futures (MYMM5 is the May 2025 contract)
    df = df[df['symbol'] == 'MYMM5'].copy()
    print(f"Filtered to {len(df)} rows for MYMM5")
    
    # Set timestamp as index and convert to NY timezone
    df.set_index('ts_event', inplace=True)
    # Ensure the index is timezone-aware and convert to NY timezone
    if df.index.tz is None:
        df.index = df.index.tz_localize('UTC')
    df.index = df.index.tz_convert('America/New_York')
    
    # Keep a copy of the original data
    df_original = df.copy()
    
    # Filter for NY session times (9:25-10:30 ET)
    # First, let's see what times we actually have
    print(f"Time range in data: {df.index.min()} to {df.index.max()}")
    print(f"Sample times: {df.index[:10].tolist()}")
    
    # Filter for session times
    session_mask = (df.index.time >= time(9, 25)) & (df.index.time <= time(10, 30))
    df = df[session_mask]
    print(f"Filtered to session times: {len(df)} rows")
    
    if len(df) == 0:
        print("No data found in session times. Checking all available times...")
        print(f"Available times: {sorted(df.index.time.unique())[:20]}")
        # Use all data if no session data found
        df = df_original
    
    # Resample to different intervals
    intervals = ['1m', '2m', '3m', '5m', '10m', '15m']
    
    for interval in intervals:
        print(f"\nResampling to {interval}...")
        
        # Resample the data
        resampled = df.resample(interval).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        print(f"Generated {len(resampled)} {interval} bars")
        
        # Save to file
        output_file = f"{output_dir}MYM_FUT_{interval}_full.csv"
        resampled.to_csv(output_file)
        print(f"Saved to: {output_file}")
    
    print("\nData processing complete!")

if __name__ == '__main__':
    process_existing_data() 