import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
import pytz
import os

def process_existing_databento_data():
    """
    Process the existing Databento data and generate files for backtesting.
    """
    print("🚀 Processing existing Databento data for backtesting...")
    
    # File paths
    input_file = 'data/processed/glbx-mdp3-20250430-20250729.ohlcv-1m.csv.zst'
    
    if not os.path.exists(input_file):
        print(f"❌ Input file not found: {input_file}")
        return
    
    print(f"📊 Loading data from: {input_file}")
    
    # Load the compressed data
    df = pd.read_csv(input_file, compression='zstd')
    
    print(f"✅ Loaded {len(df)} rows of data")
    print(f"📋 Columns: {df.columns.tolist()}")
    
    # Convert timestamp to datetime
    df['ts_event'] = pd.to_datetime(df['ts_event'])
    
    # Set timestamp as index
    df.set_index('ts_event', inplace=True)
    
    # Convert to NY timezone
    if df.index.tz is None:
        df.index = df.index.tz_localize('UTC')
    df.index = df.index.tz_convert('America/New_York')
    
    print(f"📅 Date range: {df.index.min()} to {df.index.max()}")
    print(f"⏰ Time range: {df.index.time.min()} to {df.index.time.max()}")
    
    # Filter for MYMM5 symbol (the one we know works)
    if 'symbol' in df.columns:
        df = df[df['symbol'] == 'MYMM5']
        print(f"📊 Filtered to MYMM5: {len(df)} rows")
    
    # Filter for NY session times (9:25-10:30 ET)
    session_mask = (df.index.time >= time(9, 25)) & (df.index.time <= time(10, 30))
    df = df[session_mask]
    print(f"⏰ Filtered to session times: {len(df)} rows")
    
    if len(df) == 0:
        print("❌ No data found in session times. Using all available data...")
        # Use all data if no session data found
        df = pd.read_csv(input_file, compression='zstd')
        df['ts_event'] = pd.to_datetime(df['ts_event'])
        df.set_index('ts_event', inplace=True)
        if df.index.tz is None:
            df.index = df.index.tz_localize('UTC')
        df.index = df.index.tz_convert('America/New_York')
        if 'symbol' in df.columns:
            df = df[df['symbol'] == 'MYMM5']
    
    # Generate resampled data for different timeframes
    intervals = ['1m', '2m', '3m', '5m', '10m', '15m']
    
    for interval in intervals:
        print(f"\n📊 Resampling to {interval}...")
        
        if interval == '1m':
            resampled_df = df.copy()
        else:
            # Convert interval string to pandas offset
            interval_map = {
                '2m': '2T',
                '3m': '3T', 
                '5m': '5T',
                '10m': '10T',
                '15m': '15T'
            }
            pandas_interval = interval_map[interval]
            
            # Resample the data
            resampled_df = df.resample(pandas_interval).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min', 
                'close': 'last',
                'volume': 'sum'
            }).dropna()
        
        print(f"  📈 {interval} data: {len(resampled_df)} rows")
        
        # Save to CSV
        output_file = f"data/raw/MYM_FUT_{interval}_full.csv"
        resampled_df.to_csv(output_file)
        print(f"  💾 Saved to: {output_file}")
    
    print(f"\n🎉 Data processing complete!")
    print(f"📁 Check 'data/raw/' directory for processed files.")

def main():
    """
    Main function to process existing data.
    """
    try:
        process_existing_databento_data()
    except Exception as e:
        print(f"❌ Error processing data: {e}")

if __name__ == '__main__':
    main() 