import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
import pytz
import os
import subprocess

def process_databento_1m_data():
    """
    Process the Databento 1-minute data and set up the complete backtesting pipeline.
    """
    print("🚀 Starting Databento 1-minute backtest pipeline...")
    
    # Step 1: Load and preprocess the 1m CSV
    print("\n📊 Step 1: Loading Databento 1-minute data...")
    
    raw_csv = "data/processed/glbx-mdp3-20250430-20250729.ohlcv-1m.csv.zst"
    
    if not os.path.exists(raw_csv):
        print(f"❌ Error: Data file not found at {raw_csv}")
        return
    
    # Load the compressed data
    df = pd.read_csv(raw_csv, compression='zstd')
    print(f"✅ Loaded {len(df)} rows of 1-minute data")
    print(f"📋 Columns: {df.columns.tolist()}")
    
    # Convert timestamps and set index
    print("\n⏰ Converting timestamps...")
    df['ts_event'] = pd.to_datetime(df['ts_event'])
    df = df.set_index('ts_event')
    
    # Filter for MYM futures (MYMM5 is the May 2025 contract)
    print("\n🔍 Filtering for MYM futures...")
    df = df[df['symbol'] == 'MYMM5'].copy()
    print(f"✅ Filtered to {len(df)} rows for MYMM5")
    
    # Convert to NY timezone
    print("\n🌍 Converting to NY timezone...")
    if df.index.tz is None:
        df.index = df.index.tz_localize('UTC')
    df.index = df.index.tz_convert('America/New_York')
    
    # Filter for NY session times (9:25-10:30 ET)
    print("\n⏰ Filtering for NY session times (9:25-10:30 ET)...")
    session_mask = (df.index.time >= time(9, 25)) & (df.index.time <= time(10, 30))
    df = df[session_mask]
    print(f"✅ Filtered to {len(df)} rows in session times")
    
    # Check data quality
    print(f"\n📈 Data summary:")
    print(f"   Date range: {df.index.min()} to {df.index.max()}")
    print(f"   Total rows: {len(df)}")
    print(f"   Price range: ${df['low'].min():.2f} - ${df['high'].max():.2f}")
    
    # Save cleaned CSV
    output_path = "data/raw/MYM_FUT_1m_full.csv"
    print(f"\n💾 Saving cleaned file to: {output_path}")
    df.to_csv(output_path)
    print(f"✅ Saved cleaned file to: {output_path}")
    
    # Step 2: Confirm existence of backtest script
    print("\n🔍 Step 2: Checking backtest script...")
    backtest_script = "backtest/ny_open_breakout.py"
    if not os.path.exists(backtest_script):
        print(f"❌ Error: Backtest script not found at {backtest_script}")
        return
    print(f"✅ Backtest script found at: {backtest_script}")
    
    # Step 3: Run the backtest pipeline
    print("\n🚀 Step 3: Running backtest pipeline...")
    try:
        result = subprocess.run(["python", backtest_script], 
                              capture_output=True, text=True, cwd="backtest")
        
        if result.returncode == 0:
            print("✅ Backtest completed successfully!")
            print("\n📊 Backtest Output:")
            print(result.stdout)
        else:
            print("❌ Backtest failed!")
            print("Error output:")
            print(result.stderr)
            
    except Exception as e:
        print(f"❌ Error running backtest: {e}")
    
    # Step 4: Check results
    print("\n📋 Step 4: Checking results...")
    results_file = "results/detailed_trades_log.csv"
    if os.path.exists(results_file):
        trades_df = pd.read_csv(results_file)
        print(f"✅ Trade results saved to: {results_file}")
        print(f"📊 Generated {len(trades_df)} trades")
        if len(trades_df) > 0:
            print(f"   Win rate: {(trades_df['outcome'] == 'Win').mean():.1%}")
            print(f"   Total R-multiple: {trades_df['R_multiple'].sum():.2f}")
    else:
        print(f"⚠️  No trade results found at {results_file}")
    
    print("\n🎉 Databento 1-minute backtest pipeline complete!")

if __name__ == '__main__':
    process_databento_1m_data() 