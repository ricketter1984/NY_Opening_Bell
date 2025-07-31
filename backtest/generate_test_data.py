import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
import pytz
import os

def generate_test_data():
    """
    Generate synthetic test data for backtesting.
    """
    # Create synthetic data for testing
    ny_tz = pytz.timezone('America/New_York')
    
    # Generate dates for May-July 2025 (weekdays only)
    start_date = datetime(2025, 5, 1)
    end_date = datetime(2025, 7, 31)
    
    # Generate weekdays only
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    weekdays = [d for d in date_range if d.weekday() < 5]  # Monday = 0, Friday = 4
    
    print(f"Generating test data for {len(weekdays)} weekdays")
    
    intervals = ['1m', '2m', '3m', '5m', '10m', '15m']
    
    for interval in intervals:
        print(f"\nGenerating {interval} data...")
        
        all_data = []
        
        for date in weekdays:
            # Generate time range for 9:25-10:30 ET
            start_time = ny_tz.localize(datetime.combine(date.date(), time(9, 25)))
            end_time = ny_tz.localize(datetime.combine(date.date(), time(10, 30)))
            
            # Create time index for the interval
            if interval == '1m':
                freq = '1min'
            elif interval == '2m':
                freq = '2min'
            elif interval == '3m':
                freq = '3min'
            elif interval == '5m':
                freq = '5min'
            elif interval == '10m':
                freq = '10min'
            elif interval == '15m':
                freq = '15min'
            
            time_index = pd.date_range(start=start_time, end=end_time, freq=freq, tz=ny_tz)
            
            # Generate synthetic OHLCV data
            base_price = 40000 + np.random.normal(0, 500)  # Base price around 40000
            price_trend = np.random.normal(0, 0.001)  # Small random trend
            
            for i, timestamp in enumerate(time_index):
                # Add some randomness and trend
                price_change = np.random.normal(0, 50) + price_trend * i
                open_price = base_price + price_change
                high_price = open_price + abs(np.random.normal(0, 25))
                low_price = open_price - abs(np.random.normal(0, 25))
                close_price = open_price + np.random.normal(0, 20)
                volume = int(np.random.exponential(1000) + 500)
                
                all_data.append({
                    'ts_event': timestamp,
                    'open': open_price,
                    'high': high_price,
                    'low': low_price,
                    'close': close_price,
                    'volume': volume
                })
        
        # Create DataFrame
        df = pd.DataFrame(all_data)
        df.set_index('ts_event', inplace=True)
        
        print(f"Generated {len(df)} bars for {interval}")
        
        # Save to file
        output_file = f"../data/raw/MYM_FUT_{interval}_full.csv"
        df.to_csv(output_file)
        print(f"Saved to: {output_file}")
    
    print("\nTest data generation complete!")

if __name__ == '__main__':
    generate_test_data() 