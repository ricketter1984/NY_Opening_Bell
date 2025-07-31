import os
import pandas as pd
from datetime import datetime, time
import pytz
import databento as db
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_databento_connection():
    """
    Test Databento API connection and symbol availability.
    """
    print("🔍 Testing Databento API connection...")
    
    api_key = os.getenv('DATABENTO_API_KEY')
    if not api_key:
        print("❌ DATABENTO_API_KEY not found in environment variables")
        return False
    
    try:
        client = db.Historical(api_key)
        ny_tz = pytz.timezone('America/New_York')
        
        # Test with a single day - proper time range
        test_date = datetime(2025, 5, 1)
        ny_start = ny_tz.localize(datetime.combine(test_date.date(), time(9, 0)))
        ny_end = ny_tz.localize(datetime.combine(test_date.date(), time(10, 0)))
        utc_start = ny_start.astimezone(pytz.utc)
        utc_end = ny_end.astimezone(pytz.utc)
        
        print(f"🔍 Testing symbol availability for {test_date.strftime('%Y-%m-%d')} 09:00-10:00 ET...")
        
        # Test different symbol formats
        test_symbols = ['MYM.FUT', 'MYM', 'MES.FUT', 'MES']
        
        for symbol in test_symbols:
            try:
                print(f"  Testing {symbol}...", end=' ')
                
                data = client.timeseries.get_range(
                    dataset='GLBX.MDP3',
                    schema='ohlcv-1m',
                    symbols=[symbol],
                    start=utc_start.isoformat(),
                    end=utc_end.isoformat(),
                    stype_in='raw_symbol',
                    stype_out='product_id'
                ).to_df()
                
                if not data.empty:
                    print(f"✅ Found {len(data)} rows")
                    print(f"    Columns: {data.columns.tolist()}")
                    if 'symbol' in data.columns:
                        print(f"    Symbols: {data['symbol'].unique()}")
                else:
                    print("⚠️  No data")
                    
            except Exception as e:
                print(f"❌ Error: {str(e)[:50]}...")
        
        return True
        
    except Exception as e:
        print(f"❌ API connection failed: {e}")
        return False

if __name__ == '__main__':
    test_databento_connection() 