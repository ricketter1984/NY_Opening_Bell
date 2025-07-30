import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
import pytz
import databento as db
from dotenv import load_dotenv
import argparse

# Load environment variables
load_dotenv()

class DatabentoSingleDownloader:
    """
    Downloads historical OHLCV data for a single futures symbol using Databento API.
    """
    
    def __init__(self):
        """
        Initialize the downloader with API key and configuration.
        """
        self.api_key = os.getenv('DATABENTO_API_KEY')
        if not self.api_key:
            raise ValueError("DATABENTO_API_KEY not found in environment variables. "
                           "Please set it in your .env file.")
        
        self.client = db.Historical(self.api_key)
        self.ny_tz = pytz.timezone('America/New_York')
        self.utc_tz = pytz.utc
        
        # Default configuration
        self.dataset = 'GLBX.MDP3'
        self.schema = 'ohlcv-1m'
        
        # Create output directory
        os.makedirs('data/raw', exist_ok=True)
    
    def _convert_ny_to_utc(self, date_obj: datetime, time_obj: time) -> datetime:
        """
        Converts a given date and NY time to UTC datetime object.
        """
        ny_datetime = self.ny_tz.localize(datetime.combine(date_obj.date(), time_obj))
        return ny_datetime.astimezone(self.utc_tz)
    
    def _process_downloaded_data(self, data_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process and clean the downloaded data.
        """
        if data_df.empty:
            return pd.DataFrame()
        
        print(f"📊 Processing data with columns: {data_df.columns.tolist()}")
        
        # Check if we have the expected OHLCV columns
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        missing_cols = [col for col in required_cols if col not in data_df.columns]
        
        if missing_cols:
            print(f"❌ Missing required columns: {missing_cols}")
            return pd.DataFrame()
        
        # For now, just return the data as-is since we don't have timestamp info
        # The user can process it further as needed
        print(f"✅ Data has required OHLCV columns")
        
        return data_df
    
    def list_available_symbols(self, date_str: str = '2025-05-01'):
        """
        List available symbols for a given date.
        """
        print(f"🔍 Listing available symbols for {date_str}...")
        
        test_date = datetime.strptime(date_str, '%Y-%m-%d')
        ny_start = self.ny_tz.localize(datetime.combine(test_date.date(), time(9, 0)))
        ny_end = self.ny_tz.localize(datetime.combine(test_date.date(), time(10, 0)))
        utc_start = ny_start.astimezone(pytz.utc)
        utc_end = ny_end.astimezone(pytz.utc)
        
        # Test common symbol formats
        test_symbols = [
            'MYMM5', 'MYM.FUT', 'MYM', 
            'MES.FUT', 'MES', 
            'MNQ.FUT', 'MNQ',
            'M2K.FUT', 'M2K',
            'MGC.FUT', 'MGC',
            'SIL.FUT', 'SIL',
            'MCL.FUT', 'MCL',
            'MNG.FUT', 'MNG',
            'M6E.FUT', 'M6E',
            'M6B.FUT', 'M6B',
            'M6A.FUT', 'M6A',
            'MJY.FUT', 'MJY',
            'MCD.FUT', 'MCD'
        ]
        
        working_symbols = []
        
        for symbol in test_symbols:
            try:
                print(f"  Testing {symbol}...", end=' ')
                
                data = self.client.timeseries.get_range(
                    dataset=self.dataset,
                    schema=self.schema,
                    symbols=[symbol],
                    start=utc_start.isoformat(),
                    end=utc_end.isoformat(),
                    stype_in='raw_symbol',
                    stype_out='product_id'
                ).to_df()
                
                if not data.empty:
                    print(f"✅ Found {len(data)} rows")
                    working_symbols.append(symbol)
                else:
                    print("⚠️  No data")
                    
            except Exception as e:
                print(f"❌ Error: {str(e)[:50]}...")
        
        print(f"\n📊 Summary:")
        if working_symbols:
            print(f"✅ Working symbols ({len(working_symbols)}):")
            for symbol in working_symbols:
                print(f"  - {symbol}")
        else:
            print("❌ No working symbols found")
            print("💡 Try different symbol formats or check your API access")
    
    def download_symbol_data(self, symbol: str, start_date: str, end_date: str) -> bool:
        """
        Download 1-minute OHLCV data for a specific symbol.
        
        Args:
            symbol (str): The futures symbol to download (e.g., 'MYM.FUT')
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            bool: True if successful, False otherwise
        """
        print(f"📊 Downloading data for {symbol}...")
        print(f"📅 Date range: {start_date} to {end_date}")
        print(f"📊 Dataset: {self.dataset}, Schema: {self.schema}")
        
        # Convert dates to datetime objects
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Convert to UTC for API call - use full day range
        utc_start = self._convert_ny_to_utc(start_dt, time(0, 0))  # Start of day
        utc_end = self._convert_ny_to_utc(end_dt, time(23, 59))   # End of day
        
        try:
            print(f"🔍 Fetching data from Databento...")
            
            # Fetch 1-minute OHLCV data
            data_1m = self.client.timeseries.get_range(
                dataset=self.dataset,
                schema=self.schema,
                symbols=[symbol],
                start=utc_start.isoformat(),
                end=utc_end.isoformat(),
                stype_in='raw_symbol',
                stype_out='product_id'
            ).to_df()
            
            if data_1m.empty:
                print(f"❌ No data found for {symbol}")
                print(f"💡 Try running with --list-symbols to see available symbols")
                return False
            
            print(f"✅ Downloaded {len(data_1m)} rows of data")
            print(f"📋 Columns: {data_1m.columns.tolist()}")
            
            # Process and clean the data
            processed_df = self._process_downloaded_data(data_1m)
            
            if processed_df.empty:
                print(f"❌ No data after processing for {symbol}")
                return False
            
            # Generate output filename
            symbol_clean = symbol.replace('.', '_')
            output_file = f"data/raw/{symbol_clean}_{self.schema}_{start_date}_{end_date}.csv"
            
            # Save to CSV
            processed_df.to_csv(output_file)
            print(f"💾 Saved to: {output_file}")
            print(f"📊 Final data: {len(processed_df)} rows")
            print(f"📅 Data shape: {processed_df.shape}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error downloading data for {symbol}: {e}")
            return False

def main():
    """
    Main function to run the downloader.
    """
    parser = argparse.ArgumentParser(description='Download OHLCV data for a single symbol')
    parser.add_argument('symbol', nargs='?', help='Symbol to download (e.g., MYM.FUT)')
    parser.add_argument('start_date', nargs='?', help='Start date (YYYY-MM-DD)')
    parser.add_argument('end_date', nargs='?', help='End date (YYYY-MM-DD)')
    parser.add_argument('--list-symbols', action='store_true', help='List available symbols')
    
    args = parser.parse_args()
    
    try:
        downloader = DatabentoSingleDownloader()
        
        if args.list_symbols:
            downloader.list_available_symbols()
            return
        
        if not args.symbol or not args.start_date or not args.end_date:
            print("❌ Missing required arguments")
            print("Usage examples:")
            print("  python download_ohlcv_single.py --list-symbols")
            print("  python download_ohlcv_single.py MYMM5 2025-05-01 2025-05-02")
            print("  python download_ohlcv_single.py MES.FUT 2025-05-01 2025-05-02")
            return
        
        success = downloader.download_symbol_data(args.symbol, args.start_date, args.end_date)
        
        if success:
            print(f"\n🎉 Download completed successfully!")
            print(f"📁 Check 'data/raw/' directory for the downloaded file")
        else:
            print(f"\n❌ Download failed!")
            print(f"\n💡 Try these troubleshooting steps:")
            print(f"   1. Run: python download_ohlcv_single.py --list-symbols")
            print(f"   2. Check symbol format (e.g., MYMM5 vs MYM.FUT)")
            print(f"   3. Verify your DATABENTO_API_KEY in .env file")
            
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        print("💡 Make sure your DATABENTO_API_KEY is set in the .env file")

if __name__ == '__main__':
    main() 