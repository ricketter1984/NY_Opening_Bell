import os
import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
import pytz
import databento as db
from dotenv import load_dotenv
import warnings

# Load environment variables
load_dotenv()

class DatabentoDownloader:
    """
    Downloads 1-minute OHLCV data for multiple futures symbols using Databento API.
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
        
        # Define the NY session window for data fetching (09:25 ET to 10:30 ET)
        self.session_start_time_et = time(9, 25)
        self.session_end_time_et = time(10, 30)
        
        # Define all symbols to download - using more common symbol formats
        self.symbols = {
            'Indices': ['MYM', 'MES', 'MNQ', 'M2K'],  # Removed .FUT suffix
            'Metals': ['MGC', 'SIL'],
            'Energy': ['MCL', 'MNG'],
            'Currencies': ['M6E', 'M6B', 'M6A', 'MJY', 'MCD']
        }
        
        # Time range configuration
        self.start_date = '2025-04-30'
        self.end_date = '2025-07-29'
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
    
    def _process_symbol_data(self, symbol: str, data_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process and clean the downloaded data for a specific symbol.
        """
        if data_df.empty:
            return pd.DataFrame()
        
        # Set timestamp as index
        data_df.set_index('ts_event', inplace=True)
        
        # Convert to NY timezone
        if data_df.index.tz is None:
            data_df.index = data_df.index.tz_localize('UTC')
        data_df.index = data_df.index.tz_convert('America/New_York')
        
        # Filter for NY session times (9:25-10:30 ET)
        session_mask = (data_df.index.time >= self.session_start_time_et) & \
                      (data_df.index.time <= self.session_end_time_et)
        data_df = data_df[session_mask]
        
        return data_df
    
    def test_api_connection(self):
        """
        Test the API connection with a simple request.
        """
        print("🔍 Testing Databento API connection...")
        try:
            # Try to get symbols for a specific date
            test_date = datetime(2025, 5, 1)
            utc_start = self._convert_ny_to_utc(test_date, time(9, 0))
            utc_end = self._convert_ny_to_utc(test_date, time(10, 0))
            
            print(f"🔍 Testing symbol availability for {test_date.strftime('%Y-%m-%d')}...")
            
            # Test with a simple symbol first
            try:
                test_data = self.client.timeseries.get_range(
                    dataset=self.dataset,
                    schema=self.schema,
                    symbols=['MYM'],  # Try without .FUT suffix
                    start=utc_start.isoformat(),
                    end=utc_end.isoformat(),
                    stype_in='parent'
                ).to_df()
                
                if not test_data.empty:
                    print(f"✅ Found data for MYM: {len(test_data)} rows")
                    if 'symbol' in test_data.columns:
                        print(f"📊 Available symbols in data: {test_data['symbol'].unique()}")
                else:
                    print("⚠️  No data found for MYM")
                    
            except Exception as e:
                print(f"❌ Error testing MYM: {e}")
                
        except Exception as e:
            print(f"❌ API connection failed: {e}")
            return False
        
        return True
    
    def download_symbol_data(self, symbol: str) -> bool:
        """
        Download 1-minute OHLCV data for a specific symbol.
        
        Args:
            symbol (str): The futures symbol to download (e.g., 'MYM')
            
        Returns:
            bool: True if successful, False otherwise
        """
        print(f"\n📊 Downloading data for {symbol}...")
        
        all_data = []
        current_date = datetime.strptime(self.start_date, '%Y-%m-%d')
        end_date_dt = datetime.strptime(self.end_date, '%Y-%m-%d')
        
        successful_days = 0
        total_days = 0
        
        while current_date <= end_date_dt:
            # Skip weekends
            if current_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
                current_date += timedelta(days=1)
                continue
            
            total_days += 1
            print(f"  📅 Processing {current_date.strftime('%Y-%m-%d')}...", end=' ')
            
            # Convert session window to UTC for Databento API call
            utc_start = self._convert_ny_to_utc(current_date, self.session_start_time_et)
            utc_end = self._convert_ny_to_utc(current_date, self.session_end_time_et)
            
            try:
                # Fetch 1-minute OHLCV data
                data_1m = self.client.timeseries.get_range(
                    dataset=self.dataset,
                    schema=self.schema,
                    symbols=[symbol],
                    start=utc_start.isoformat(),
                    end=utc_end.isoformat(),
                    stype_in='parent'
                ).to_df()
                
                if not data_1m.empty:
                    # Filter for the specific symbol (in case multiple symbols returned)
                    if 'symbol' in data_1m.columns:
                        data_1m = data_1m[data_1m['symbol'] == symbol]
                    
                    if not data_1m.empty:
                        all_data.append(data_1m)
                        successful_days += 1
                        print("✅")
                    else:
                        print("⚠️  (No data for symbol)")
                else:
                    print("⚠️  (No data)")
                    
            except Exception as e:
                print(f"❌ Error: {str(e)[:50]}...")
            
            current_date += timedelta(days=1)
        
        if not all_data:
            print(f"❌ No data collected for {symbol}")
            return False
        
        # Combine all daily data
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"  📈 Collected {len(combined_df)} rows across {successful_days}/{total_days} days")
        
        # Process and clean the data
        processed_df = self._process_symbol_data(symbol, combined_df)
        
        if processed_df.empty:
            print(f"❌ No session data found for {symbol}")
            return False
        
        # Save to CSV
        output_file = f"data/raw/{symbol}_1m_full.csv"
        processed_df.to_csv(output_file)
        print(f"  💾 Saved to: {output_file}")
        print(f"  📊 Final data: {len(processed_df)} rows")
        
        return True
    
    def download_all_symbols(self):
        """
        Download data for all configured symbols.
        """
        print("🚀 Starting Databento OHLCV download for all symbols...")
        print(f"📅 Date range: {self.start_date} to {self.end_date}")
        print(f"⏰ Session window: {self.session_start_time_et} to {self.session_end_time_et} ET")
        print(f"📊 Dataset: {self.dataset}, Schema: {self.schema}")
        
        # First test the API connection
        if not self.test_api_connection():
            print("❌ API connection failed. Please check your API key and network connection.")
            return
        
        results = {}
        
        for category, symbols in self.symbols.items():
            print(f"\n{'='*50}")
            print(f"📋 Category: {category}")
            print(f"{'='*50}")
            
            for symbol in symbols:
                try:
                    success = self.download_symbol_data(symbol)
                    results[symbol] = success
                except Exception as e:
                    print(f"❌ Failed to download {symbol}: {e}")
                    results[symbol] = False
        
        # Print summary
        print(f"\n{'='*50}")
        print("📊 DOWNLOAD SUMMARY")
        print(f"{'='*50}")
        
        successful_symbols = [s for s, success in results.items() if success]
        failed_symbols = [s for s, success in results.items() if not success]
        
        print(f"✅ Successful downloads: {len(successful_symbols)}")
        for symbol in successful_symbols:
            print(f"  - {symbol}")
        
        if failed_symbols:
            print(f"\n❌ Failed downloads: {len(failed_symbols)}")
            for symbol in failed_symbols:
                print(f"  - {symbol}")
        
        print(f"\n🎉 Download process complete!")
        print(f"📁 Check 'data/raw/' directory for downloaded files.")

def main():
    """
    Main function to run the downloader.
    """
    try:
        downloader = DatabentoDownloader()
        downloader.download_all_symbols()
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        print("💡 Make sure your DATABENTO_API_KEY is set in the .env file")

if __name__ == '__main__':
    main() 