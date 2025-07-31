import os
import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
import pytz
import databento as db
from dotenv import load_dotenv

# Load environment variables from .env file (if present)
load_dotenv()

class DataLoader:
    """
    Handles fetching historical OHLCV data from Databento and resampling it
    to various timeframes for the NY Opening Bell strategy.
    """

    def __init__(self, api_key: str = None):
        """
        Initializes the DataLoader with a Databento API key.
        If api_key is None, it attempts to load from DATABENTO_API_KEY environment variable.
        """
        if api_key is None:
            self.api_key = os.getenv('DATABENTO_API_KEY')
            if not self.api_key:
                raise ValueError("Databento API key not found. "
                                 "Please set DATABENTO_API_KEY environment variable or pass it to DataLoader.")
        else:
            self.api_key = api_key

        self.client = db.Historical(self.api_key)
        self.ny_tz = pytz.timezone('America/New_York')
        self.utc_tz = pytz.utc
        
        # Define the NY session window for data fetching (09:25 ET to 10:30 ET)
        self.session_start_time_et = time(9, 25)
        self.session_end_time_et = time(10, 30)

    def _convert_ny_to_utc(self, date_obj: datetime, time_obj: time) -> datetime:
        """
        Converts a given date and NY time to UTC datetime object.
        """
        ny_datetime = self.ny_tz.localize(datetime.combine(date_obj.date(), time_obj))
        return ny_datetime.astimezone(self.utc_tz)

    def fetch_and_resample_data(self, 
                                symbol: str, 
                                start_date: str, 
                                end_date: str,
                                intervals: list = ['1m', '2m', '3m', '5m', '10m', '15m'],
                                dataset: str = 'GLBX.MDP3', # Common for CME futures
                                save_path: str = 'data/raw/') -> dict:
        """
        Fetches 1-minute OHLCV data for a given symbol and date range,
        then resamples it into specified intervals and saves to CSV.

        Args:
            symbol (str): The futures contract symbol (e.g., 'MYM.FUT' for continuous).
            start_date (str): Start date in 'YYYY-MM-DD' format.
            end_date (str): End date in 'YYYY-MM-DD' format.
            intervals (list): List of time intervals for resampling (e.g., ['1m', '5m']).
            dataset (str): Databento dataset ID.
            save_path (str): Directory to save the raw CSV files.

        Returns:
            dict: A dictionary where keys are intervals and values are pandas DataFrames
                  containing the aggregated data for the entire period.
        """
        all_resampled_data = {interval: [] for interval in intervals}
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')

        os.makedirs(save_path, exist_ok=True)

        while current_date <= end_date_dt:
            print(f"Processing data for {symbol} on {current_date.strftime('%Y-%m-%d')}...")
            
            # Convert session window to UTC for Databento API call
            utc_start = self._convert_ny_to_utc(current_date, self.session_start_time_et)
            utc_end = self._convert_ny_to_utc(current_date, self.session_end_time_et)

            try:
                # Fetch 1-minute OHLCV data
                data_1m = self.client.timeseries.get_range(
                    dataset=dataset,
                    schema='ohlcv-1m', # Requesting 1-minute schema
                    symbols=[symbol],
                    start=utc_start.isoformat(),
                    end=utc_end.isoformat(),
                    stype_in='parent' 
                ).to_df()

                if data_1m.empty:
                    print(f"No 1-minute data found for {symbol} on {current_date.strftime('%Y-%m-%d')} within session window. This might be due to incorrect symbol, date range, or data availability for 'ohlcv-1m' schema. Skipping.")
                    current_date += timedelta(days=1)
                    continue # Skip to next day if data_1m is empty
                else:
                    # ONLY PROCESS IF data_1m IS NOT EMPTY
                    data_1m.set_index('ts_event', inplace=True)
                    data_1m.index = pd.to_datetime(data_1m.index, unit='ns', utc=True).tz_convert(self.ny_tz)

                    # Filter to exact session slice
                    data_1m = data_1m.between_time(self.session_start_time_et, self.session_end_time_et)

                    if data_1m.empty: # Check again after time filtering
                        print(f"No 1-minute data found for {symbol} on {current_date.strftime('%Y-%m-%d')} after session filtering. Skipping.")
                        current_date += timedelta(days=1)
                        continue

                    # Resample and save for each requested interval
                    for interval in intervals:
                        if interval == '1m': 
                            resampled_df = data_1m # Use the fetched 1-minute data directly
                        else:
                            # Resample from the 1-minute data to the desired interval
                            resampled_df = data_1m.resample(interval, origin='start_day').agg({
                                'open': 'first',
                                'high': 'max',
                                'low': 'min',
                                'close': 'last',
                                'volume': 'sum'
                            }).dropna() # Drop any intervals that might not have data

                        if not resampled_df.empty:
                            # Add a 'date' column for easier filtering later if needed
                            resampled_df['date'] = resampled_df.index.date
                            all_resampled_data[interval].append(resampled_df)
                        
            except Exception as e: # Catch all exceptions for robustness
                print(f"An error occurred for {symbol} on {current_date.strftime('%Y-%m-%d')}: {e}")
                current_date += timedelta(days=1) # Ensure we move to the next day even on error
                continue # Continue to the next day's processing
        
        # Concatenate all daily data for each interval into a single DataFrame
        final_data_frames = {}
        for interval, df_list in all_resampled_data.items():
            if df_list:
                final_data_frames[interval] = pd.concat(df_list).sort_index()
                # Save the full concatenated DataFrame for each interval
                full_file_name = f"{symbol.replace('.', '_')}_{interval}_full.csv" # Replace . for valid filename
                final_data_frames[interval].to_csv(os.path.join(save_path, full_file_name))
                print(f"Saved aggregated {interval} data to {os.path.join(save_path, full_file_name)}")
            else:
                print(f"No data collected for interval: {interval}")

        return final_data_frames

if __name__ == '__main__':
    # Example usage:
    # Ensure DATABENTO_API_KEY is set in your environment or .env file
    # Example: DATABENTO_API_KEY="YOUR_DATABENTO_API_KEY" in a .env file at the project root

    loader = DataLoader()

    # Test Symbol & Dates
    # 'MYM.FUT' is a 'parent' symbology.
    # The date range must correspond to when MYM.FUT was the active continuous contract.
    test_symbol = 'MYM.FUT' 
    test_start_date = '2025-04-30' # As per your project brief
    test_end_date = '2025-07-29' # As per your project brief

    print(f"Starting data fetch and resampling for {test_symbol} from {test_start_date} to {test_end_date}")
    
    # This will fetch 1-minute data and save it to data/raw/
    # It will also resample and save 2m, 3m, 5m, 10m, 15m data.
    all_interval_data = loader.fetch_and_resample_data(
        symbol=test_symbol,
        start_date=test_start_date,
        end_date=test_end_date,
        intervals=['1m', '2m', '3m', '5m', '10m', '15m'], # Fetching all desired intervals
        save_path='data/raw/' 
    )

    print("\nData loading and resampling complete.")
    for interval, df in all_interval_data.items():
        print(f"Interval: {interval}, Total Bars: {len(df)}")
        # print(df.head()) # Uncomment to see the head of each DataFrame
