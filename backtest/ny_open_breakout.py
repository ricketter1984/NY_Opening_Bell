import pandas as pd
import os
from datetime import datetime, time, timedelta
import pytz
import numpy as np

# Import strategy and metrics classes
from strategy_momentum import MomentumStrategy
from strategy_reversal import ReversalStrategy
from metrics import PerformanceMetrics

class NYOpenBacktester:
    """
    Orchestrates the backtesting of NY Open strategies.
    Loads 1-minute data, resamples, runs strategies, and compiles results.
    """

    def __init__(self, 
                 data_path_prefix: str, # Changed to prefix as loader saves multiple files
                 output_dir: str = 'results/',
                 ny_open_time: time = time(9, 30),
                 session_end_time: time = time(10, 00) # User specified 09:30-10:00 ET
                ):
        """
        Initializes the backtester.

        Args:
            data_path_prefix (str): Prefix for the loaded data files (e.g., 'data/raw/MYM_FUT_').
                                    Assumes loader.py saves files like 'MYM_FUT_1m_full.csv', etc.
            output_dir (str): Directory to save backtest results.
            ny_open_time (time): The exact New York session open time (ET).
            session_end_time (time): The end time for the session window to analyze (ET).
        """
        self.data_path_prefix = data_path_prefix
        self.output_dir = output_dir
        self.ny_tz = pytz.timezone('America/New_York')
        self.utc_tz = pytz.utc # Databento typically uses UTC for raw timestamps
        self.ny_open_time = ny_open_time
        self.session_end_time = session_end_time

        os.makedirs(self.output_dir, exist_ok=True)

    def _load_resampled_data(self, interval: str) -> pd.DataFrame:
        """
        Loads the pre-resampled OHLCV data for a specific interval.
        """
        file_path = f"{self.data_path_prefix}{interval}_full.csv"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Resampled data file not found at: {file_path}. "
                                    f"Please ensure loader.py has been run to generate this file.")

        print(f"Loading {interval} data from: {file_path}")
        df = pd.read_csv(file_path, index_col='ts_event', parse_dates=True)
        
        # Ensure index is datetime and localize to NY timezone as it was saved that way by loader
        if df.index.tz is None:
            df.index = df.index.tz_localize(self.ny_tz)
        else:
            df.index = df.index.tz_convert(self.ny_tz)
        
        # Filter to the specific session window if not already done perfectly by loader
        # (though loader should handle this, it's a good safety check)
        df = df.between_time(self.ny_open_time, self.session_end_time)
        print(f"{interval} data loaded and localized to New York time. Total bars: {len(df)}")
        return df

    def run_backtest(self, 
                     intervals: list = ['1m', '2m', '3m', '5m', '10m', '15m'],
                     momentum_params: dict = None,
                     reversal_params: dict = None
                    ) -> pd.DataFrame:
        """
        Runs the backtest for both momentum and reversal strategies across specified intervals.

        Args:
            intervals (list): List of time intervals to backtest.
            momentum_params (dict): Dictionary of parameters for MomentumStrategy.
            reversal_params (dict): Dictionary of parameters for ReversalStrategy.

        Returns:
            pd.DataFrame: A consolidated DataFrame of all trades from all strategies and intervals.
        """
        all_trades = []

        for interval in intervals:
            print(f"\n--- Running backtest for {interval} interval ---")
            try:
                df_interval_data = self._load_resampled_data(interval)
            except FileNotFoundError as e:
                print(f"Skipping {interval} due to missing file: {e}")
                continue
            except Exception as e:
                print(f"Error loading {interval} data: {e}. Skipping.")
                continue

            if df_interval_data.empty:
                print(f"No data for {interval} interval. Skipping strategies for this interval.")
                continue

            # Group by date to run strategy day by day
            unique_dates = df_interval_data.index.normalize().unique()
            print(f"Backtesting {len(unique_dates)} days for {interval} interval.")

            for current_date in unique_dates:
                # Filter data for the current day's session window
                df_daily_session = df_interval_data[df_interval_data.index.date == current_date.date()].copy()
                
                if df_daily_session.empty:
                    print(f"No {interval} data for session {current_date.strftime('%Y-%m-%d')}. Skipping.")
                    continue

                # --- Run Momentum Strategy ---
                current_momentum_params = momentum_params.copy() if momentum_params else {}
                current_momentum_params['interval'] = interval 
                momentum_strategy = MomentumStrategy(**current_momentum_params)
                momentum_trades = momentum_strategy.run_strategy(df_daily_session)
                if not momentum_trades.empty:
                    momentum_trades['date'] = current_date.date()
                    momentum_trades['timeframe'] = interval
                    momentum_trades['strategy_type'] = 'Momentum'
                    all_trades.append(momentum_trades)

                # --- Run Reversal Strategy ---
                current_reversal_params = reversal_params.copy() if reversal_params else {}
                current_reversal_params['interval'] = interval
                reversal_strategy = ReversalStrategy(**current_reversal_params)
                reversal_trades = reversal_strategy.run_strategy(df_daily_session)
                if not reversal_trades.empty:
                    reversal_trades['date'] = current_date.date()
                    reversal_trades['timeframe'] = interval
                    reversal_trades['strategy_type'] = 'Reversal'
                    all_trades.append(reversal_trades)
        
        if not all_trades:
            print("No trades generated during the backtest.")
            return pd.DataFrame() # Return empty DataFrame if no trades

        consolidated_trades = pd.concat(all_trades, ignore_index=True)
        
        # Save detailed per-trade logs
        detailed_log_path = os.path.join(self.output_dir, 'detailed_trades_log.csv')
        consolidated_trades.to_csv(detailed_log_path, index=False)
        print(f"\nDetailed trade log saved to: {detailed_log_path}")

        return consolidated_trades

    def generate_summary_report(self, consolidated_trades_df: pd.DataFrame):
        """
        Generates and prints a summary report using PerformanceMetrics.
        """
        metrics_calculator = PerformanceMetrics()

        if consolidated_trades_df.empty:
            print("\nNo trades to generate a summary report.")
            return

        print("\n--- Consolidated Backtest Summary ---")
        overall_metrics = metrics_calculator.calculate_metrics(consolidated_trades_df)
        for key, value in overall_metrics.items():
            print(f"{key.replace('_', ' ').title()}: {value}")

        print("\n--- Summary by Strategy Type ---")
        for strategy_type in consolidated_trades_df['strategy_type'].unique():
            strategy_df = consolidated_trades_df[consolidated_trades_df['strategy_type'] == strategy_type]
            strategy_metrics = metrics_calculator.calculate_metrics(strategy_df)
            print(f"\nStrategy: {strategy_type}")
            for key, value in strategy_metrics.items():
                print(f"  {key.replace('_', ' ').title()}: {value}")

        print("\n--- Summary by Timeframe ---")
        for timeframe in consolidated_trades_df['timeframe'].unique():
            timeframe_df = consolidated_trades_df[consolidated_trades_df['timeframe'] == timeframe]
            timeframe_metrics = metrics_calculator.calculate_metrics(timeframe_df)
            print(f"\nTimeframe: {timeframe}")
            for key, value in timeframe_metrics.items():
                print(f"  {key.replace('_', ' ').title()}: {value}")

        # You can add more granular summaries (e.g., Strategy x Timeframe) here

if __name__ == '__main__':
    # Define the prefix for your pre-processed data files.
    # Assuming loader.py saves files like 'data/raw/MYM_FUT_1m_full.csv', 'data/raw/MYM_FUT_2m_full.csv', etc.
    data_file_prefix = '../data/raw/MYM_FUT_' 

    # Define strategy parameters (these can be tuned later)
    momentum_strategy_params = {
        'stop_loss_method': 'atr',
        'atr_period': 14,
        'atr_multiplier': 2.0,
        'take_profit_r_multiple': 1.5
    }

    reversal_strategy_params = {
        'stop_loss_method': 'atr',
        'atr_period': 14,
        'atr_multiplier': 3.0, # Often wider stop for reversals
        'take_profit_r_multiple': 2.0,
        'flush_bar_min_range_pct': 0.25 # Tune this for what constitutes a 'strong' flush
    }

    # Initialize and run the backtester
    backtester = NYOpenBacktester(
        data_path_prefix=data_file_prefix,
        output_dir='../results/',
        ny_open_time=time(9, 30),
        session_end_time=time(10, 00) # As specified in the brief
    )

    # Define the intervals to backtest (these should match what loader.py generates)
    target_intervals = ['1m', '2m', '3m', '5m', '10m', '15m']
    
    consolidated_trades = backtester.run_backtest(
        intervals=target_intervals,
        momentum_params=momentum_strategy_params,
        reversal_params=reversal_strategy_params
    )

    # Generate and print the summary report
    backtester.generate_summary_report(consolidated_trades)

    print("\nBacktesting process complete. Check the 'results/' directory for trade logs.")
