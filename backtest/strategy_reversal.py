import pandas as pd
import numpy as np
from datetime import time
from stoploss import atr_stop, structure_stop, bar_range_stop, calculate_atr

class ReversalStrategy:
    """
    Implements the Reversal strategy for the NY Opening Bell.
    This strategy aims to capture reversals after an initial "flush" move.
    """

    def __init__(self, 
                 interval: str,
                 stop_loss_method: str = 'atr', # 'atr', 'structure', 'bar_range'
                 atr_period: int = 14,
                 atr_multiplier: float = 2.0,
                 structure_lookback_bars: int = 3,
                 bar_range_pct: float = 0.5,
                 take_profit_r_multiple: float = 1.5, # Fixed R-multiple for simplicity initially
                 flush_bar_min_range_pct: float = 0.5 # Min percentage of daily range for a bar to be considered a 'flush'
                ):
        """
        Initializes the ReversalStrategy with parameters for entry and stop-loss/take-profit.

        Args:
            interval (str): The timeframe interval (e.g., '1m', '5m').
            stop_loss_method (str): Method to use for stop loss ('atr', 'structure', 'bar_range').
            atr_period (int): Period for ATR calculation.
            atr_multiplier (float): Multiplier for ATR stop loss.
            structure_lookback_bars (int): Number of bars to look back for structure stop.
            bar_range_pct (float): Percentage of bar range for bar range stop.
            take_profit_r_multiple (float): R-multiple for fixed take profit.
            flush_bar_min_range_pct (float): Minimum percentage of the session's first hour range
                                             for a bar to be considered a "flush" bar.
        """
        self.interval = interval
        self.stop_loss_method = stop_loss_method
        self.atr_period = atr_period
        self.atr_multiplier = atr_multiplier
        self.structure_lookback_bars = structure_lookback_bars
        self.bar_range_pct = bar_range_pct
        self.take_profit_r_multiple = take_profit_r_multiple
        self.flush_bar_min_range_pct = flush_bar_min_range_pct
        self.ny_open_time = time(9, 30) # New York session open time

    def _calculate_stop_loss(self, df: pd.DataFrame, entry_time_idx: pd.Timestamp, 
                             entry_price: float, direction: str) -> float:
        """
        Helper to calculate stop loss based on the chosen method.
        This is a duplicate of the one in MomentumStrategy for now,
        but can be customized for reversal specifics if needed.
        """
        if self.stop_loss_method == 'atr':
            atr_series = calculate_atr(df, period=self.atr_period)
            current_atr = atr_series.loc[:entry_time_idx].iloc[-1] if not atr_series.loc[:entry_time_idx].empty else np.nan
            if pd.isna(current_atr):
                print(f"Warning: ATR not available at {entry_time_idx}. Using a default small stop.")
                return entry_price * (0.995 if direction == 'long' else 1.005) # 0.5% stop
            return atr_stop(entry_price, current_atr, direction, self.atr_multiplier)
        
        elif self.stop_loss_method == 'structure':
            return structure_stop(df, entry_time_idx, direction, self.structure_lookback_bars)
        
        elif self.stop_loss_method == 'bar_range':
            # For bar_range_stop in reversal, reference bar could be the flush bar or the first reversal bar
            # For simplicity, let's use the bar immediately preceding the entry bar (the second reversal bar)
            entry_loc = df.index.get_loc(entry_time_idx)
            if entry_loc >= 1:
                ref_bar_idx = df.index[entry_loc - 1] 
                ref_bar_high = df.loc[ref_bar_idx]['high']
                ref_bar_low = df.loc[ref_bar_idx]['low']
                return bar_range_stop(entry_price, ref_bar_high, ref_bar_low, direction, self.bar_range_pct)
            else:
                print(f"Warning: Not enough bars for bar_range stop at {entry_time_idx}. Using a default small stop.")
                return entry_price * (0.995 if direction == 'long' else 1.005) # 0.5% stop
        else:
            raise ValueError(f"Unknown stop loss method: {self.stop_loss_method}")

    def _is_flush_bar(self, bar: pd.Series, session_range: float) -> bool:
        """
        Determines if a bar is a "flush" bar based on its range relative to the session's range.
        A "flush" bar is a large, strong directional bar.
        """
        if session_range == 0:
            return False
        bar_range = bar['high'] - bar['low']
        return (bar_range / session_range) >= self.flush_bar_min_range_pct

    def _check_confirmation_indicators(self, df: pd.DataFrame, current_bar_idx: pd.Timestamp, direction: str) -> bool:
        """
        Placeholder for checking CVD divergence, MACD shift, and Stochastic confirmation.
        This function should be expanded with your specific logic.

        Args:
            df (pd.DataFrame): The full DataFrame for the day.
            current_bar_idx (pd.Timestamp): The timestamp of the current bar (e.g., the 3rd reversal bar).
            direction (str): 'long' or 'short'.

        Returns:
            bool: True if all confirmation indicators align for the reversal, False otherwise.
        """
        # --- Placeholder for your indicator logic ---
        # 1. Calculate CVD (Cumulative Volume Delta) and check for divergence
        #    Example: If price makes new low but CVD makes higher low (bullish divergence for long)
        # 2. Calculate MACD and check for shift/cross
        #    Example: MACD line crossing above signal line (bullish for long)
        # 3. Calculate Stochastics (Fast/Slow) and check for overbought/oversold and cross
        #    Example: Stochastics moving up from oversold (bullish for long)

        # For now, always return True to allow basic backtesting of the price action
        # You will replace this with your actual indicator logic.
        # print(f"Checking indicators at {current_bar_idx} for {direction} reversal...")
        # Add your actual indicator calculations and conditions here
        
        # Example: Simple check for a very strong reversal candle (replace with your actual logic)
        # This is just a conceptual placeholder.
        # if direction == 'long' and df.loc[current_bar_idx]['close'] > df.loc[current_bar_idx]['open'] * 1.005:
        #     return True
        # elif direction == 'short' and df.loc[current_bar_idx]['close'] < df.loc[current_bar_idx]['open'] * 0.995:
        #     return True
        # return False
        
        return True # TEMPORARY: Always returns True for initial testing. REPLACE THIS!

    def run_strategy(self, df_daily: pd.DataFrame) -> pd.DataFrame:
        """
        Runs the reversal strategy on a single day's OHLCV data.

        Args:
            df_daily (pd.DataFrame): OHLCV data for a single trading day,
                                     indexed by timestamp (e.g., 1m, 5m bars).
                                     Assumes index is localized to America/New_York.

        Returns:
            pd.DataFrame: A DataFrame of simulated trades for the day.
                          Columns: entry_time, direction, entry_price, sl, tp,
                                   exit_time, exit_price, outcome, R_multiple.
        """
        trades = []
        df_daily = df_daily.sort_index()

        # Find the index of the 9:30 AM ET bar (Bar 1)
        try:
            ny_open_bar_idx = df_daily.index[df_daily.index.time >= self.ny_open_time][0]
            ny_open_bar_loc = df_daily.index.get_loc(ny_open_bar_idx)
        except IndexError:
            return pd.DataFrame(columns=['entry_time', 'direction', 'entry_price', 'sl', 'tp', 
                                         'exit_time', 'exit_price', 'outcome', 'R_multiple'])

        # We need at least 3 bars for flush + 2 reversal bars + 1 entry bar
        # So, we need at least 4 bars after the open (flush, reversal1, reversal2, entry)
        if ny_open_bar_loc + 3 >= len(df_daily):
            return pd.DataFrame(columns=['entry_time', 'direction', 'entry_price', 'sl', 'tp', 
                                         'exit_time', 'exit_price', 'outcome', 'R_multiple'])

        # Calculate the range of the first hour (approx) for flush bar detection
        # This is a rough estimation, adjust as needed.
        session_first_hour_end_time = (pd.to_datetime(ny_open_bar_idx.date()) + pd.Timedelta(hours=10, minutes=30)).time()
        first_hour_df = df_daily.between_time(self.ny_open_time, session_first_hour_end_time)
        session_range = first_hour_df['high'].max() - first_hour_df['low'].min() if not first_hour_df.empty else 0


        # Iterate through bars starting from the NY open to find a flush and reversal
        for i in range(ny_open_bar_loc, len(df_daily) - 2): # Need at least 3 bars ahead for reversal pattern
            flush_bar = df_daily.iloc[i]
            reversal_bar1 = df_daily.iloc[i + 1]
            reversal_bar2 = df_daily.iloc[i + 2] # This is the confirmation bar for entry

            is_flush = self._is_flush_bar(flush_bar, session_range)
            
            if not is_flush:
                continue

            trade_direction = None
            if flush_bar['close'] < flush_bar['open']: # Initial flush down
                # Check for reversal up (reversal_bar1 and reversal_bar2 close higher)
                if reversal_bar1['close'] > reversal_bar1['open'] and \
                   reversal_bar2['close'] > reversal_bar2['open']:
                    trade_direction = 'long'
            elif flush_bar['close'] > flush_bar['open']: # Initial flush up
                # Check for reversal down (reversal_bar1 and reversal_bar2 close lower)
                if reversal_bar1['close'] < reversal_bar1['open'] and \
                   reversal_bar2['close'] < reversal_bar2['open']:
                    trade_direction = 'short'

            if trade_direction:
                # Check additional confirmation indicators before entering
                if self._check_confirmation_indicators(df_daily, reversal_bar2.name, trade_direction):
                    entry_time = reversal_bar2.name # Timestamp of Bar 3 (reversal_bar2)
                    entry_price = reversal_bar2['open'] # Enter at the open of Bar 3

                    # Calculate Stop Loss
                    stop_loss = self._calculate_stop_loss(df_daily, entry_time, entry_price, trade_direction)
                    
                    # Calculate Take Profit (fixed R-multiple for now)
                    risk_per_share = abs(entry_price - stop_loss)
                    if risk_per_share == 0: # Avoid division by zero if SL is at entry (unlikely but safe)
                        continue 

                    if trade_direction == 'long':
                        take_profit = entry_price + (risk_per_share * self.take_profit_r_multiple)
                    else: # short
                        take_profit = entry_price - (risk_per_share * self.take_profit_r_multiple)

                    # Simulate trade execution from entry bar onwards
                    exit_price = np.nan
                    outcome = 'N/A'
                    r_multiple = np.nan
                    exit_time = np.nan

                    # Slice df from entry bar onwards for simulation
                    simulation_df = df_daily.loc[entry_time:].iloc[1:] # Start from the bar *after* entry bar open

                    for j, current_bar in simulation_df.iterrows():
                        if trade_direction == 'long':
                            # Check for stop loss hit
                            if current_bar['low'] <= stop_loss:
                                exit_price = stop_loss
                                outcome = 'Loss'
                                r_multiple = -1.0 
                                exit_time = current_bar.name
                                break
                            # Check for take profit hit
                            elif current_bar['high'] >= take_profit:
                                exit_price = take_profit
                                outcome = 'Win'
                                r_multiple = self.take_profit_r_multiple
                                exit_time = current_bar.name
                                break
                        elif trade_direction == 'short':
                            # Check for stop loss hit
                            if current_bar['high'] >= stop_loss:
                                exit_price = stop_loss
                                outcome = 'Loss'
                                r_multiple = -1.0
                                exit_time = current_bar.name
                                break
                            # Check for take profit hit
                            elif current_bar['low'] <= take_profit:
                                exit_price = take_profit
                                outcome = 'Win'
                                r_multiple = self.take_profit_r_multiple
                                exit_time = current_bar.name
                                break
                    
                    # If trade did not hit SL/TP by end of session, close at last bar's close
                    if pd.isna(exit_price):
                        exit_price = simulation_df.iloc[-1]['close'] if not simulation_df.empty else entry_price
                        exit_time = simulation_df.index[-1] if not simulation_df.empty else entry_time
                        pnl = (exit_price - entry_price) if trade_direction == 'long' else (entry_price - exit_price)
                        r_multiple = pnl / risk_per_share if risk_per_share != 0 else 0
                        outcome = 'Partial Win/Loss (Session End)'

                    trades.append({
                        'entry_time': entry_time,
                        'direction': trade_direction,
                        'entry_price': entry_price,
                        'sl': stop_loss,
                        'tp': take_profit,
                        'exit_time': exit_time,
                        'exit_price': exit_price,
                        'outcome': outcome,
                        'R_multiple': r_multiple
                    })
                    # Only take one trade per day for simplicity in this initial version
                    break 

        return pd.DataFrame(trades)

if __name__ == '__main__':
    # Example usage:
    # Create a dummy DataFrame for a single day, 5-minute interval
    from datetime import timedelta
    import pytz

    ny_tz = pytz.timezone('America/New_York')
    
    # Generate 5-minute bars from 9:25 ET to 10:30 ET
    start_dt = ny_tz.localize(datetime(2024, 7, 29, 9, 25))
    end_dt = ny_tz.localize(datetime(2024, 7, 29, 10, 30))
    
    # Create a sequence of 5-minute intervals
    time_index = pd.date_range(start=start_dt, end=end_dt, freq='5min', tz=ny_tz)

    # Synthetic OHLCV data - simulating a downward flush then a reversal
    data_dict_down_flush_up_reversal = {
        'open':  [100, 98,  95,  96,  97,  98,  99,  98,  97,  96,  95,  94,  93],
        'high':  [101, 99,  96,  97,  98,  99, 100,  99,  98,  97,  96,  95,  94],
        'low':   [97,  94,  92,  94,  95,  96,  97,  96,  95,  94,  93,  92,  91],
        'close': [98,  95,  93,  95,  96,  97,  98,  97,  96,  95,  94,  93,  92],
        'volume': [2000, 3500, 4000, 1500, 1200, 1000, 900, 800, 700, 600, 500, 400, 300]
    }
    # Adjusting data to clearly show a flush followed by two bullish bars
    # Bar 1 (9:30): 100 -> 95 (flush down)
    # Bar 2 (9:35): 95 -> 97 (reversal up)
    # Bar 3 (9:40): 97 -> 99 (reversal up, entry at open 97)
    data_dict_reversal_example = {
        'open':  [100, 95, 97, 99, 98, 97, 96, 95, 94, 93, 92],
        'high':  [101, 96, 98, 100, 99, 98, 97, 96, 95, 94, 93],
        'low':   [94, 93, 95, 97, 96, 95, 94, 93, 92, 91, 90],
        'close': [95, 97, 99, 98, 97, 96, 95, 94, 93, 92, 91],
        'volume': [5000, 2000, 1800, 1000, 900, 800, 700, 600, 500, 400, 300]
    }
    df_test_day_reversal = pd.DataFrame(data_dict_reversal_example, index=time_index[:len(data_dict_reversal_example)])


    print("Test Day Data (5m interval for Reversal):\n", df_test_day_reversal)

    # Initialize strategy with ATR stop for reversal
    reversal_strategy_atr = ReversalStrategy(
        interval='5m', 
        stop_loss_method='atr', 
        atr_period=5, # Shorter period for this small test data
        atr_multiplier=3.0, # Larger multiplier for potentially wider initial moves
        take_profit_r_multiple=2.0,
        flush_bar_min_range_pct=0.2 # Bar range must be at least 20% of session range to be a "flush"
    )
    trades_reversal_atr = reversal_strategy_atr.run_strategy(df_test_day_reversal)
    print("\nTrades with ATR Stop (Reversal Strategy):\n", trades_reversal_atr)

    # You can similarly test with 'structure' and 'bar_range' stop methods
    # reversal_strategy_struct = ReversalStrategy(
    #     interval='5m', 
    #     stop_loss_method='structure', 
    #     structure_lookback_bars=2,
    #     take_profit_r_multiple=2.0,
    #     flush_bar_min_range_pct=0.2
    # )
    # trades_reversal_struct = reversal_strategy_struct.run_strategy(df_test_day_reversal)
    # print("\nTrades with Structure Stop (Reversal Strategy):\n", trades_reversal_struct)
