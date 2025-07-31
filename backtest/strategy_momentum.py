import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta # Added datetime and timedelta for test data generation
from stoploss import atr_stop, structure_stop, bar_range_stop, calculate_atr

class MomentumStrategy:
    """
    Implements the Momentum Continuation strategy for the NY Opening Bell.
    """

    def __init__(self, 
                 interval: str,
                 stop_loss_method: str = 'atr', # 'atr', 'structure', 'bar_range'
                 atr_period: int = 14,
                 atr_multiplier: float = 2.0,
                 structure_lookback_bars: int = 3,
                 bar_range_pct: float = 0.5,
                 take_profit_r_multiple: float = 1.5 # Fixed R-multiple for simplicity initially
                ):
        """
        Initializes the MomentumStrategy with parameters for entry and stop-loss/take-profit.

        Args:
            interval (str): The timeframe interval (e.g., '1m', '5m').
            stop_loss_method (str): Method to use for stop loss ('atr', 'structure', 'bar_range').
            atr_period (int): Period for ATR calculation.
            atr_multiplier (float): Multiplier for ATR stop loss.
            structure_lookback_bars (int): Number of bars to look back for structure stop.
            bar_range_pct (float): Percentage of bar range for bar range stop.
            take_profit_r_multiple (float): R-multiple for fixed take profit.
        """
        self.interval = interval
        self.stop_loss_method = stop_loss_method
        self.atr_period = atr_period
        self.atr_multiplier = atr_multiplier
        self.structure_lookback_bars = structure_lookback_bars
        self.bar_range_pct = bar_range_pct
        self.take_profit_r_multiple = take_profit_r_multiple
        self.ny_open_time = time(9, 30) # New York session open time

    def _calculate_stop_loss(self, df: pd.DataFrame, entry_time_idx: pd.Timestamp, 
                             entry_price: float, direction: str) -> float:
        """
        Helper to calculate stop loss based on the chosen method.
        """
        if self.stop_loss_method == 'atr':
            # Calculate ATR for the relevant period leading up to the entry
            atr_series = calculate_atr(df, period=self.atr_period)
            # Use .bfill() to get the first valid ATR if entry_time_idx is early in the series
            current_atr = atr_series.loc[:entry_time_idx].iloc[-1] if not atr_series.loc[:entry_time_idx].empty else np.nan
            if pd.isna(current_atr):
                # Fallback if ATR cannot be calculated (e.g., not enough data)
                print(f"Warning: ATR not available at {entry_time_idx}. Using a default small stop.")
                return entry_price * (0.995 if direction == 'long' else 1.005) # 0.5% stop
            return atr_stop(entry_price, current_atr, direction, self.atr_multiplier)
        
        elif self.stop_loss_method == 'structure':
            return structure_stop(df, entry_time_idx, direction, self.structure_lookback_bars)
        
        elif self.stop_loss_method == 'bar_range':
            # For bar_range_stop, we need the high/low of the reference bar.
            # The reference bar is typically Bar 2 in this strategy.
            # We need to find the index of Bar 2 relative to the entry_time_idx (Bar 3 open)
            # This assumes df is sorted by time.
            entry_loc = df.index.get_loc(entry_time_idx)
            if entry_loc >= 1: # Ensure Bar 2 exists
                bar2_idx = df.index[entry_loc - 1] # Bar 2 is the one before Bar 3 open
                bar2_high = df.loc[bar2_idx]['high']
                bar2_low = df.loc[bar2_idx]['low']
                return bar_range_stop(entry_price, bar2_high, bar2_low, direction, self.bar_range_pct)
            else:
                print(f"Warning: Not enough bars for bar_range stop at {entry_time_idx}. Using a default small stop.")
                return entry_price * (0.995 if direction == 'long' else 1.005) # 0.5% stop
        else:
            raise ValueError(f"Unknown stop loss method: {self.stop_loss_method}")

    def run_strategy(self, df_daily: pd.DataFrame) -> pd.DataFrame:
        """
        Runs the momentum strategy on a single day's OHLCV data.

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
        
        # Ensure the DataFrame is sorted by time
        df_daily = df_daily.sort_index()

        # Find the index of the 9:30 AM ET bar (Bar 1)
        try:
            # Find the first bar that starts at or after 9:30 AM ET
            ny_open_bar_idx = df_daily.index[df_daily.index.time >= self.ny_open_time][0]
            ny_open_bar_loc = df_daily.index.get_loc(ny_open_bar_idx)
        except IndexError:
            # No bars found at or after 9:30 AM ET for this day
            return pd.DataFrame(columns=['entry_time', 'direction', 'entry_price', 'sl', 'tp', 
                                         'exit_time', 'exit_price', 'outcome', 'R_multiple'])

        # We need at least 3 bars for the strategy (Bar 1, Bar 2, Bar 3 for entry)
        if ny_open_bar_loc + 2 >= len(df_daily):
            # Not enough bars after 9:30 AM for a valid entry
            return pd.DataFrame(columns=['entry_time', 'direction', 'entry_price', 'sl', 'tp', 
                                         'exit_time', 'exit_price', 'outcome', 'R_multiple'])

        bar1 = df_daily.iloc[ny_open_bar_loc]
        bar2 = df_daily.iloc[ny_open_bar_loc + 1]
        bar3 = df_daily.iloc[ny_open_bar_loc + 2] # This is the entry bar

        direction = None
        if bar1['close'] > bar1['open'] and bar2['close'] > bar2['open']:
            direction = 'long'
        elif bar1['close'] < bar1['open'] and bar2['close'] < bar2['open']:
            direction = 'short'

        if direction:
            entry_time = bar3.name # Timestamp of Bar 3
            entry_price = bar3['open'] # Enter at the open of Bar 3

            # Calculate Stop Loss
            stop_loss = self._calculate_stop_loss(df_daily, entry_time, entry_price, direction)
            
            # Calculate Take Profit (fixed R-multiple for now)
            risk_per_share = abs(entry_price - stop_loss)
            if risk_per_share == 0: # Avoid division by zero if SL is at entry
                return pd.DataFrame() # No trade if risk is zero

            if direction == 'long':
                take_profit = entry_price + (risk_per_share * self.take_profit_r_multiple)
            else: # short
                take_profit = entry_price - (risk_per_share * self.take_profit_r_multiple)

            # Simulate trade execution from Bar 3 onwards
            exit_price = np.nan
            outcome = 'N/A'
            r_multiple = np.nan
            exit_time = np.nan

            # Slice df from entry bar onwards for simulation
            simulation_df = df_daily.loc[entry_time:].iloc[1:] # Start from the bar *after* entry bar open

            for i, current_bar in simulation_df.iterrows():
                if direction == 'long':
                    # Check for stop loss hit
                    if current_bar['low'] <= stop_loss:
                        exit_price = stop_loss
                        outcome = 'Loss'
                        r_multiple = -1.0 # R-multiple for hitting stop loss
                        exit_time = current_bar.name
                        break
                    # Check for take profit hit
                    elif current_bar['high'] >= take_profit:
                        exit_price = take_profit
                        outcome = 'Win'
                        r_multiple = self.take_profit_r_multiple
                        exit_time = current_bar.name
                        break
                elif direction == 'short':
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
                pnl = (exit_price - entry_price) if direction == 'long' else (entry_price - exit_price)
                r_multiple = pnl / risk_per_share if risk_per_share != 0 else 0
                outcome = 'Partial Win/Loss (Session End)'

            trades.append({
                'entry_time': entry_time,
                'direction': direction,
                'entry_price': entry_price,
                'sl': stop_loss,
                'tp': take_profit,
                'exit_time': exit_time,
                'exit_price': exit_price,
                'outcome': outcome,
                'R_multiple': r_multiple
            })

        return pd.DataFrame(trades)

if __name__ == '__main__':
    # Example usage:
    # Create a dummy DataFrame for a single day, 5-minute interval
    import pytz

    ny_tz = pytz.timezone('America/New_York')
    
    # Generate 5-minute bars from 9:25 ET to 10:30 ET
    start_dt = ny_tz.localize(datetime(2024, 7, 29, 9, 25))
    end_dt = ny_tz.localize(datetime(2024, 7, 29, 10, 30))
    
    # Create a sequence of 5-minute intervals
    time_index = pd.date_range(start=start_dt, end=end_dt, freq='5min', tz=ny_tz)

    # --- FIX: Ensure data length matches time_index length ---
    num_bars = len(time_index)
    
    # Extend or repeat data to match num_bars (14 in this case for 9:25 to 10:30 at 5min freq)
    # Original data had 13 elements, extending to 14
    open_prices =   [100, 102, 105, 104, 106, 105, 104, 103, 105, 106, 107, 106, 105, 104]
    high_prices =   [103, 106, 107, 106, 108, 107, 106, 105, 107, 108, 109, 108, 107, 106]
    low_prices =    [99, 101, 104, 103, 105, 104, 103, 102, 104, 105, 106, 105, 104, 103]
    close_prices =  [102, 105, 104, 105, 107, 106, 105, 104, 106, 107, 108, 107, 106, 105]
    volumes =       [1000, 1200, 1100, 900, 800, 700, 600, 500, 750, 850, 950, 800, 700, 600]

    data_dict = {
        'open': open_prices[:num_bars],
        'high': high_prices[:num_bars],
        'low': low_prices[:num_bars],
        'close': close_prices[:num_bars],
        'volume': volumes[:num_bars]
    }
    df_test_day = pd.DataFrame(data_dict, index=time_index)
    # --- END FIX ---

    print("Test Day Data (5m interval):\n", df_test_day)

    # Initialize strategy with ATR stop
    momentum_strategy_atr = MomentumStrategy(
        interval='5m', 
        stop_loss_method='atr', 
        atr_period=5, # Shorter period for this small test data
        atr_multiplier=2.0,
        take_profit_r_multiple=2.0
    )
    trades_atr = momentum_strategy_atr.run_strategy(df_test_day)
    print("\nTrades with ATR Stop:\n", trades_atr)

    # Initialize strategy with Structure stop
    momentum_strategy_struct = MomentumStrategy(
        interval='5m', 
        stop_loss_method='structure', 
        structure_lookback_bars=2,
        take_profit_r_multiple=2.0
    )
    trades_struct = momentum_strategy_struct.run_strategy(df_test_day)
    print("\nTrades with Structure Stop:\n", trades_struct)

    # Initialize strategy with Bar Range stop
    momentum_strategy_bar_range = MomentumStrategy(
        interval='5m', 
        stop_loss_method='bar_range', 
        bar_range_pct=0.75,
        take_profit_r_multiple=2.0
    )
    trades_bar_range = momentum_strategy_bar_range.run_strategy(df_test_day)
    print("\nTrades with Bar Range Stop:\n", trades_bar_range)
