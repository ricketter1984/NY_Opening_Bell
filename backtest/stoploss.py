import pandas as pd
import numpy as np

def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Calculates the Average True Range (ATR) for a given DataFrame.

    Args:
        df (pd.DataFrame): DataFrame with 'high', 'low', 'close' columns.
        period (int): The lookback period for ATR calculation.

    Returns:
        pd.Series: A Series containing the ATR values.
    """
    if not all(col in df.columns for col in ['high', 'low', 'close']):
        raise ValueError("DataFrame must contain 'high', 'low', and 'close' columns for ATR calculation.")

    # Calculate True Range (TR)
    # TR = max[(High - Low), abs(High - PrevClose), abs(Low - PrevClose)]
    high_low = df['high'] - df['low']
    high_prev_close = np.abs(df['high'] - df['close'].shift(1))
    low_prev_close = np.abs(df['low'] - df['close'].shift(1))

    true_range = pd.DataFrame({'hl': high_low, 'hpc': high_prev_close, 'lpc': low_prev_close}).max(axis=1)

    # Calculate ATR using Exponential Moving Average (EMA) or Simple Moving Average (SMA)
    # EMA is common for ATR, but SMA is simpler for initial backtesting and often used.
    # For simplicity and common use in backtesting, we'll use a rolling mean (SMA-like)
    # If you prefer EMA, you'd use .ewm(span=period, adjust=False).mean()
    atr = true_range.rolling(window=period).mean()
    
    return atr

def atr_stop(entry_price: float, atr_value: float, direction: str, multiplier: float = 2.0) -> float:
    """
    Calculates a stop-loss price based on ATR.

    Args:
        entry_price (float): The price at which the trade was entered.
        atr_value (float): The ATR value at the time of entry.
        direction (str): 'long' or 'short'.
        multiplier (float): Multiplier for ATR to determine stop distance.

    Returns:
        float: The calculated stop-loss price.
    """
    if direction == 'long':
        return entry_price - (atr_value * multiplier)
    elif direction == 'short':
        return entry_price + (atr_value * multiplier)
    else:
        raise ValueError("Direction must be 'long' or 'short'.")

def structure_stop(df: pd.DataFrame, entry_time_idx: pd.Timestamp, direction: str, lookback_bars: int = 3) -> float:
    """
    Calculates a stop-loss price based on recent price structure (e.g., lowest low for long, highest high for short).

    Args:
        df (pd.DataFrame): DataFrame with 'high', 'low' columns.
        entry_time_idx (pd.Timestamp): The timestamp of the entry bar.
        direction (str): 'long' or 'short'.
        lookback_bars (int): Number of bars to look back from the entry bar to find structure.

    Returns:
        float: The calculated stop-loss price.
    """
    # Find the index of the entry bar
    entry_loc = df.index.get_loc(entry_time_idx)

    # Get the relevant lookback period (excluding the entry bar itself for structure)
    start_lookback_loc = max(0, entry_loc - lookback_bars)
    lookback_df = df.iloc[start_lookback_loc:entry_loc] # Bars *before* entry

    if lookback_df.empty:
        # Fallback if not enough bars for lookback (e.g., very start of data)
        print(f"Warning: Not enough lookback bars for structure stop at {entry_time_idx}. Using a default small stop.")
        if direction == 'long':
            return df.loc[entry_time_idx]['open'] * 0.99 # 1% below open as fallback
        else:
            return df.loc[entry_time_idx]['open'] * 1.01 # 1% above open as fallback


    if direction == 'long':
        # For long, stop below the lowest low in the lookback period
        return lookback_df['low'].min()
    elif direction == 'short':
        # For short, stop above the highest high in the lookback period
        return lookback_df['high'].max()
    else:
        raise ValueError("Direction must be 'long' or 'short'.")

def bar_range_stop(entry_price: float, bar_high: float, bar_low: float, direction: str, pct: float = 0.5) -> float:
    """
    Calculates a stop-loss price based on a percentage of a reference bar's range.
    The reference bar is typically the entry bar or a preceding signal bar.

    Args:
        entry_price (float): The price at which the trade was entered.
        bar_high (float): The high of the reference bar.
        bar_low (float): The low of the reference bar.
        direction (str): 'long' or 'short'.
        pct (float): Percentage of the bar's range to use for stop distance (e.g., 0.5 for 50%).

    Returns:
        float: The calculated stop-loss price.
    """
    bar_range = bar_high - bar_low
    stop_distance = bar_range * pct

    if direction == 'long':
        return entry_price - stop_distance
    elif direction == 'short':
        return entry_price + stop_distance
    else:
        raise ValueError("Direction must be 'long' or 'short'.")

if __name__ == '__main__':
    # Example Usage for stoploss.py
    data = {
        'open': [100, 102, 105, 103, 106, 108],
        'high': [103, 106, 108, 105, 109, 110],
        'low': [99, 101, 104, 102, 105, 107],
        'close': [102, 105, 103, 104, 108, 109]
    }
    df = pd.DataFrame(data, index=pd.to_datetime(['2024-07-29 09:30', '2024-07-29 09:31', '2024-07-29 09:32',
                                                '2024-07-29 09:33', '2024-07-29 09:34', '2024-07-29 09:35']))

    # Test ATR calculation
    atr_values = calculate_atr(df, period=3)
    print("ATR Values:\n", atr_values)
    
    # Test ATR stop
    entry_price_atr = 105.5
    current_atr = atr_values.iloc[-1] # Use the latest ATR for example
    stop_long_atr = atr_stop(entry_price_atr, current_atr, 'long', multiplier=2.0)
    stop_short_atr = atr_stop(entry_price_atr, current_atr, 'short', multiplier=2.0)
    print(f"\nATR Stop (Long, Entry {entry_price_atr}, ATR {current_atr:.2f}): {stop_long_atr:.2f}")
    print(f"ATR Stop (Short, Entry {entry_price_atr}, ATR {current_atr:.2f}): {stop_short_atr:.2f}")

    # Test Structure stop
    entry_time_idx_struct = pd.to_datetime('2024-07-29 09:34')
    entry_price_struct = df.loc[entry_time_idx_struct]['open']
    stop_long_struct = structure_stop(df, entry_time_idx_struct, 'long', lookback_bars=2)
    stop_short_struct = structure_stop(df, entry_time_idx_struct, 'short', lookback_bars=2)
    print(f"\nStructure Stop (Long, Entry {entry_price_struct}): {stop_long_struct:.2f}")
    print(f"Structure Stop (Short, Entry {entry_price_struct}): {stop_short_struct:.2f}")

    # Test Bar Range stop
    entry_price_bar_range = 104.0
    ref_bar_high = 108.0 # Example reference bar (e.g., Bar 2 high)
    ref_bar_low = 104.0 # Example reference bar (e.g., Bar 2 low)
    stop_long_bar_range = bar_range_stop(entry_price_bar_range, ref_bar_high, ref_bar_low, 'long', pct=0.75)
    stop_short_bar_range = bar_range_stop(entry_price_bar_range, ref_bar_high, ref_bar_low, 'short', pct=0.75)
    print(f"\nBar Range Stop (Long, Entry {entry_price_bar_range}): {stop_long_bar_range:.2f}")
    print(f"Bar Range Stop (Short, Entry {entry_price_bar_range}): {stop_short_bar_range:.2f}")
