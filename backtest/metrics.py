import pandas as pd
import numpy as np

class PerformanceMetrics:
    """
    Calculates and provides key performance metrics for trading strategy backtests.
    """

    def __init__(self):
        pass

    def calculate_metrics(self, trades_df: pd.DataFrame) -> dict:
        """
        Calculates a set of common performance metrics from a DataFrame of trades.

        Args:
            trades_df (pd.DataFrame): DataFrame containing trade results with at least
                                      'R_multiple' and 'outcome' columns.

        Returns:
            dict: A dictionary containing various performance metrics.
        """
        if trades_df.empty:
            return {
                'total_trades': 0,
                'winning_trades': 0,
                'losing_trades': 0,
                'win_rate': 0.0,
                'avg_r_multiple': 0.0,
                'total_r_multiple': 0.0,
                'profit_factor': 0.0,
                'max_drawdown': 0.0,
                'max_drawdown_pct': 0.0,
                'net_profit_points': 0.0, # Assuming R_multiple is based on points/ticks for now
                'avg_profit_per_trade': 0.0,
                'avg_loss_per_trade': 0.0,
            }

        total_trades = len(trades_df)
        winning_trades = trades_df[trades_df['outcome'] == 'Win']
        losing_trades = trades_df[trades_df['outcome'] == 'Loss']
        
        num_winning_trades = len(winning_trades)
        num_losing_trades = len(losing_trades)
        
        win_rate = (num_winning_trades / total_trades) * 100 if total_trades > 0 else 0.0

        # R-multiple calculations
        total_r_multiple = trades_df['R_multiple'].sum()
        avg_r_multiple = trades_df['R_multiple'].mean()

        # Profit Factor
        # Sum of R-multiples for winning trades
        gross_profit_r = winning_trades['R_multiple'].sum()
        # Sum of absolute R-multiples for losing trades
        gross_loss_r = abs(losing_trades['R_multiple'].sum())
        
        profit_factor = gross_profit_r / gross_loss_r if gross_loss_r > 0 else np.inf # Avoid division by zero

        # Equity Curve and Max Drawdown
        # Assuming each R_multiple represents a unit of risk/reward
        equity_curve = trades_df['R_multiple'].cumsum()
        max_equity = equity_curve.cummax()
        drawdown = max_equity - equity_curve
        max_drawdown = drawdown.max() if not drawdown.empty else 0.0
        # Percentage drawdown relative to peak equity
        max_drawdown_pct = (max_drawdown / max_equity.max()) * 100 if max_equity.max() > 0 else 0.0


        # Net Profit in points/ticks (assuming R_multiple is based on 1 unit of risk)
        # This assumes R_multiple is already normalized by the initial risk.
        # If R_multiple is directly PnL in points, then total_r_multiple is net_profit_points
        net_profit_points = total_r_multiple # This is simplified, actual PnL depends on position sizing

        # Average profit/loss per trade (in R-multiples)
        avg_profit_per_trade = winning_trades['R_multiple'].mean() if num_winning_trades > 0 else 0.0
        avg_loss_per_trade = losing_trades['R_multiple'].mean() if num_losing_trades > 0 else 0.0


        metrics = {
            'total_trades': total_trades,
            'winning_trades': num_winning_trades,
            'losing_trades': num_losing_trades,
            'win_rate': round(win_rate, 2),
            'avg_r_multiple': round(avg_r_multiple, 2),
            'total_r_multiple': round(total_r_multiple, 2),
            'profit_factor': round(profit_factor, 2),
            'max_drawdown': round(max_drawdown, 2),
            'max_drawdown_pct': round(max_drawdown_pct, 2),
            'net_profit_points': round(net_profit_points, 2),
            'avg_profit_per_trade': round(avg_profit_per_trade, 2),
            'avg_loss_per_trade': round(avg_loss_per_trade, 2),
        }
        
        return metrics

    def compare_strategies(self, momentum_trades_df: pd.DataFrame, reversal_trades_df: pd.DataFrame) -> dict:
        """
        Compares the success rates of the momentum and reversal strategies.

        Args:
            momentum_trades_df (pd.DataFrame): DataFrame of trades from the momentum strategy.
            reversal_trades_df (pd.DataFrame): DataFrame of trades from the reversal strategy.

        Returns:
            dict: A dictionary with comparison metrics.
        """
        momentum_metrics = self.calculate_metrics(momentum_trades_df)
        reversal_metrics = self.calculate_metrics(reversal_trades_df)

        comparison = {
            'momentum_strategy': {
                'total_trades': momentum_metrics['total_trades'],
                'win_rate': momentum_metrics['win_rate'],
                'avg_r_multiple': momentum_metrics['avg_r_multiple'],
                'profit_factor': momentum_metrics['profit_factor'],
                'total_r_multiple': momentum_metrics['total_r_multiple']
            },
            'reversal_strategy': {
                'total_trades': reversal_metrics['total_trades'],
                'win_rate': reversal_metrics['win_rate'],
                'avg_r_multiple': reversal_metrics['avg_r_multiple'],
                'profit_factor': reversal_metrics['profit_factor'],
                'total_r_multiple': reversal_metrics['total_r_multiple']
            }
        }
        return comparison

if __name__ == '__main__':
    # Example Usage for metrics.py

    # Sample Momentum Trades DataFrame
    momentum_data = {
        'entry_time': pd.to_datetime(['2024-07-01 09:40', '2024-07-02 09:40', '2024-07-03 09:40']),
        'direction': ['long', 'short', 'long'],
        'entry_price': [100, 105, 110],
        'sl': [98, 107, 108],
        'tp': [103, 102, 113],
        'exit_time': pd.to_datetime(['2024-07-01 09:45', '2024-07-02 09:42', '2024-07-03 10:00']),
        'exit_price': [103, 107, 108],
        'outcome': ['Win', 'Loss', 'Loss'],
        'R_multiple': [1.5, -1.0, -1.0]
    }
    momentum_trades_df = pd.DataFrame(momentum_data)
    print("Sample Momentum Trades:\n", momentum_trades_df)

    # Sample Reversal Trades DataFrame
    reversal_data = {
        'entry_time': pd.to_datetime(['2024-07-01 09:50', '2024-07-02 09:55', '2024-07-03 09:45']),
        'direction': ['short', 'long', 'short'],
        'entry_price': [108, 102, 112],
        'sl': [110, 100, 114],
        'tp': [105, 105, 109],
        'exit_time': pd.to_datetime(['2024-07-01 10:00', '2024-07-02 10:05', '2024-07-03 09:50']),
        'exit_price': [105, 105, 114],
        'outcome': ['Win', 'Win', 'Loss'],
        'R_multiple': [1.5, 1.5, -1.0]
    }
    reversal_trades_df = pd.DataFrame(reversal_data)
    print("\nSample Reversal Trades:\n", reversal_trades_df)

    metrics_calculator = PerformanceMetrics()

    # Calculate metrics for Momentum Strategy
    momentum_results = metrics_calculator.calculate_metrics(momentum_trades_df)
    print("\n--- Momentum Strategy Metrics ---")
    for key, value in momentum_results.items():
        print(f"{key}: {value}")

    # Calculate metrics for Reversal Strategy
    reversal_results = metrics_calculator.calculate_metrics(reversal_trades_df)
    print("\n--- Reversal Strategy Metrics ---")
    for key, value in reversal_results.items():
        print(f"{key}: {value}")

    # Compare both strategies
    comparison_results = metrics_calculator.compare_strategies(momentum_trades_df, reversal_trades_df)
    print("\n--- Strategy Comparison ---")
    print("Momentum:", comparison_results['momentum_strategy'])
    print("Reversal:", comparison_results['reversal_strategy'])

    # Test with empty DataFrame
    empty_df = pd.DataFrame(columns=['entry_time', 'direction', 'entry_price', 'sl', 'tp', 
                                     'exit_time', 'exit_price', 'outcome', 'R_multiple'])
    empty_metrics = metrics_calculator.calculate_metrics(empty_df)
    print("\n--- Metrics for Empty DataFrame ---")
    print(empty_metrics)
