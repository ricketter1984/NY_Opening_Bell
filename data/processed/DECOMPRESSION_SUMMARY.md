# Databento File Decompression Summary

## Successfully Processed File
- **Source**: `data/raw/glbx-mdp3-20250430-20250729.ohlcv-1s.csv.zst`
- **Output**: `data/processed/glbx-mdp3-20250430-20250729.ohlcv-1s.csv`

## File Statistics
- **Compressed Size**: 12.85 MB
- **Decompressed Size**: 163.08 MB (in memory)
- **Final CSV Size**: 119.90 MB
- **Compression Ratio**: ~9.3x reduction

## Data Overview
- **Total Records**: 1,460,425 rows
- **Columns**: 10
- **Memory Usage**: ~274 MB
- **Date Range**: 2025-04-30 to 2025-07-29
- **Timeframe**: 1-second OHLCV data

## Column Structure
1. `ts_event` - Timestamp (ISO format with nanoseconds)
2. `rtype` - Record type (32 = OHLCV)
3. `publisher_id` - Data publisher ID (1)
4. `instrument_id` - Unique instrument identifier (42003054)
5. `open` - Opening price
6. `high` - High price
7. `low` - Low price  
8. `close` - Closing price
9. `volume` - Trading volume
10. `symbol` - Trading symbol (MYMM5)

## Sample Data
```csv
ts_event,rtype,publisher_id,instrument_id,open,high,low,close,volume,symbol
2025-04-30T00:00:00.000000000Z,32,1,42003054,40653.0,40653.0,40647.0,40647.0,6,MYMM5
2025-04-30T00:00:01.000000000Z,32,1,42003054,40646.0,40647.0,40644.0,40644.0,6,MYMM5
```

## Data Quality
- ✅ No missing values in any column
- ✅ Proper data types detected
- ✅ Consistent timestamp format
- ✅ Complete OHLCV data structure

## Usage Notes
- The data appears to be for the MYMM5 symbol (possibly a futures contract)
- Timestamps are in UTC with nanosecond precision
- Data includes 1-second interval OHLCV bars
- Ready for analysis with pandas or other data analysis tools

## Next Steps
This decompressed data is now ready for:
- Time series analysis
- Trading strategy backtesting
- Market microstructure analysis
- Integration with your NY Opening Bell project
