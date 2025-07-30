# NY_Opening_Bell 🛎️

**Backtest engine for futures strategies at the New York session open (9:30 AM ET).**  
Built for high-frequency datasets (1s–1m) from [Databento](https://databento.com/), this system supports robust testing of:

- 📈 **Momentum Breakouts**: entry in direction of first two bars post-open
- 🔁 **Reversal After Fake Move**: contrarian setups following failed sweeps
- ⏱️ Multi-timeframe analysis (1m, 2m, 3m, 5m, 10m, 15m)
- 💾 CSV and Jupyter-based deep analysis of trade logs
- ⚙️ Modular strategy files, configurable ATR-based stop logic

### 📦 Instruments Supported
- 🧠 Index Futures: `MYM`, `MES`, `MNQ`, `M2K`
- 🪙 Metals: `MGC`, `SIL`
- ⚡ Energy: `MCL`, `MNG`
- 💱 Currency Futures: `6E`, `6J`, `6B`, etc.

### 🚀 Quickstart
```bash
# 1. Activate virtualenv & install
pip install -r requirements.txt

# 2. Set API key in .env
DATABENTO_API_KEY="your_key_here"

# 3. Download OHLCV
python download_ohlcv_single.py MYMM5 2025-05-01 2025-05-02

# 4. Run backtest
python backtest/ny_open_breakout.py
