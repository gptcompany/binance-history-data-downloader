# Binance Clean Downloader Documentation  
  
## Overview  
The Clean Downloader is a streamlined solution for downloading cryptocurrency market data from Binance and converting it to clean CSV format compatible with Nautilus Trader and other trading systems.  
  
## Features Implemented  
  
### Core Functionality  
- **Direct API Access**: Uses Binance public REST API (no authentication required)  
- **OHLCV Data**: Downloads Open, High, Low, Close, Volume data  
- **Clean CSV Export**: Properly formatted CSV with standard headers  
- **Timestamp Processing**: Converts Unix timestamps to readable datetime format  
- **Multi-Symbol Support**: Can download data for any Binance trading pair  
- **Multi-Timeframe Support**: Supports all Binance intervals (1m, 5m, 15m, 1h, 4h, 1d, etc.)  
  
### Technical Implementation  
ECHO is on.
### Supported Parameters  
  
**Symbols**: Any Binance trading pair (BTCUSDT, ETHUSDT, etc.)  
**Intervals**: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M  
**Limit**: 1-1000 candles per request (API limitation)  
  
### File Output  
Files are saved in the data/ directory with naming convention:  
SYMBOL_INTERVAL_clean.csv  
  
Example: BTCUSDT_1h_clean.csv  
  
### Integration with Nautilus Trader  
CSV files can be imported into Nautilus Trader using:  
- ParquetDataCatalog for backtesting  
- Direct CSV import for analysis  
- Custom data providers for live trading  
