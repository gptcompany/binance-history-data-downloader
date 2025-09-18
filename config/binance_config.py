# Binance Data Download Configuration  
from nautilus_trader.adapters.binance.config import BinanceDataClientConfig  
from nautilus_trader.config import TradingNodeConfig  
  
# Binance Data Client - Read-only, no API keys needed for public data  
binance_config = BinanceDataClientConfig(  
    api_key=None,  # Not needed for public market data  
    api_secret=None,  # Not needed for public market data  
    base_url_http=None,  # Uses default Binance API  
    base_url_ws=None,  # Uses default Binance WebSocket  
    us=False,  # Use Binance.com  
)  
