#!/usr/bin/env python3
import os
import sys
import json
import logging
import argparse
import time  # Added for retries and rate limiting
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from tqdm import tqdm  # Progress bar
import venv_utils  # Local utils
from pathlib import Path

# Import constants from binance_downloader
from binance_downloader import (
    BASE_URL,
    DEFAULT_SYMBOLS,
    DEFAULT_DATA_TYPES_DAILY,
    DEFAULT_DATA_TYPES_MONTHLY,
    DEFAULT_START_DATE_STR
)

# Configure logging
logger = logging.getLogger(__name__)
# Basic config setup here, will be reconfigured in main based on verbosity
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants for retries
RETRY_COUNT = 3
RETRY_BACKOFF_FACTOR = 0.5
RETRY_STATUS_FORCELIST = (500, 502, 503, 504)

# Rate limiting delay (can be adjusted)
RATE_LIMIT_DELAY = 0.2  # seconds

def setup_session_with_retries() -> requests.Session:
   """Creates a requests session with retry logic."""
   session = requests.Session()
   retry_strategy = Retry(
       total=RETRY_COUNT,
       backoff_factor=RETRY_BACKOFF_FACTOR,
       status_forcelist=RETRY_STATUS_FORCELIST,
       allowed_methods=["HEAD", "GET"] # Add GET if needed later
   )
   adapter = HTTPAdapter(max_retries=retry_strategy)
   session.mount("http://", adapter)
   session.mount("https://", adapter)
   return session

def discover_earliest_date(
   symbol: str,
   data_type: str,
   start_guess_date: date,
   interval: Optional[str] = None,
   session: Optional[requests.Session] = None
) -> date:
   """
   Find earliest available date for a symbol/type/interval combination using binary search.

   Add logging for:
   - Initial search range
   - Each binary search step (mid point being tested)
   - API responses (success/failure, status code)
   - Retry attempts
   - Final result and confidence
    Use binary search to speed up the process.
    """
   log_prefix = f"[{symbol} {data_type} {interval or ''}]"
   logger.info(f"{log_prefix} Starting discovery...")

   if session is None:
       session = setup_session_with_retries()

   today = date.today()
   # Use the provided start_guess_date
   left, right = start_guess_date, today
   earliest_found = None # Initialize to None to detect edge cases

   logger.debug(f"{log_prefix} Initial search range: {left.strftime('%Y-%m-%d')} to {right.strftime('%Y-%m-%d')}")

   url_pattern_base = f"{BASE_URL}/daily/{data_type}/{symbol}"
   if interval:
       url_pattern_base = f"{url_pattern_base}/{interval}"

   # Check boundaries first to handle edge cases
   logger.debug(f"{log_prefix} Checking initial left boundary: {left.strftime('%Y-%m-%d')}")
   if check_date_exists(left, symbol, data_type, interval, url_pattern_base, session, log_prefix):
       logger.info(f"{log_prefix} Data found at the earliest possible date: {left.strftime('%Y-%m-%d')}")
       return left # Earliest possible date works

   logger.debug(f"{log_prefix} Checking initial right boundary: {right.strftime('%Y-%m-%d')}")
   if not check_date_exists(right, symbol, data_type, interval, url_pattern_base, session, log_prefix):
        logger.warning(f"{log_prefix} No data found even for today ({right.strftime('%Y-%m-%d')}). Returning None.")
        return None # No data found at all in the range

   # Perform binary search
   while left <= right:
       mid_date = left + (right - left) // 2
       logger.debug(f"{log_prefix} Testing mid date: {mid_date.strftime('%Y-%m-%d')} (Range: {left.strftime('%Y-%m-%d')} - {right.strftime('%Y-%m-%d')})")

       if check_date_exists(mid_date, symbol, data_type, interval, url_pattern_base, session, log_prefix):
           # Data exists, record this as potential earliest and look earlier
           earliest_found = mid_date
           right = mid_date - timedelta(days=1)
           logger.debug(f"{log_prefix} Data found for {mid_date.strftime('%Y-%m-%d')}. New range: {left.strftime('%Y-%m-%d')} - {right.strftime('%Y-%m-%d')}")
       else:
           # Data doesn't exist, look later
           left = mid_date + timedelta(days=1)
           logger.debug(f"{log_prefix} Data NOT found for {mid_date.strftime('%Y-%m-%d')}. New range: {left.strftime('%Y-%m-%d')} - {right.strftime('%Y-%m-%d')}")

       # Rate limiting
       time.sleep(RATE_LIMIT_DELAY)

   if earliest_found:
       logger.info(f"{log_prefix} Discovery complete. Earliest date found: {earliest_found.strftime('%Y-%m-%d')}")
   else:
       # This case should ideally be caught by boundary checks, but added for safety
       logger.warning(f"{log_prefix} Discovery complete, but no date found within the range {start_guess_date.strftime('%Y-%m-%d')} - {today.strftime('%Y-%m-%d')}.")

   return earliest_found


def check_date_exists(
   check_date: date,
   symbol: str,
   data_type: str,
   interval: Optional[str],
   url_pattern_base: str,
   session: requests.Session,
   log_prefix: str
) -> bool:
   """Checks if data exists for a specific date via HEAD request, handles retries."""
   date_str = check_date.strftime("%Y-%m-%d")
   if interval:
       filename = f"{symbol}-{interval}-{date_str}.zip"
   else:
       filename = f"{symbol}-{data_type}-{date_str}.zip"
   url = f"{url_pattern_base}/{filename}"

   try:
       response = session.head(url, timeout=15) # Increased timeout slightly
       logger.debug(f"{log_prefix} HEAD {url} - Status: {response.status_code}")

       if response.status_code == 200:
           return True
       elif response.status_code == 404:
           return False
       else:
           # Log unexpected status codes
           logger.warning(f"{log_prefix} Unexpected status code {response.status_code} for {url}")
           # Consider non-404 errors as potentially transient? For binary search, safer to assume non-existence.
           return False
   except requests.exceptions.RetryError as e:
        logger.error(f"{log_prefix} Max retries exceeded for {url}: {e}")
        return False # Treat max retries failure as non-existent for search logic
   except requests.RequestException as e:
       logger.error(f"{log_prefix} Request failed for {url}: {e}")
       # Treat other request exceptions as non-existent for search logic
       return False

def save_discovered_ranges(ranges: Dict, output_path: str) -> None:
    """
    Save discovered date ranges to a JSON file.
    Format: {
        "BTCUSDT": {
            "klines": {"1m": "2019-09-01", ...},
            "trades": "2019-09-01",
            ...
        },
        ...
    }
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(ranges, f, indent=2, sort_keys=True)
    
    logger.info(f"Saved date ranges to {output_path}")

def discover_all_ranges(symbols: List[str], data_types: List[str], start_guess_date: date) -> Dict:
   """
   Discover ranges for all combinations.
   Show progress bar using tqdm if available.
   Uses a single session for efficiency.
   """
   ranges = {}
   total_combinations = 0
   combinations_list = []

   # Pre-calculate combinations for accurate progress bar
   for symbol in symbols:
       for data_type in data_types:
           if data_type in ["klines", "indexPriceKlines", "markPriceKlines", "premiumIndexKlines"]:
               for interval in ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]:
                    total_combinations += 1
                    combinations_list.append((symbol, data_type, interval))
           else:
               total_combinations += 1
               combinations_list.append((symbol, data_type, None))

   session = setup_session_with_retries() # Use one session for all requests

   with tqdm(total=total_combinations, desc="Discovering data ranges") as pbar:
       for symbol, data_type, interval in combinations_list:
           if symbol not in ranges:
               ranges[symbol] = {}

           earliest = discover_earliest_date(symbol, data_type, start_guess_date, interval, session)

           earliest_str = earliest.strftime("%Y-%m-%d") if earliest else None

           if interval:
               if data_type not in ranges[symbol]:
                   ranges[symbol][data_type] = {}
               ranges[symbol][data_type][interval] = earliest_str
               pbar.set_postfix_str(f"{symbol} {data_type} {interval} -> {earliest_str}", refresh=True)
           else:
               ranges[symbol][data_type] = earliest_str
               pbar.set_postfix_str(f"{symbol} {data_type} -> {earliest_str}", refresh=True)

           pbar.update(1)

   return ranges

def main():
    parser = argparse.ArgumentParser(description="Discover earliest available dates for Binance Futures data")
    parser.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS,
                      help="Symbols to check (default: from binance_downloader)")
    parser.add_argument("--data-types", nargs="+", 
                      default=DEFAULT_DATA_TYPES_DAILY + DEFAULT_DATA_TYPES_MONTHLY,
                      help="Data types to check (default: all types)")
    parser.add_argument("--output", default="data_ranges.json",
                      help="Path to save JSON output (default: data_ranges.json)")
   parser.add_argument("--start-guess", default="2015-01-01", # Changed default start guess
                     help="Earliest date to start searching from (YYYY-MM-DD format, default: 2015-01-01)")
   parser.add_argument("--verbose", "-v", action="store_true",
                     help="Enable detailed debug logging")

   args = parser.parse_args()

   # Reconfigure logging based on verbosity
   log_level = logging.DEBUG if args.verbose else logging.INFO
   # Remove existing handlers if any before reconfiguring
   for handler in logging.root.handlers[:]:
       logging.root.removeHandler(handler)
   logging.basicConfig(
       level=log_level,
       format='%(asctime)s - %(levelname)s - %(name)s - %(message)s' # Added logger name
   )

   logger.info(f"Starting data range discovery with arguments: {args}")
   logger.debug(f"Verbose logging enabled.")

   # Verify virtual environment
   if not venv_utils.is_venv_active():
       logger.warning("No virtual environment detected. It's recommended to run this script in a venv")

   try:
       start_guess_date = datetime.strptime(args.start_guess, "%Y-%m-%d").date()
   except ValueError:
       logger.error(f"Invalid start-guess date format: {args.start_guess}. Please use YYYY-MM-DD.")
       sys.exit(1)

   ranges = discover_all_ranges(args.symbols, args.data_types, start_guess_date)
   save_discovered_ranges(ranges, args.output)

if __name__ == "__main__":
    main()