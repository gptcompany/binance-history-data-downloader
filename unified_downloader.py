#!/usr/bin/env python3
import os
import sys
import json
import logging
import argparse
import time
import csv
import hashlib
import zipfile
import asyncio
import aiohttp
import ssl
# import requests # No longer needed
# from requests.adapters import HTTPAdapter # No longer needed
# from requests.packages.urllib3.util.retry import Retry # No longer needed
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from tqdm import tqdm # Optional progress bar

# --- Configuration ---
BASE_URL = "https://data.binance.vision/data/futures/um"
DEFAULT_SYMBOLS = ["BTCUSDT", "BTCUSDC"]
DEFAULT_START_DATE_STR = "2020-01-01"
CONFIG_FILE = Path("data_availability.json")
DOWNLOADS_DIR = Path("downloads")
EXTRACTED_DATA_DIR = Path("data")
REPORTS_DIR = Path("reports")
LOGS_DIR = Path("logs")

# Data types configuration (can be expanded)
# Define types that use intervals globally
INTERVAL_TYPES = ["klines", "indexPriceKlines", "markPriceKlines", "premiumIndexKlines"]
DEFAULT_KLINES_INTERVAL = "1m" # Default interval for klines types
ALL_DATA_TYPES = [
    "klines", "bookDepth", "metrics", "trades", "aggTrades",
    "indexPriceKlines", "markPriceKlines", "premiumIndexKlines", "fundingRate"
]
DAILY_TYPES = [dt for dt in ALL_DATA_TYPES if dt != "fundingRate"]
MONTHLY_TYPES = ["fundingRate"]

# Download/Retry Settings
MAX_CONCURRENT_DOWNLOADS = 5
MAX_DOWNLOAD_RETRIES = 2 # Number of redownload attempts *after* initial check fails
DOWNLOAD_RETRY_DELAY = 5 # seconds between download retries
CHECKSUM_VERIFICATION = True # Verify checksum after download
CLEAN_UP_ZIPS = True # Delete ZIP after successful extraction
VERIFY_EXTRACTED_FILES = True # Check if extracted CSV is valid/non-empty

# Discovery Settings
DISCOVERY_RETRY_COUNT = 3
DISCOVERY_BACKOFF_FACTOR = 0.5
DISCOVERY_STATUS_FORCELIST = (500, 502, 503, 504)
DISCOVERY_RATE_LIMIT_DELAY = 0.2 # seconds between discovery checks
DISCOVERY_START_GUESS_DATE_STR = "2017-01-01" # A reasonable early guess for discovery

# --- Logging Setup ---
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
LOGS_DIR.mkdir(parents=True, exist_ok=True)
log_filename = LOGS_DIR / f"unified_downloader_{timestamp}.log"
log_level = logging.INFO
log_format = '%(asctime)s - %(levelname)s - [%(name)s] - %(message)s'

logger = logging.getLogger("downloader") # Specific logger instance
logger.setLevel(log_level)
# Prevent adding handlers multiple times if re-run
if not logger.handlers:
    # File Handler
    try:
        file_handler = logging.FileHandler(log_filename, mode='w', encoding='utf-8')
        file_handler.setLevel(log_level)
        file_handler.setFormatter(logging.Formatter(log_format))
        logger.addHandler(file_handler)
    except Exception as e:
        print(f"Warning: Could not create log file handler for {log_filename}: {e}. Logging to console only.")

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(console_handler)

logger.info(f"Logging initialized. Log file: {log_filename}")

# --- Helper Functions ---

def create_ssl_context():
    """Create SSL context with permissive settings for Binance API"""
    ssl_context = ssl.create_default_context()
    # More permissive SSL settings to handle certificate issues
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    # Enable legacy protocols if needed
    ssl_context.set_ciphers('DEFAULT')
    return ssl_context

def create_aiohttp_connector():
    """Create aiohttp connector with SSL settings"""
    ssl_context = create_ssl_context()
    return aiohttp.TCPConnector(
        ssl=ssl_context,
        limit=100,  # Connection pool limit
        limit_per_host=20,  # Connections per host
        keepalive_timeout=30,  # Keep connections alive
        enable_cleanup_closed=True
    )

# --- Discovery Functions ---

async def check_date_exists(
   check_date: date,
   symbol: str,
   data_type: str,
   interval: Optional[str],
   url_pattern_base: str,
   session: aiohttp.ClientSession, # Use aiohttp session
   log_prefix: str
) -> bool:
    """Checks if data exists for a specific date via HEAD request using aiohttp."""
    date_str = check_date.strftime("%Y-%m-%d")
    # Determine filename based on type and interval
    if data_type == "fundingRate": # Monthly
         # Funding rate is monthly, filename uses YYYY-MM
         month_str = check_date.strftime("%Y-%m")
         filename = f"{symbol}-{data_type}-{month_str}.zip"
         # Adjust URL base for monthly data
         url = f"{BASE_URL}/monthly/{data_type}/{symbol}/{filename}"
    elif interval: # Daily interval types
        filename = f"{symbol}-{interval}-{date_str}.zip"
        url = f"{url_pattern_base}/{filename}"
    else: # Daily non-interval types
        filename = f"{symbol}-{data_type}-{date_str}.zip"
        url = f"{url_pattern_base}/{filename}"


    retries = 0
    while retries < DISCOVERY_RETRY_COUNT:
        try:
            # Use HEAD request with timeout
            async with session.head(url, timeout=15) as response:
                logger.debug(f"{log_prefix} HEAD {url} - Status: {response.status}")

                if response.status == 200:
                    return True
                elif response.status == 404:
                    return False # Data definitely doesn't exist for this date
                else:
                    # Log unexpected status codes but treat as non-existent for search
                    logger.warning(f"{log_prefix} Unexpected status code {response.status} for {url}")
                    return False
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            retries += 1
            logger.warning(f"{log_prefix} Discovery HEAD request attempt {retries}/{DISCOVERY_RETRY_COUNT} failed for {url}: {type(e).__name__}. Retrying in {DISCOVERY_BACKOFF_FACTOR}s...")
            if retries >= DISCOVERY_RETRY_COUNT:
                logger.error(f"{log_prefix} Max retries exceeded for discovery HEAD request {url}. Assuming non-existent.")
                return False
            await asyncio.sleep(DISCOVERY_BACKOFF_FACTOR * (2 ** (retries - 1))) # Exponential backoff
        except Exception as e:
            logger.error(f"{log_prefix} Unexpected error during discovery HEAD request for {url}: {e}")
            return False # Treat other errors as non-existent for safety
    return False # Should not be reached if retries are handled correctly


async def discover_earliest_date(
    symbol: str,
    data_type: str,
    interval: Optional[str],
    start_guess_date: date,
    session: aiohttp.ClientSession # Use aiohttp session
) -> Optional[date]:
    """
    Find earliest available date for a symbol/type/interval combination using binary search.
    Handles both daily and monthly (fundingRate) data types.
    """
    log_prefix = f"Discovery [{symbol} {data_type} {interval or ''}]"
    logger.info(f"{log_prefix} Starting discovery...")

    today = date.today()
    # Use the provided start_guess_date
    left, right = start_guess_date, today
    earliest_found = None

    is_monthly = data_type == "fundingRate"
    if is_monthly:
        # Adjust range for monthly checks (first day of month)
        left = left.replace(day=1)
        right = right.replace(day=1)
        logger.debug(f"{log_prefix} Initial monthly search range: {left.strftime('%Y-%m')} to {right.strftime('%Y-%m')}")
    else:
        logger.debug(f"{log_prefix} Initial daily search range: {left.strftime('%Y-%m-%d')} to {right.strftime('%Y-%m-%d')}")

    # Base URL construction needs adjustment based on daily/monthly
    if is_monthly:
         url_pattern_base = f"{BASE_URL}/monthly/{data_type}/{symbol}" # No interval for monthly
    elif interval:
        url_pattern_base = f"{BASE_URL}/daily/{data_type}/{symbol}/{interval}"
    else:
        url_pattern_base = f"{BASE_URL}/daily/{data_type}/{symbol}"


    # Check the most recent boundary first to ensure *some* data exists
    latest_check_date = right
    logger.debug(f"{log_prefix} Initial boundary check: left={left.isoformat()}, right={right.isoformat()}") # DEBUG LOG
    logger.debug(f"{log_prefix} Checking latest boundary: {latest_check_date.strftime('%Y-%m-%d' if not is_monthly else '%Y-%m')}")
    latest_exists = await check_date_exists(latest_check_date, symbol, data_type, interval, url_pattern_base, session, log_prefix)
    logger.debug(f"{log_prefix} Result for latest boundary ({latest_check_date.isoformat()}): {latest_exists}") # DEBUG LOG

    if not latest_exists:
        # If latest doesn't exist, try the previous period
        prev_period_date = None
        if is_monthly:
            prev_month_year = latest_check_date.year
            prev_month_month = latest_check_date.month - 1
            if prev_month_month == 0:
                prev_month_month = 12
                prev_month_year -= 1
            # Ensure the previous month is not before the start guess date's month
            if date(prev_month_year, prev_month_month, 1) >= left:
                 prev_period_date = date(prev_month_year, prev_month_month, 1)
        else:
            if latest_check_date - timedelta(days=1) >= left:
                prev_period_date = latest_check_date - timedelta(days=1)

        logger.debug(f"{log_prefix} Calculated prev_period_date: {prev_period_date.isoformat() if prev_period_date else 'None'}") # DEBUG LOG

        if prev_period_date:
            logger.debug(f"{log_prefix} Latest boundary failed, checking previous period: {prev_period_date.strftime('%Y-%m-%d' if not is_monthly else '%Y-%m')}")
            prev_exists = await check_date_exists(prev_period_date, symbol, data_type, interval, url_pattern_base, session, log_prefix) # Use new variable
            logger.debug(f"{log_prefix} Result for previous boundary ({prev_period_date.isoformat()}): {prev_exists}") # DEBUG LOG

            if not prev_exists: # Check the new variable
                 logger.warning(f"{log_prefix} No data found for latest or previous period ({prev_period_date.strftime('%Y-%m-%d' if not is_monthly else '%Y-%m')}). Returning None.")
                 return None
            else:
                 # Previous period exists. This is the latest known date with data.
                 logger.info(f"{log_prefix} Data found for previous period ({prev_period_date.strftime('%Y-%m-%d' if not is_monthly else '%Y-%m')}). This is the latest available.")
                 # If this previous period is the same as the start guess, it's the earliest.
                 if prev_period_date == left:
                     logger.info(f"{log_prefix} Previous period matches start guess. Returning it as earliest.")
                     return prev_period_date
                 # Otherwise, adjust the right boundary for the binary search
                 logger.info(f"{log_prefix} Adjusting search range end to {prev_period_date.strftime('%Y-%m-%d' if not is_monthly else '%Y-%m')}.")
                 right = prev_period_date # Start search from the latest known existing date
        else:
            # Previous period is before left boundary, or couldn't be calculated
            logger.warning(f"{log_prefix} Latest boundary failed, and previous period is invalid or before start guess. No data found.")
            return None

    # Now, 'right' is guaranteed to be a date where data exists (or the original 'right' if it existed initially)
    # Check the earliest boundary
    logger.debug(f"{log_prefix} Checking earliest boundary: {left.strftime('%Y-%m-%d' if not is_monthly else '%Y-%m')}")
    left_exists = await check_date_exists(left, symbol, data_type, interval, url_pattern_base, session, log_prefix) # Use variable
    logger.debug(f"{log_prefix} Result for earliest boundary ({left.isoformat()}): {left_exists}") # DEBUG LOG
    if left_exists: # Check variable
        logger.info(f"{log_prefix} Data found at the earliest possible guess date: {left.strftime('%Y-%m-%d' if not is_monthly else '%Y-%m')}")
        return left

    # If we reach here, data exists somewhere between left+1 and right (inclusive)
    logger.debug(f"{log_prefix} Proceeding with binary search in range: {left.strftime('%Y-%m-%d' if not is_monthly else '%Y-%m')} to {right.strftime('%Y-%m-%d' if not is_monthly else '%Y-%m')}")


    # Perform binary search
    while left <= right:
        # Calculate midpoint differently for monthly vs daily
        if is_monthly:
             # Calculate midpoint month
             total_months = (right.year - left.year) * 12 + right.month - left.month
             mid_month_offset = total_months // 2
             mid_year = left.year + (left.month + mid_month_offset -1) // 12
             mid_month = (left.month + mid_month_offset -1) % 12 + 1
             mid_date = date(mid_year, mid_month, 1)
        else:
             mid_date = left + (right - left) // 2

        mid_date_str = mid_date.strftime('%Y-%m-%d' if not is_monthly else '%Y-%m')
        left_str = left.strftime('%Y-%m-%d' if not is_monthly else '%Y-%m')
        right_str = right.strftime('%Y-%m-%d' if not is_monthly else '%Y-%m')
        logger.debug(f"{log_prefix} Testing mid date: {mid_date_str} (Range: {left_str} - {right_str})")

        if await check_date_exists(mid_date, symbol, data_type, interval, url_pattern_base, session, log_prefix):
            earliest_found = mid_date
            # Adjust right boundary based on monthly/daily
            if is_monthly:
                 # Move to the previous month
                 prev_month_year = mid_date.year
                 prev_month_month = mid_date.month - 1
                 if prev_month_month == 0:
                     prev_month_month = 12
                     prev_month_year -= 1
                 right = date(prev_month_year, prev_month_month, 1)
            else:
                 right = mid_date - timedelta(days=1)
            logger.debug(f"{log_prefix} Data found for {mid_date_str}. New range end: {right.strftime('%Y-%m-%d' if not is_monthly else '%Y-%m')}")
        else:
            # Adjust left boundary based on monthly/daily
            if is_monthly:
                 # Move to the next month
                 next_month_year = mid_date.year
                 next_month_month = mid_date.month + 1
                 if next_month_month > 12:
                     next_month_month = 1
                     next_month_year += 1
                 left = date(next_month_year, next_month_month, 1)
            else:
                 left = mid_date + timedelta(days=1)
            logger.debug(f"{log_prefix} Data NOT found for {mid_date_str}. New range start: {left.strftime('%Y-%m-%d' if not is_monthly else '%Y-%m')}")

        # Rate limiting
        await asyncio.sleep(DISCOVERY_RATE_LIMIT_DELAY)

    if earliest_found:
        earliest_found_str = earliest_found.strftime('%Y-%m-%d' if not is_monthly else '%Y-%m')
        logger.info(f"{log_prefix} Discovery complete. Earliest date found: {earliest_found_str}")
    else:
        start_guess_str = start_guess_date.strftime('%Y-%m-%d' if not is_monthly else '%Y-%m')
        today_str = today.strftime('%Y-%m-%d' if not is_monthly else '%Y-%m')
        logger.warning(f"{log_prefix} Discovery complete, but no date found within the range {start_guess_str} - {today_str}.")

    return earliest_found


# --- Download/Verify/Extract Functions ---

async def download_file(session: aiohttp.ClientSession, url: str, destination_path: Path, log_prefix: str) -> bool:
    """Downloads a file asynchronously with retries using aiohttp."""
    retries = 0
    # Use MAX_DOWNLOAD_RETRIES + 1 total attempts (initial + retries)
    while retries <= MAX_DOWNLOAD_RETRIES:
        try:
            attempt_num = retries + 1
            logger.debug(f"{log_prefix} Download attempt {attempt_num}/{MAX_DOWNLOAD_RETRIES + 1}: {url}")
            async with session.get(url, timeout=60) as response: # Use a reasonable timeout
                if response.status == 404:
                    logger.info(f"{log_prefix} File not available (404): {url}")
                    return False  # Don't retry 404s
                response.raise_for_status()  # Raise for other bad status codes (e.g., 5xx)

                destination_path.parent.mkdir(parents=True, exist_ok=True)
                with open(destination_path, 'wb') as f:
                    while True:
                        chunk = await response.content.read(8192) # Read in larger chunks
                        if not chunk:
                            break
                        f.write(chunk)
                logger.info(f"{log_prefix} Successfully downloaded {destination_path.name}")
                return True
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            retries += 1
            logger.warning(f"{log_prefix} Download attempt {attempt_num} failed for {url}: {type(e).__name__}. Retrying in {DOWNLOAD_RETRY_DELAY}s...")
            if retries > MAX_DOWNLOAD_RETRIES:
                logger.error(f"{log_prefix} Max download retries reached for {url}. Download failed.")
                return False
            await asyncio.sleep(DOWNLOAD_RETRY_DELAY)
        except Exception as e:
            logger.error(f"{log_prefix} Unexpected error during download of {url}: {type(e).__name__} - {e}")
            return False # Don't retry unexpected errors
    return False # Should only be reached if loop finishes due to retries exceeded

async def verify_checksum(session: aiohttp.ClientSession, file_path: Path, checksum_url: str, log_prefix: str) -> bool:
    """Verifies the checksum of a downloaded file using aiohttp."""
    if not CHECKSUM_VERIFICATION:
        logger.debug(f"{log_prefix} Checksum verification skipped.")
        return True

    try:
        # 1. Download checksum file content
        logger.debug(f"{log_prefix} Downloading checksum: {checksum_url}")
        async with session.get(checksum_url, timeout=15) as response:
            if response.status == 404:
                logger.warning(f"{log_prefix} Checksum file not found: {checksum_url}. Cannot verify.")
                return False # Treat as failure if checksum file is missing
            response.raise_for_status()
            checksum_content = await response.text()
            # Expected format: <sha256_hash>  <filename>
            expected_checksum = checksum_content.split()[0].lower()

        # 2. Calculate checksum of the downloaded file
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(65536), b""): # Read in larger blocks for hashing
                sha256_hash.update(byte_block)
        calculated_checksum = sha256_hash.hexdigest().lower()

        # 3. Compare checksums
        if calculated_checksum == expected_checksum:
            logger.info(f"{log_prefix} Checksum verified for {file_path.name}")
            return True
        else:
            logger.error(f"{log_prefix} Checksum mismatch for {file_path.name}: Expected {expected_checksum}, Got {calculated_checksum}")
            return False

    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.error(f"{log_prefix} Error downloading checksum file {checksum_url}: {e}")
        return False
    except IndexError:
         logger.error(f"{log_prefix} Could not parse checksum from {checksum_url}. Content: '{checksum_content[:100]}...'")
         return False
    except Exception as e:
        logger.error(f"{log_prefix} Unexpected error during checksum verification for {file_path.name}: {e}")
        return False

def verify_csv_file(csv_path: Path, log_prefix: str) -> bool:
    """Verifies that a CSV file exists and is not empty."""
    if not VERIFY_EXTRACTED_FILES:
        logger.debug(f"{log_prefix} CSV file verification skipped.")
        return True # Assume valid if verification is off

    try:
        if not csv_path.exists():
            logger.warning(f"{log_prefix} CSV file does not exist: {csv_path}")
            return False
        if csv_path.stat().st_size == 0:
            logger.error(f"{log_prefix} CSV file is empty: {csv_path}")
            return False
        # Optional: Add more robust checks like trying to read a line with pandas/csv reader
        # import pandas as pd
        # pd.read_csv(csv_path, nrows=1)
        logger.debug(f"{log_prefix} CSV file verified: {csv_path.name}")
        return True
    except Exception as e:
        logger.error(f"{log_prefix} Error verifying CSV file {csv_path}: {e}")
        return False

def extract_zip(zip_path: Path, extract_to_dir: Path, log_prefix: str) -> bool:
    """Extracts a zip file, verifies the extracted CSV, and optionally cleans up."""
    try:
        extract_to_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Check for corruption before extraction
            corruption_test = zip_ref.testzip()
            if corruption_test is not None:
                logger.error(f"{log_prefix} Zip file {zip_path.name} is corrupted (failed test on: {corruption_test})")
                return False

            csv_name = None
            for name in zip_ref.namelist():
                if name.endswith('.csv'):
                    csv_name = name
                    break
            if not csv_name:
                logger.error(f"{log_prefix} No CSV file found in {zip_path.name}")
                return False

            zip_ref.extractall(extract_to_dir)
            extracted_path = extract_to_dir / csv_name
            logger.info(f"{log_prefix} Successfully extracted {zip_path.name} to {extracted_path}")

            # Verify the extracted CSV
            if not verify_csv_file(extracted_path, log_prefix):
                logger.error(f"{log_prefix} Extracted file verification failed: {extracted_path}")
                try:
                    extracted_path.unlink() # Attempt to remove invalid CSV
                except OSError as e:
                    logger.error(f"{log_prefix} Failed to remove invalid extracted CSV {extracted_path}: {e}")
                return False # Extraction considered failed if CSV is invalid

            # Clean up ZIP if successful and enabled
            if CLEAN_UP_ZIPS:
                try:
                    zip_path.unlink()
                    logger.info(f"{log_prefix} Cleaned up ZIP file: {zip_path.name}")
                except OSError as e:
                    logger.error(f"{log_prefix} Failed to clean up ZIP file {zip_path.name}: {e}")
            return True # Extraction successful

    except zipfile.BadZipFile:
        logger.error(f"{log_prefix} Error: Bad zip file {zip_path.name}")
        return False
    except Exception as e:
        logger.error(f"{log_prefix} Error extracting {zip_path.name}: {e}")
        return False


async def process_file(
    session: aiohttp.ClientSession,
    symbol: str,
    data_type: str,
    interval: Optional[str],
    target_date: date,
    is_monthly: bool,
    semaphore: asyncio.Semaphore
) -> bool:
    """
    Downloads, verifies, and extracts a single data file for a given date/month.
    Returns True if the final CSV is valid, False otherwise.
    """
    date_str = target_date.strftime("%Y-%m-%d")
    month_str = target_date.strftime("%Y-%m")
    log_prefix = f"[{symbol} {data_type} {interval or ''} {month_str if is_monthly else date_str}]"

    # Determine paths and URLs
    if is_monthly:
        file_date_str = month_str
        url_freq = "monthly"
        extract_sub_dir = EXTRACTED_DATA_DIR / symbol / data_type
        download_sub_dir = DOWNLOADS_DIR / symbol / data_type
        url_path_segment = f"{url_freq}/{data_type}/{symbol}"
        file_name_base = f"{symbol}-{data_type}-{file_date_str}"
    else: # Daily
        file_date_str = date_str
        url_freq = "daily"
        extract_sub_dir = EXTRACTED_DATA_DIR / symbol / data_type
        download_sub_dir = DOWNLOADS_DIR / symbol / data_type
        url_path_segment = f"{url_freq}/{data_type}/{symbol}"
        if interval:
            extract_sub_dir = extract_sub_dir / interval
            download_sub_dir = download_sub_dir / interval
            url_path_segment = f"{url_path_segment}/{interval}"
            file_name_base = f"{symbol}-{interval}-{file_date_str}"
        else:
            file_name_base = f"{symbol}-{data_type}-{file_date_str}"

    zip_file_name = f"{file_name_base}.zip"
    checksum_file_name = f"{zip_file_name}.CHECKSUM"
    csv_file_name = f"{file_name_base}.csv"

    download_url = f"{BASE_URL}/{url_path_segment}/{zip_file_name}"
    checksum_url = f"{BASE_URL}/{url_path_segment}/{checksum_file_name}"
    zip_destination_path = download_sub_dir / zip_file_name
    expected_csv_path = extract_sub_dir / csv_file_name

    # Ensure directories exist
    extract_sub_dir.mkdir(parents=True, exist_ok=True)
    download_sub_dir.mkdir(parents=True, exist_ok=True)

    # --- Processing Steps ---
    logger.debug(f"{log_prefix} Starting processing.")

    # 1. Check if valid CSV already exists
    if verify_csv_file(expected_csv_path, log_prefix):
        logger.info(f"{log_prefix} Skipping, valid extracted file already exists: {expected_csv_path.name}")
        return True

    # Use semaphore to limit concurrency for network operations
    async with semaphore:
        # 2. Check if ZIP exists and is valid
        zip_exists_and_valid = False
        if zip_destination_path.exists():
            logger.info(f"{log_prefix} Found existing ZIP: {zip_destination_path.name}. Verifying...")
            if await verify_checksum(session, zip_destination_path, checksum_url, log_prefix):
                logger.info(f"{log_prefix} Existing ZIP is valid.")
                zip_exists_and_valid = True
            else:
                logger.warning(f"{log_prefix} Existing ZIP is corrupt. Deleting and re-downloading.")
                try:
                    zip_destination_path.unlink()
                except OSError as e:
                    logger.error(f"{log_prefix} Failed to delete corrupt zip {zip_destination_path.name}: {e}. Cannot proceed.")
                    return False # Critical failure

        # 3. Download if necessary
        if not zip_exists_and_valid:
            logger.info(f"{log_prefix} Downloading: {download_url}")
            download_successful = await download_file(session, download_url, zip_destination_path, log_prefix)
            if not download_successful:
                # If download fails (e.g., 404 or max retries), the file is considered missing for this date.
                logger.warning(f"{log_prefix} Download failed. File considered missing.")
                return False # File is missing or couldn't be downloaded

            # Verify newly downloaded ZIP
            if not await verify_checksum(session, zip_destination_path, checksum_url, log_prefix):
                logger.error(f"{log_prefix} Newly downloaded ZIP failed verification. Cannot proceed.")
                # Optionally delete the corrupt downloaded zip
                # try: zip_destination_path.unlink() except OSError: pass
                return False # Critical failure

        # 4. Extract ZIP (if it exists and is valid)
        if zip_destination_path.exists():
            logger.info(f"{log_prefix} Extracting {zip_destination_path.name}")
            extract_successful = extract_zip(zip_destination_path, extract_sub_dir, log_prefix)
            if not extract_successful:
                 logger.error(f"{log_prefix} Extraction failed.")
                 return False # Extraction failed, final CSV is not valid/available
            # If extraction succeeded, the final CSV should be valid (verified in extract_zip)
            return True
        else:
            # This case should ideally not happen if download logic is correct, but handles edge cases.
            logger.warning(f"{log_prefix} ZIP file not found for extraction. This might indicate a prior download failure.")
            return False


# --- Reporting Logic ---
def write_final_report(missing_files: List[Dict]):
    if not missing_files:
        logger.info("No missing files to report.")
        return

    report_filename = REPORTS_DIR / f"missing_files_report_{timestamp}.csv"
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Writing missing files report to: {report_filename}")
    try:
        with open(report_filename, 'w', newline='', encoding='utf-8') as csvfile:
            if missing_files:
                # Ensure all dicts have the same keys for header consistency
                # Get all unique keys from all dictionaries
                fieldnames_set = set()
                for item in missing_files:
                    fieldnames_set.update(item.keys())
                fieldnames = sorted(list(fieldnames_set)) # Sort for consistent order

                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(missing_files)
            else:
                 csvfile.write("No missing files found after retries.\n")
        logger.info("Missing files report written successfully.")
    except Exception as e:
        logger.error(f"Failed to write missing files report: {e}")


# --- Main Application Logic ---

async def main(symbols: List[str], start_date_arg: date):
    logger.info(f"Starting unified downloader for symbols: {symbols}, start date: {start_date_arg.isoformat()}")

    # 1. Load or Discover Earliest Dates
    availability_config = {}
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, 'r') as f:
                availability_config = json.load(f)
            logger.info(f"Loaded existing availability config from {CONFIG_FILE}")
        except Exception as e:
            logger.warning(f"Could not load config file {CONFIG_FILE}: {e}. Will run discovery.")
            availability_config = {}

    # Use aiohttp session for discovery
    discovery_needed = False
    discovery_start_guess = datetime.strptime(DISCOVERY_START_GUESS_DATE_STR, "%Y-%m-%d").date()
    combinations_to_discover = []

    # Determine which combinations need discovery
    for symbol in symbols:
        if symbol not in availability_config: availability_config[symbol] = {}
        for data_type in ALL_DATA_TYPES:
            if data_type not in availability_config[symbol]: availability_config[symbol][data_type] = {}
            if data_type in INTERVAL_TYPES:
                interval = DEFAULT_KLINES_INTERVAL # Assuming default interval for now
                # Check if key exists and is not None/empty string
                if interval not in availability_config[symbol][data_type] or not availability_config[symbol][data_type].get(interval):
                    combinations_to_discover.append((symbol, data_type, interval))
                    discovery_needed = True
            else: # Non-interval types
                config_key = 'all'
                # Check if key exists and is not None/empty string
                if config_key not in availability_config[symbol][data_type] or not availability_config[symbol][data_type].get(config_key):
                    combinations_to_discover.append((symbol, data_type, None)) # None interval
                    discovery_needed = True

    # Run discovery if needed
    if discovery_needed:
        logger.info(f"Running discovery for {len(combinations_to_discover)} combinations...")
        # Create aiohttp session specifically for discovery with SSL settings
        connector = create_aiohttp_connector()
        async with aiohttp.ClientSession(connector=connector) as discovery_aio_session:
            discovery_tasks = []
            for symbol, data_type, interval in combinations_to_discover:
                 discovery_tasks.append(
                     discover_earliest_date(symbol, data_type, interval, discovery_start_guess, discovery_aio_session)
                 )

            # Run discovery tasks concurrently
            discovered_dates = [] # Store results in order
            try:
                # Use tqdm for progress if available
                with tqdm(total=len(discovery_tasks), desc="Discovering earliest dates") as pbar:
                    # Use asyncio.gather to run all and maintain order
                    discovered_dates = await asyncio.gather(*[asyncio.create_task(coro) for coro in discovery_tasks])
                    pbar.update(len(discovered_dates)) # Update progress after all complete
            except ImportError:
                logger.info("tqdm not found, running discovery without progress bar.")
                # Gather results without progress bar if tqdm is not available
                discovered_dates = await asyncio.gather(*discovery_tasks)

            # Update config with discovered dates
            if len(discovered_dates) == len(combinations_to_discover): # Ensure results match tasks
                for i, (symbol, data_type, interval) in enumerate(combinations_to_discover):
                    earliest = discovered_dates[i]
                    # Determine format based on monthly/daily for saving
                    is_monthly_discovery = data_type == "fundingRate"
                    date_format_save = "%Y-%m" if is_monthly_discovery else "%Y-%m-%d"
                    earliest_str = earliest.strftime(date_format_save) if earliest else None

                    config_key = interval if interval else 'all'
                    # Ensure nested dicts exist before assignment
                    if symbol not in availability_config: availability_config[symbol] = {}
                    if data_type not in availability_config[symbol]: availability_config[symbol][data_type] = {}
                    availability_config[symbol][data_type][config_key] = earliest_str
                    logger.info(f"Discovered: {symbol} {data_type} {interval or ''} -> {earliest_str}")
            else:
                 logger.error("Mismatch between discovery tasks and results. Config update skipped.")


        # Save updated config
        try:
            CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True) # Ensure config dir exists
            with open(CONFIG_FILE, 'w') as f:
                json.dump(availability_config, f, indent=2, sort_keys=True)
            logger.info(f"Saved updated availability config to {CONFIG_FILE}")
        except Exception as e:
            logger.error(f"Could not save config file {CONFIG_FILE}: {e}")

    # 2. Verification and Download Loop
    logger.info("Starting verification and download phase...")
    all_tasks_info = [] # Store info about each task for reporting
    missing_files_final = []
    today = date.today()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
    # Create a new session specifically for downloads, manage its lifecycle
    download_session = None
    try:
        # Create download session with SSL settings
        connector = create_aiohttp_connector()
        timeout = aiohttp.ClientTimeout(total=120, connect=30)
        download_session = aiohttp.ClientSession(
            connector=connector, 
            timeout=timeout,
            headers={'User-Agent': 'Mozilla/5.0 (compatible; Binance-Data-Downloader/1.0)'}
        )

        # Generate all expected file tasks based on effective start dates
        for symbol in symbols:
            for data_type in ALL_DATA_TYPES:
                is_monthly = data_type == "fundingRate"
                interval = DEFAULT_KLINES_INTERVAL if data_type in INTERVAL_TYPES else None
                config_key = interval if interval else 'all'

                # Determine effective start date from config or argument
                earliest_available_str = availability_config.get(symbol, {}).get(data_type, {}).get(config_key)
                earliest_available_date = None
                if earliest_available_str:
                    try:
                        # Handle monthly YYYY-MM vs daily YYYY-MM-DD parsing
                        date_format_parse = "%Y-%m" if is_monthly else "%Y-%m-%d"
                        earliest_available_date = datetime.strptime(earliest_available_str, date_format_parse).date()
                        if is_monthly: earliest_available_date = earliest_available_date.replace(day=1) # Ensure it's the 1st for comparison
                    except ValueError:
                        logger.warning(f"Invalid date format '{earliest_available_str}' in config for {symbol}/{data_type}/{config_key}. Ignoring.")

                # Use the later of arg start date or discovered earliest date
                effective_start_date = start_date_arg
                if earliest_available_date and earliest_available_date > effective_start_date:
                    effective_start_date = earliest_available_date
                    logger.info(f"Adjusted start date for {symbol}/{data_type}/{config_key} to {effective_start_date.isoformat()} based on availability.")
                elif not earliest_available_date:
                    logger.warning(f"No availability info for {symbol}/{data_type}/{config_key}. Using argument start date: {start_date_arg.isoformat()}")

                # Iterate through dates/months to generate tasks
                current_date_iter = effective_start_date
                while current_date_iter <= today:
                    target_date_for_task = current_date_iter
                    if is_monthly:
                        # Monthly data file (e.g., 2023-05) becomes available after the month ends (e.g., June 1st).
                        # We need to check for the file corresponding to the *previous* month.
                        # Only generate a task if the *start* of the current iteration month is before the *start* of today's month.
                        current_month_start = current_date_iter.replace(day=1)
                        if current_month_start < today.replace(day=1):
                             # The file we need corresponds to the month *before* current_date_iter
                             target_month_date = (current_month_start - timedelta(days=1)).replace(day=1)
                             all_tasks_info.append({
                                 "symbol": symbol, "data_type": data_type, "interval": None,
                                 "target_date": target_month_date, "is_monthly": True
                             })
                        # Move to the first day of the next month
                        next_month_year = current_date_iter.year + (current_date_iter.month // 12)
                        next_month_month = (current_date_iter.month % 12) + 1
                        # Stop if the next month is after today's month start
                        if date(next_month_year, next_month_month, 1) > today.replace(day=1):
                            break
                        current_date_iter = date(next_month_year, next_month_month, 1)
                    else: # Daily
                        # Generate task for the current day
                        all_tasks_info.append({
                            "symbol": symbol, "data_type": data_type, "interval": interval,
                            "target_date": target_date_for_task, "is_monthly": False
                        })
                        # Move to the next day
                        current_date_iter += timedelta(days=1)

        # Create coroutines for all processing tasks
        process_tasks_coroutines = [
            process_file(
                download_session, t["symbol"], t["data_type"], t["interval"],
                t["target_date"], t["is_monthly"], semaphore
            ) for t in all_tasks_info
        ]

        # Execute tasks concurrently and collect results
        logger.info(f"Generated {len(all_tasks_info)} file processing tasks.")
        results = [] # Store results in order corresponding to all_tasks_info
        try:
            with tqdm(total=len(process_tasks_coroutines), desc="Processing files") as pbar:
                 # Use asyncio.gather to maintain order easily
                 results = await asyncio.gather(*[asyncio.create_task(coro) for coro in process_tasks_coroutines])
                 # Update progress bar based on number of results (though gather waits for all)
                 pbar.update(len(results))
        except ImportError:
            logger.info("tqdm not found, processing files without progress bar.")
            results = await asyncio.gather(*process_tasks_coroutines) # Gather results without progress

        # Identify missing files based on results
        if len(results) == len(all_tasks_info):
            for i, task_info in enumerate(all_tasks_info):
                if not results[i]: # If process_file returned False
                    date_str = task_info["target_date"].strftime("%Y-%m" if task_info["is_monthly"] else "%Y-%m-%d")
                    missing_detail = {
                        "symbol": task_info["symbol"],
                        "data_type": task_info["data_type"],
                        "interval": task_info["interval"] if task_info["interval"] else 'N/A',
                        "date_or_month": date_str,
                        "is_monthly": task_info["is_monthly"],
                        "status": "Missing or invalid after processing"
                    }
                    missing_files_final.append(missing_detail)
                    logger.warning(f"Failed to process/verify: {task_info['symbol']} {task_info['data_type']} {task_info['interval'] or ''} for {date_str}")
        else:
            logger.error("Mismatch between processing tasks and results. Cannot reliably report missing files.")


        logger.info(f"Verification and download phase complete. Found {len(missing_files_final)} missing/invalid file(s).")

    finally:
        if download_session:
            await download_session.close() # Ensure download session is closed
            logger.debug("Download session closed.")

    # 3. Final Report
    write_final_report(missing_files_final)

    logger.info("Unified downloader process finished.")


# --- Entry Point ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unified Binance Futures Data Downloader and Verifier.")
    parser.add_argument(
        "--symbols",
        type=str,
        default=",".join(DEFAULT_SYMBOLS),
        help=f"Comma-separated list of symbols (default: {','.join(DEFAULT_SYMBOLS)})"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=DEFAULT_START_DATE_STR,
        help=f"Start date for verification/download (YYYY-MM-DD format, default: {DEFAULT_START_DATE_STR})"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable detailed debug logging"
    )

    args = parser.parse_args()

    # Update log level if verbose
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled.")

    # Parse symbols
    symbols_list = [s.strip().upper() for s in args.symbols.split(',') if s.strip()]
    if not symbols_list:
        logger.error("No valid symbols provided.")
        sys.exit(1)

    # Parse start date
    try:
        start_date_obj = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    except ValueError:
        logger.error(f"Invalid start-date format: {args.start_date}. Please use YYYY-MM-DD.")
        sys.exit(1)

    # Run the main async function
    try:
        asyncio.run(main(symbols_list, start_date_obj))
    except KeyboardInterrupt:
        logger.info("Process interrupted by user.")
        sys.exit(0)
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}") # Log full traceback
        sys.exit(1)