import os
import requests
import pandas as pd
# import datetime # Already imported below as datetime from datetime
import aiohttp
import asyncio
import zipfile
import logging
import hashlib
import argparse
import csv # +++ Add csv import +++
import venv_utils # +++ Add venv_utils import +++
from pathlib import Path
from datetime import datetime, date, timedelta # +++ Import specific types +++

# --- Logging Configuration ---
# Get root logger
logger = logging.getLogger()
# Remove existing handlers if any (to avoid duplicates if script is re-run in interactive session)
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
logger.setLevel(logging.INFO) # Set level on the root logger

# Create formatter
log_format = '%(asctime)s - %(levelname)s - %(message)s'
formatter = logging.Formatter(log_format)

# Create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Create file handler
log_dir = Path("logs")
try:
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = log_dir / f"binance_downloader_{timestamp}.log"
    file_handler = logging.FileHandler(log_filename, mode='w', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    print(f"Logging to console and file: {log_filename}") # Indicate where logs are going
except Exception as e:
    print(f"Warning: Could not create log file handler for {log_dir / 'binance_downloader_*.log'}: {e}. Logging to console only.")
# --- End Logging Configuration ---


# Constants (Defaults)
BASE_URL = "https://data.binance.vision/data/futures/um"
DEFAULT_SYMBOLS = ["BTCUSDT", "BTCUSDC"]
MAX_CONCURRENT_DOWNLOADS = 5  # Limit concurrent downloads
CLEAN_UP_ZIPS = True  # Set to False to keep ZIP files
MAX_TASKS_BEFORE_WAIT = 1000  # Maximum number of tasks to queue before waiting
VERIFY_EXTRACTED_FILES = True  # Enable additional verification of extracted files
DEFAULT_DATA_TYPES_DAILY = [
    "klines", "bookDepth", "metrics", "trades", "aggTrades", # Existing
    "indexPriceKlines", "markPriceKlines", "premiumIndexKlines" # New (Removed bookTicker)
]
DEFAULT_DATA_TYPES_MONTHLY = ["fundingRate"]
DEFAULT_KLINES_INTERVAL = "1m" # Default interval for klines, indexPriceKlines, markPriceKlines, premiumIndexKlines
DEFAULT_START_DATE_STR = "2020-01-01" # *** Updated Default Start Date ***
MAX_VERIFICATION_RETRIES = 2 # +++ Number of redownload attempts after initial check +++

# Define types that use intervals globally
INTERVAL_TYPES = ["klines", "indexPriceKlines", "markPriceKlines", "premiumIndexKlines"]

# Directories
DOWNLOADS_DIR = Path("downloads")
EXTRACTED_DATA_DIR = Path("data")
REPORTS_DIR = Path("reports")

# --- Helper Functions ---

MAX_RETRIES = 3
RETRY_DELAY = 5 # seconds

async def download_file(session, url, destination_path):
    """Downloads a file asynchronously with retries."""
    retries = 0
    while retries < MAX_RETRIES:
        try:
            logging.debug(f"Attempting download (try {retries+1}/{MAX_RETRIES}): {url}")
            async with session.get(url) as response:
                if response.status == 404:
                    logging.info(f"File not available (404): {url}")
                    return False  # Don't retry 404s - data likely doesn't exist
                response.raise_for_status()  # Raise an exception for other bad status codes
                destination_path.parent.mkdir(parents=True, exist_ok=True)
                with open(destination_path, 'wb') as f:
                    while True:
                        chunk = await response.content.read(1024) # Read in chunks
                        if not chunk:
                            break
                        f.write(chunk)
                logging.info(f"Successfully downloaded {url} to {destination_path}")
                return True
        except (aiohttp.ClientError, asyncio.TimeoutError) as e: # Catch ClientError and TimeoutError specifically for retries
            retries += 1
            logging.warning(f"Download attempt {retries}/{MAX_RETRIES} failed for {url}: {type(e).__name__} - {e}. Retrying in {RETRY_DELAY}s...")
            if retries >= MAX_RETRIES:
                logging.error(f"Max retries reached for {url}. Download failed.")
                return False
            await asyncio.sleep(RETRY_DELAY) # Wait before retrying
        except Exception as e:
            # Log other unexpected errors and don't retry
            logging.error(f"An unexpected non-retryable error occurred during download of {url}: {type(e).__name__} - {e}")
            return False # Don't retry for other exceptions
    return False

async def verify_checksum(session, file_path, checksum_url):
    """Verifies the checksum of a downloaded file."""
    try:
        # 1. Download checksum file content
        async with session.get(checksum_url) as response:
            if response.status == 404:
                logging.error(f"Checksum file not found for {file_path.name} at {checksum_url}. Verification failed.")
                return False # Treat as failure if checksum file is missing
            response.raise_for_status()
            checksum_content = await response.text()
            # Expected format: <sha256_hash>  <filename>
            expected_checksum = checksum_content.split()[0].lower()

        # 2. Calculate checksum of the downloaded file
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        calculated_checksum = sha256_hash.hexdigest().lower()

        # 3. Compare checksums
        if calculated_checksum == expected_checksum:
            logging.info(f"Checksum verified for {file_path.name}")
            return True
        else:
            logging.error(f"Checksum mismatch for {file_path.name}: Expected {expected_checksum}, Got {calculated_checksum}")
            return False

    except aiohttp.ClientError as e:
        logging.error(f"Error downloading checksum file {checksum_url}: {e}")
        return False # Fail verification if checksum download fails
    except IndexError:
         logging.error(f"Could not parse checksum from {checksum_url}. Content: '{checksum_content[:100]}...'")
         return False # Fail if checksum format is unexpected
    except Exception as e:
        logging.error(f"An unexpected error occurred during checksum verification for {file_path.name}: {e}")
        return False

def verify_csv_file(csv_path: Path) -> bool:
    """
    Verifies that a CSV file is complete and valid.
    Returns True if the file is valid, False otherwise.
    """
    try:
        if not csv_path.exists():
            return False
        if csv_path.stat().st_size == 0:
            logging.error(f"CSV file is empty: {csv_path}")
            return False
        df = pd.read_csv(csv_path, nrows=1) # Just check if readable
        return True
    except Exception as e:
        logging.error(f"Error verifying CSV file {csv_path}: {e}")
        return False

def extract_zip(zip_path, extract_to_dir):
    """Extracts a zip file to the specified directory and verifies the extracted file."""
    try:
        extract_to_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            if zip_ref.testzip() is not None:
                logging.error(f"Zip file {zip_path.name} is corrupted")
                return False
            csv_name = None
            for name in zip_ref.namelist():
                if name.endswith('.csv'):
                    csv_name = name
                    break
            if not csv_name:
                logging.error(f"No CSV file found in {zip_path.name}")
                return False
            zip_ref.extractall(extract_to_dir)
            extracted_path = extract_to_dir / csv_name
            if VERIFY_EXTRACTED_FILES and not verify_csv_file(extracted_path):
                logging.error(f"Extracted file verification failed: {extracted_path}")
                try:
                    extracted_path.unlink()
                except OSError as e:
                    logging.error(f"Failed to remove invalid CSV {extracted_path}: {e}")
                return False
            logging.info(f"Successfully extracted and verified {zip_path.name} to {extract_to_dir}")
            return True
    except zipfile.BadZipFile:
        logging.error(f"Error: Bad zip file {zip_path.name}")
        return False
    except Exception as e:
        logging.error(f"Error extracting {zip_path.name}: {e}")
        return False

async def process_single_file(session, download_url, checksum_url, zip_destination_path, extract_to_dir):
    """Helper coroutine to download, verify, and extract a single file."""
    logging.debug(f"Processing: {download_url}")

    # --- Step 1: Check if extracted CSV already exists ---
    expected_csv_name = zip_destination_path.stem + ".csv"
    expected_csv_path = extract_to_dir / expected_csv_name
    if expected_csv_path.exists():
        # Check validity if it exists
        if VERIFY_EXTRACTED_FILES and not verify_csv_file(expected_csv_path):
             logging.warning(f"Existing CSV file is invalid/empty: {expected_csv_path}. Will attempt redownload/re-extraction.")
             try:
                 expected_csv_path.unlink() # Remove invalid file before proceeding
             except OSError as e:
                 logging.error(f"Failed to remove invalid existing CSV {expected_csv_path}: {e}. Skipping.")
                 return # Cannot proceed if invalid file cannot be removed
        else:
             logging.info(f"Skipping, valid extracted file already exists: {expected_csv_path}")
             return # Skip if valid CSV exists

    # Limit concurrent downloads with semaphore
    async with semaphore:
        # --- Step 2: Check if ZIP file exists ---
        zip_exists = zip_destination_path.exists()
        proceed_to_download = False

        if zip_exists:
            logging.info(f"Found existing ZIP: {zip_destination_path.name}. Verifying checksum...")
            # --- Step 3: Verify existing ZIP ---
            if await verify_checksum(session, zip_destination_path, checksum_url):
                logging.info(f"Existing ZIP {zip_destination_path.name} is valid. Proceeding to extraction.")
                # Proceed directly to extraction (Step 6)
            else:
                logging.warning(f"Existing ZIP {zip_destination_path.name} is corrupt (checksum mismatch). Deleting and re-downloading.")
                try:
                    zip_destination_path.unlink() # Delete corrupt zip
                except OSError as e:
                    logging.error(f"Failed to delete corrupt zip {zip_destination_path.name}: {e}")
                    return # Don't proceed if deletion fails
                proceed_to_download = True
        else:
            # --- Step 4: ZIP doesn't exist, proceed to download ---
            proceed_to_download = True

        # --- Step 5: Download if needed ---
        if proceed_to_download:
            logging.info(f"Downloading: {download_url}")
            download_successful = await download_file(session, download_url, zip_destination_path)
            if not download_successful:
                return # Stop processing this file if download fails

            # --- Step 5b: Verify newly downloaded ZIP ---
            logging.info(f"Verifying checksum for newly downloaded {zip_destination_path.name}...")
            if not await verify_checksum(session, zip_destination_path, checksum_url):
                logging.error(f"Newly downloaded ZIP {zip_destination_path.name} failed checksum verification. Skipping extraction.")
                return # Stop processing this file

        # --- Step 6: Extract ZIP ---
        if zip_destination_path.exists():
            logging.info(f"Extracting {zip_destination_path.name} to {extract_to_dir}")
            extract_successful = extract_zip(zip_destination_path, extract_to_dir)

            # --- Step 7: Clean up ZIP if extraction was successful ---
            if extract_successful and CLEAN_UP_ZIPS:
                try:
                    zip_destination_path.unlink()
                    logging.info(f"Cleaned up ZIP file: {zip_destination_path.name}")
                except OSError as e:
                    logging.error(f"Failed to clean up ZIP file {zip_destination_path.name}: {e}")
        else:
             logging.warning(f"ZIP file {zip_destination_path.name} not found for extraction. This might happen if download failed.")


# --- Main Download Logic ---
async def download_data(symbols, data_types_daily, data_types_monthly, klines_interval, start_date, end_date):
    """
    Main function to download, process, verify, and retry missing Binance futures data.
    """
    logging.info(f"Starting data download process for symbols: {symbols}, daily types: {data_types_daily}, monthly types: {data_types_monthly}")
    logging.info(f"Date range: {start_date} to {end_date}, Interval: {klines_interval}")

    # Create necessary directories
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    EXTRACTED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    for symbol in symbols:
        symbol_download_dir = DOWNLOADS_DIR / symbol
        symbol_extract_dir = EXTRACTED_DATA_DIR / symbol
        symbol_download_dir.mkdir(parents=True, exist_ok=True)
        symbol_extract_dir.mkdir(parents=True, exist_ok=True)
        all_data_types = list(set(data_types_daily + data_types_monthly))
        for data_type in all_data_types:
            extract_path_base = symbol_extract_dir / data_type
            download_path_base = symbol_download_dir / data_type
            if data_type in INTERVAL_TYPES:
                 extract_path_base = extract_path_base / klines_interval
                 download_path_base = download_path_base / klines_interval
            extract_path_base.mkdir(parents=True, exist_ok=True)
            download_path_base.mkdir(parents=True, exist_ok=True)

    # Define the semaphore for concurrency control
    global semaphore
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

    # Configure timeout for the session
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = []

        # --- Initial Download Pass ---
        logging.info("--- Starting Initial Download Pass ---")
        current_date_dl = start_date
        while current_date_dl <= end_date:
            date_str = current_date_dl.strftime("%Y-%m-%d")
            logging.info(f"Processing date: {date_str}")

            for symbol in symbols:
                symbol_download_dir = DOWNLOADS_DIR / symbol
                symbol_extract_dir = EXTRACTED_DATA_DIR / symbol

                # Daily Data Types
                for data_type in data_types_daily:
                    extract_sub_dir = symbol_extract_dir / data_type
                    url_path_segment = f"daily/{data_type}/{symbol}"
                    interval = 'N/A'
                    if data_type in INTERVAL_TYPES:
                        file_name_base = f"{symbol}-{klines_interval}-{date_str}"
                        url_path_segment = f"daily/{data_type}/{symbol}/{klines_interval}"
                        extract_sub_dir = symbol_extract_dir / data_type / klines_interval
                        interval = klines_interval
                    elif data_type in ["bookDepth", "metrics", "trades"]:
                        file_name_base = f"{symbol}-{data_type}-{date_str}"
                    elif data_type == "aggTrades":
                        file_name_base = f"{symbol}-{data_type}-{date_str}"
                        url_path_segment = f"daily/aggTrades/{symbol}"
                    else:
                        logging.warning(f"Path/filename logic not defined for daily data type: {data_type}. Skipping.")
                        continue

                    zip_file_name = f"{file_name_base}.zip"
                    checksum_file_name = f"{zip_file_name}.CHECKSUM"
                    download_url = f"{BASE_URL}/{url_path_segment}/{zip_file_name}"
                    checksum_url = f"{BASE_URL}/{url_path_segment}/{checksum_file_name}"
                    download_sub_dir = symbol_download_dir / data_type
                    if data_type in INTERVAL_TYPES:
                        download_sub_dir = download_sub_dir / klines_interval
                    zip_destination_path = download_sub_dir / zip_file_name

                    tasks.append(asyncio.create_task(
                        process_single_file(session, download_url, checksum_url, zip_destination_path, extract_sub_dir)
                    ))
                    if len(tasks) >= MAX_TASKS_BEFORE_WAIT:
                        logging.info(f"Reached {MAX_TASKS_BEFORE_WAIT} queued tasks. Processing batch...")
                        await asyncio.gather(*tasks)
                        tasks = []

                # Monthly Data Types (Funding Rate)
                if current_date_dl.day == 1:
                    prev_month_date = current_date_dl - timedelta(days=1)
                    prev_month_str = prev_month_date.strftime("%Y-%m")
                    for data_type in data_types_monthly:
                        if data_type == "fundingRate":
                            file_name_base = f"{symbol}-{data_type}-{prev_month_str}"
                            url_path_segment = f"monthly/{data_type}/{symbol}"
                            extract_sub_dir = symbol_extract_dir / data_type
                            zip_file_name = f"{file_name_base}.zip"
                            checksum_file_name = f"{zip_file_name}.CHECKSUM"
                            download_url = f"{BASE_URL}/{url_path_segment}/{zip_file_name}"
                            checksum_url = f"{BASE_URL}/{url_path_segment}/{checksum_file_name}"
                            zip_destination_path = symbol_download_dir / data_type / zip_file_name
                            zip_destination_path.parent.mkdir(parents=True, exist_ok=True)
                            logging.info(f"Attempting download of monthly {data_type} for {symbol} - {prev_month_str}")
                            tasks.append(asyncio.create_task(
                                process_single_file(session, download_url, checksum_url, zip_destination_path, extract_sub_dir)
                            ))
                        else:
                             logging.warning(f"Monthly data type '{data_type}' not currently supported for download. Skipping.")

            current_date_dl += timedelta(days=1)

        # Wait for remaining initial download tasks
        if tasks:
            logging.info(f"Waiting for {len(tasks)} remaining initial download tasks...")
            await asyncio.gather(*tasks)
            logging.info("Initial download tasks finished.")
        else:
            logging.info("No initial download tasks were created.")
        # --- End Initial Download Pass ---


        # --- Verification and Retry Loop ---
        final_missing_details = []
        for attempt in range(MAX_VERIFICATION_RETRIES + 1): # Initial check + N retries
            logging.info(f"--- Starting Verification Attempt {attempt + 1}/{MAX_VERIFICATION_RETRIES + 1} ---")
            current_missing_details = []
            processed_months_verification = set()

            # --- Verification Logic (Integrated) ---
            current_date_verify = start_date
            while current_date_verify <= end_date:
                date_str = current_date_verify.strftime("%Y-%m-%d")
                for symbol in symbols:
                    symbol_extract_dir = EXTRACTED_DATA_DIR / symbol
                    # Daily Check
                    for data_type in data_types_daily:
                        extract_sub_dir = symbol_extract_dir / data_type
                        file_name_base = f"{symbol}-{data_type}-{date_str}"
                        interval = 'N/A'
                        if data_type in INTERVAL_TYPES:
                            extract_sub_dir = extract_sub_dir / klines_interval
                            file_name_base = f"{symbol}-{klines_interval}-{date_str}"
                            interval = klines_interval
                        csv_filename = f"{file_name_base}.csv"
                        csv_path = extract_sub_dir / csv_filename

                        if not csv_path.exists() or not verify_csv_file(csv_path): # Check existence and validity
                            if csv_path.exists(): # Log if invalid
                                 logging.warning(f"Found invalid/empty CSV during verification: {csv_path}")
                            download_sub_dir = DOWNLOADS_DIR / symbol / data_type
                            zip_file_name = f"{file_name_base}.zip"
                            if data_type in INTERVAL_TYPES:
                                download_sub_dir = download_sub_dir / klines_interval
                            zip_path = download_sub_dir / zip_file_name
                            current_missing_details.append({
                                'symbol': symbol, 'data_type': data_type, 'interval': interval,
                                'date_or_month': date_str, 'is_monthly': False,
                                'expected_csv_path': str(csv_path), 'zip_exists': zip_path.exists(),
                                'expected_zip_path': str(zip_path)
                            })
                current_date_verify += timedelta(days=1)

            # Monthly Check
            unique_months_in_range = set()
            temp_date = start_date
            while temp_date <= end_date:
                if temp_date.day == 1:
                     prev_month_date = temp_date - timedelta(days=1)
                     unique_months_in_range.add(prev_month_date.strftime("%Y-%m"))
                if temp_date == end_date:
                     current_real_month = date.today().strftime("%Y-%m")
                     end_dt_month = end_date.strftime("%Y-%m")
                     if end_dt_month < current_real_month:
                          unique_months_in_range.add(end_dt_month)
                     elif end_dt_month == (date.today().replace(day=1) - timedelta(days=1)).strftime("%Y-%m"):
                         unique_months_in_range.add(end_dt_month)
                temp_date += timedelta(days=1)
            start_dt_prev_month = (start_date.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
            if start_dt_prev_month in unique_months_in_range:
                 unique_months_in_range.remove(start_dt_prev_month)
            today_year_month = date.today().strftime("%Y-%m")

            for month_str in sorted(list(unique_months_in_range)):
                if month_str >= today_year_month: continue
                for symbol in symbols:
                    symbol_extract_dir = EXTRACTED_DATA_DIR / symbol
                    for data_type in data_types_monthly:
                        if data_type == "fundingRate":
                            extract_sub_dir = symbol_extract_dir / data_type
                            file_name_base = f"{symbol}-{data_type}-{month_str}"
                            csv_filename = f"{file_name_base}.csv"
                            csv_path = extract_sub_dir / csv_filename
                            if (not csv_path.exists() or not verify_csv_file(csv_path)) and month_str not in processed_months_verification:
                                if csv_path.exists(): # Log if invalid
                                     logging.warning(f"Found invalid/empty monthly CSV during verification: {csv_path}")
                                processed_months_verification.add(month_str)
                                download_sub_dir = DOWNLOADS_DIR / symbol / data_type
                                zip_file_name = f"{file_name_base}.zip"
                                zip_path = download_sub_dir / zip_file_name
                                current_missing_details.append({
                                    'symbol': symbol, 'data_type': data_type, 'interval': 'N/A',
                                    'date_or_month': month_str, 'is_monthly': True,
                                    'expected_csv_path': str(csv_path), 'zip_exists': zip_path.exists(),
                                    'expected_zip_path': str(zip_path)
                                })
            # --- End of Verification Logic ---

            # Remove duplicates before processing retries
            # Convert list of dicts to list of tuples, use set for uniqueness, convert back
            unique_missing_tuples = set(tuple(sorted(d.items())) for d in current_missing_details)
            current_missing_details = [dict(t) for t in unique_missing_tuples]


            if not current_missing_details:
                logging.info(f"Verification Attempt {attempt + 1} PASSED. All expected files found and valid.")
                final_missing_details = []
                break # Exit retry loop

            logging.warning(f"Verification Attempt {attempt + 1} found {len(current_missing_details)} missing or invalid file(s).")
            final_missing_details = current_missing_details # Store the latest list for final report

            if attempt < MAX_VERIFICATION_RETRIES:
                logging.info(f"--- Attempting Redownload for {len(current_missing_details)} missing/invalid file(s) (Retry {attempt + 1}/{MAX_VERIFICATION_RETRIES}) ---")
                retry_tasks = []
                for missing_detail in current_missing_details:
                    # Reconstruct args for process_single_file
                    symbol = missing_detail['symbol']
                    data_type = missing_detail['data_type']
                    interval = missing_detail['interval'] # Will be klines_interval or 'N/A'
                    date_or_month_str = missing_detail['date_or_month']
                    is_monthly = missing_detail['is_monthly']

                    # Determine paths and URLs based on type and interval
                    if is_monthly:
                        file_name_base = f"{symbol}-{data_type}-{date_or_month_str}"
                        url_path_segment = f"monthly/{data_type}/{symbol}"
                        extract_sub_dir = EXTRACTED_DATA_DIR / symbol / data_type
                        download_sub_dir = DOWNLOADS_DIR / symbol / data_type
                    else: # Daily
                        extract_sub_dir = EXTRACTED_DATA_DIR / symbol / data_type
                        download_sub_dir = DOWNLOADS_DIR / symbol / data_type
                        if data_type in INTERVAL_TYPES:
                            file_name_base = f"{symbol}-{interval}-{date_or_month_str}" # Use interval from detail
                            url_path_segment = f"daily/{data_type}/{symbol}/{interval}" # Use interval from detail
                            extract_sub_dir = extract_sub_dir / interval
                            download_sub_dir = download_sub_dir / interval
                        elif data_type in ["bookDepth", "metrics", "trades"]:
                             file_name_base = f"{symbol}-{data_type}-{date_or_month_str}"
                             url_path_segment = f"daily/{data_type}/{symbol}"
                        elif data_type == "aggTrades":
                             file_name_base = f"{symbol}-{data_type}-{date_or_month_str}"
                             url_path_segment = f"daily/aggTrades/{symbol}"
                        else:
                             logging.error(f"Cannot reconstruct retry parameters for unknown daily type: {data_type}. Skipping retry.")
                             continue # Skip this file

                    zip_file_name = f"{file_name_base}.zip"
                    checksum_file_name = f"{zip_file_name}.CHECKSUM"
                    download_url = f"{BASE_URL}/{url_path_segment}/{zip_file_name}"
                    checksum_url = f"{BASE_URL}/{url_path_segment}/{checksum_file_name}"
                    zip_destination_path = download_sub_dir / zip_file_name

                    # Create task (process_single_file handles the check if CSV exists)
                    retry_tasks.append(asyncio.create_task(
                        process_single_file(session, download_url, checksum_url, zip_destination_path, extract_sub_dir)
                    ))

                if retry_tasks:
                    logging.info(f"Waiting for {len(retry_tasks)} redownload tasks...")
                    await asyncio.gather(*retry_tasks)
                    logging.info("Redownload tasks finished.")
                else:
                    logging.info("No tasks needed for redownload attempt.")
            # Implicitly loop to the next verification attempt

        # --- After the loop ---
        if final_missing_details:
            logging.warning("-----------------------------------------------------")
            logging.warning(f"Final Verification Result: {len(final_missing_details)} missing or invalid file(s) after {MAX_VERIFICATION_RETRIES + 1} attempt(s).")
            for detail in final_missing_details:
                log_msg = f"Missing/Invalid: {detail['expected_csv_path']}"
                if not detail['zip_exists']:
                    log_msg += f" (Note: Corresponding ZIP file {detail['expected_zip_path']} also not found in downloads)"
                logging.warning(f" - {log_msg}")
            logging.warning("-----------------------------------------------------")
            logging.warning("Potential Reasons: Data not available from Binance, persistent download/extraction error, or script interruption.")

            # Write final summary to CSV
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                summary_filename = REPORTS_DIR / f"binance_downloader_summary_{timestamp}.csv"
                REPORTS_DIR.mkdir(parents=True, exist_ok=True)

                if final_missing_details:
                     fieldnames = list(final_missing_details[0].keys())
                     with open(summary_filename, 'w', newline='', encoding='utf-8') as csvfile:
                         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                         writer.writeheader()
                         writer.writerows(final_missing_details)
                     logging.info(f"Missing/invalid files summary saved to: {summary_filename}")
            except Exception as e:
                logging.error(f"Failed to write final missing files summary CSV: {e}")
        else:
             logging.info("Final Verification Result: PASSED.")
             pass # Already logged success inside the loop

    logging.info("Data download and verification process finished.")


# --- Main Execution ---
if __name__ == "__main__":
    # --- Log Python Interpreter ---
    try:
        current_python = venv_utils.get_current_python_executable()
        logger.info(f"Running with Python interpreter: {current_python}")
    except Exception as e:
        logger.warning(f"Could not determine Python interpreter using venv_utils: {e}")

    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="Download Binance Futures market data.")
    parser.add_argument(
        '--symbols',
        type=str, # Expect comma-separated string
        default=None, # Default to None, will use DEFAULT_SYMBOLS later if None
        help='Comma-separated list of symbols (e.g., "BTCUSDT,BTCUSDC")'
    )
    parser.add_argument(
        '--data-types',
        type=str, # Expect comma-separated string
        default=None, # Default to None, will use defaults later if None
        help='Comma-separated list of data types (e.g., "klines,trades")'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        default=None, # Default to None, will use DEFAULT_START_DATE_STR later if None
        help='Start date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default=None, # Default to None, will use yesterday later if None
        help='End date (YYYY-MM-DD), defaults to yesterday'
    )
    parser.add_argument(
        '--interval',
        type=str,
        default=None, # Default to None, will use DEFAULT_KLINES_INTERVAL later if None
        help='Interval for applicable data types (e.g., "1m")'
    )

    args = parser.parse_args()

    # --- Process Arguments ---
    # Symbols
    if args.symbols:
        symbols_to_download = [s.strip().upper() for s in args.symbols.split(',') if s.strip()]
    else:
        symbols_to_download = DEFAULT_SYMBOLS
        logging.info(f"No symbols provided, using default: {symbols_to_download}")

    # Data Types
    if args.data_types:
        requested_types_list = [dt.strip() for dt in args.data_types.split(',') if dt.strip()]
        daily_types_to_download = [dt for dt in requested_types_list if dt in DEFAULT_DATA_TYPES_DAILY]
        monthly_types_to_download = [dt for dt in requested_types_list if dt in DEFAULT_DATA_TYPES_MONTHLY]
        requested_types_set = set(requested_types_list)
        recognized_types_set = set(daily_types_to_download + monthly_types_to_download)
        unrecognized = requested_types_set - recognized_types_set
        if unrecognized:
            logging.warning(f"Unrecognized or unsupported data types requested and will be ignored: {', '.join(unrecognized)}")
        if not daily_types_to_download and not monthly_types_to_download and recognized_types_set:
             logging.warning("No valid data types specified after filtering. Check available types.")
             # Decide if you want to exit or use defaults here. Using defaults for now.
             daily_types_to_download = DEFAULT_DATA_TYPES_DAILY
             monthly_types_to_download = DEFAULT_DATA_TYPES_MONTHLY
             logging.info(f"Falling back to default data types.")

    else:
        daily_types_to_download = DEFAULT_DATA_TYPES_DAILY
        monthly_types_to_download = DEFAULT_DATA_TYPES_MONTHLY
        logging.info(f"No data types provided, using defaults: Daily={daily_types_to_download}, Monthly={monthly_types_to_download}")


    # Start Date
    start_date_str = args.start_date if args.start_date else DEFAULT_START_DATE_STR
    try:
        start_dt = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    except ValueError:
        logging.error(f"Invalid start date format: {start_date_str}. Please use YYYY-MM-DD.")
        exit(1)

    # End Date
    end_date_str = args.end_date # Keep as None if not provided
    if end_date_str:
        try:
            end_dt = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        except ValueError:
            logging.error(f"Invalid end date format: {end_date_str}. Please use YYYY-MM-DD.")
            exit(1)
    else:
        end_dt = date.today() - timedelta(days=1) # Default to yesterday
        logging.info(f"No end date provided, using default: {end_dt.strftime('%Y-%m-%d')} (yesterday)")


    if end_dt < start_dt:
        logging.error(f"End date ({end_dt}) cannot be before start date ({start_dt}).")
        exit(1)

    # Interval
    klines_interval_to_use = args.interval if args.interval else DEFAULT_KLINES_INTERVAL
    if not args.interval:
         logging.info(f"No interval provided, using default: {klines_interval_to_use}")


    logging.info(f"Final symbols: {symbols_to_download}")
    logging.info(f"Final daily data types: {daily_types_to_download}")
    logging.info(f"Final monthly data types: {monthly_types_to_download}")
    logging.info(f"Final start date: {start_dt}")
    logging.info(f"Final end date: {end_dt}")
    logging.info(f"Final klines interval: {klines_interval_to_use}")

    # --- Run Async Download ---
    try:
        asyncio.run(download_data(
            symbols=symbols_to_download,
            data_types_daily=daily_types_to_download,
            data_types_monthly=monthly_types_to_download,
            klines_interval=klines_interval_to_use,
            start_date=start_dt,
            end_date=end_dt
        ))
    except KeyboardInterrupt:
        logging.info("Download process interrupted by user.")
    except Exception as e:
        logging.error(f"An unexpected error occurred in the main execution: {e}")
        import traceback # +++ Add traceback for debugging +++
        logging.error(traceback.format_exc()) # +++ Log traceback +++
    finally:
        logging.info("Script finished.")
        logging.shutdown()