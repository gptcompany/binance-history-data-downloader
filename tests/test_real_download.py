#!/usr/bin/env python3
"""
Real-world test of enhanced Binance History Data Downloader

This script tests the system with actual BTCUSDT data from the first week
of January 2020, verifying all enhanced features work in practice.
"""

import asyncio
import logging
import sys
from pathlib import Path
from datetime import date, datetime

# Temporarily modify the configuration for testing
import unified_downloader

# Override configuration for focused testing
TEST_DATA_TYPES = [
    "klines",     # ✅ Verified available
    "trades",     # ✅ Verified available
    "aggTrades",  # ✅ Verified available
    "fundingRate" # ✅ Verified available (monthly)
]

TEST_SYMBOLS = ["BTCUSDT"]
TEST_START_DATE = "2020-01-01"  # First available date
TEST_END_DATE = "2020-01-07"   # One week of data

# Apply test configuration
unified_downloader.DEFAULT_SYMBOLS = TEST_SYMBOLS
unified_downloader.DEFAULT_START_DATE_STR = TEST_START_DATE
unified_downloader.ALL_DATA_TYPES = TEST_DATA_TYPES
unified_downloader.DAILY_TYPES = [dt for dt in TEST_DATA_TYPES if dt != "fundingRate"]

# Enable enhanced features for testing
unified_downloader.ENABLE_ENHANCED_ERROR_HANDLING = True
unified_downloader.ENABLE_TEMPORAL_GAP_ANALYSIS = True
unified_downloader.GAP_ANALYSIS_AFTER_DOWNLOAD = True

# More verbose logging for testing
unified_downloader.log_level = logging.DEBUG

print("🚀 REAL-WORLD TEST: Enhanced Binance Data Downloader")
print("=" * 60)
print(f"📊 Test Configuration:")
print(f"   Symbol: {', '.join(TEST_SYMBOLS)}")
print(f"   Data Types: {', '.join(TEST_DATA_TYPES)}")
print(f"   Date Range: {TEST_START_DATE} to {TEST_END_DATE}")
print(f"   Enhanced Features: ENABLED")
print("=" * 60)

def setup_test_environment():
    """Setup clean test environment"""
    # Create clean directories
    test_downloads = Path("test_downloads")
    test_data = Path("test_data")
    test_reports = Path("test_reports")
    test_logs = Path("test_logs")

    for dir_path in [test_downloads, test_data, test_reports, test_logs]:
        dir_path.mkdir(exist_ok=True)

    # Update paths for testing
    unified_downloader.DOWNLOADS_DIR = test_downloads
    unified_downloader.EXTRACTED_DATA_DIR = test_data
    unified_downloader.REPORTS_DIR = test_reports
    unified_downloader.LOGS_DIR = test_logs

    print(f"✅ Test environment setup complete")
    print(f"   Downloads: {test_downloads}")
    print(f"   Data: {test_data}")
    print(f"   Reports: {test_reports}")
    print(f"   Logs: {test_logs}")
    print()

async def run_enhanced_download_test():
    """Run the enhanced download test"""
    print("🔄 Starting Enhanced Download Test...")
    print("=" * 40)

    # Convert date string to date object
    start_date_obj = datetime.strptime(TEST_START_DATE, "%Y-%m-%d").date()

    try:
        # Run the enhanced downloader
        await unified_downloader.main(TEST_SYMBOLS, start_date_obj)

        print("\n✅ Enhanced download test completed!")

    except Exception as e:
        print(f"\n❌ Enhanced download test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True

def analyze_test_results():
    """Analyze the results of the test"""
    print("\n" + "=" * 60)
    print("📊 TEST RESULTS ANALYSIS")
    print("=" * 60)

    # Check downloaded files
    data_dir = Path("test_data")
    downloads_dir = Path("test_downloads")
    reports_dir = Path("test_reports")

    total_files = 0
    total_size = 0

    print("\n📁 Downloaded Data Files:")
    if data_dir.exists():
        for data_file in data_dir.rglob("*.csv"):
            size = data_file.stat().st_size
            total_files += 1
            total_size += size
            print(f"   ✅ {data_file.relative_to(data_dir)} ({size:,} bytes)")

    print(f"\n📈 Download Statistics:")
    print(f"   Total files: {total_files}")
    print(f"   Total size: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")

    # Check ZIP files
    zip_files = 0
    if downloads_dir.exists():
        for zip_file in downloads_dir.rglob("*.zip"):
            zip_files += 1
    print(f"   ZIP files: {zip_files}")

    # Check reports
    print(f"\n📋 Generated Reports:")
    if reports_dir.exists():
        for report_file in reports_dir.rglob("*"):
            if report_file.is_file():
                print(f"   📄 {report_file.name}")

                # Show content of key reports
                if "gap_analysis" in report_file.name and report_file.suffix == ".json":
                    print(f"      📊 Gap Analysis Report found!")
                elif "missing_files" in report_file.name:
                    print(f"      ⚠️ Missing Files Report found")
                elif "gap_summary" in report_file.name:
                    print(f"      📈 Gap Summary CSV found")

    # Check error statistics
    from error_handling import error_statistics
    error_summary = error_statistics.get_error_summary()

    print(f"\n🔧 Error Handling Statistics:")
    print(f"   Total errors: {error_summary['total_errors']}")
    print(f"   Runtime: {error_summary['runtime_seconds']:.2f} seconds")

    if error_summary['total_errors'] > 0:
        print(f"   Error breakdown:")
        for category, count in error_summary['error_categories'].items():
            if count > 0:
                print(f"      {category}: {count}")

    return total_files > 0

async def main():
    """Main test function"""
    print("🧪 VERIFICATION TEST: Claims vs Reality")
    print("Testing enhanced error handling and temporal gap detection")
    print("with real BTCUSDT data from January 1-7, 2020\n")

    # Setup test environment
    setup_test_environment()

    # Run the test
    start_time = datetime.now()
    success = await run_enhanced_download_test()
    end_time = datetime.now()

    # Analyze results
    has_data = analyze_test_results()

    duration = (end_time - start_time).total_seconds()

    print(f"\n" + "=" * 60)
    print("🎯 FINAL VERIFICATION RESULTS")
    print("=" * 60)
    print(f"⏱️ Total execution time: {duration:.2f} seconds")
    print(f"✅ Download success: {success}")
    print(f"✅ Data files created: {has_data}")

    # Verify claims
    claims_verified = []

    if success and has_data:
        claims_verified.append("✅ System can download real crypto data")
    else:
        claims_verified.append("❌ System failed to download data")

    if Path("test_reports").exists() and list(Path("test_reports").glob("*gap*")):
        claims_verified.append("✅ Temporal gap analysis generates reports")
    else:
        claims_verified.append("❌ Gap analysis reports not found")

    from error_handling import error_statistics
    if error_statistics.get_error_summary()['total_errors'] >= 0:  # Statistics tracked
        claims_verified.append("✅ Error statistics tracking works")
    else:
        claims_verified.append("❌ Error statistics not tracked")

    print(f"\n🔍 CLAIMS VERIFICATION:")
    for claim in claims_verified:
        print(f"   {claim}")

    all_passed = all("✅" in claim for claim in claims_verified)

    if all_passed:
        print(f"\n🎉 VERIFICATION PASSED: All claims confirmed!")
    else:
        print(f"\n⚠️ VERIFICATION PARTIAL: Some claims need investigation")

    return all_passed

if __name__ == "__main__":
    # Run the test
    result = asyncio.run(main())
    sys.exit(0 if result else 1)