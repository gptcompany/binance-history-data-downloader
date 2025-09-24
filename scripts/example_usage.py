#!/usr/bin/env python3
"""
Example usage of enhanced Binance History Data Downloader features.

This script demonstrates how to use the new error handling,
temporal gap detection, and smart retry capabilities.
"""

import asyncio
import logging
from pathlib import Path
from datetime import date, datetime, timedelta

from error_handling import (
    EnhancedErrorHandler, ErrorClassifier, error_statistics,
    ErrorCategory, CircuitBreaker
)
from temporal_gap_detector import (
    TemporalGapDetector, analyze_temporal_gaps,
    GapType, GapSeverity
)


async def example_error_handling():
    """Demonstrate enhanced error handling capabilities"""
    print("=" * 60)
    print("EXAMPLE 1: Enhanced Error Handling")
    print("=" * 60)

    error_handler = EnhancedErrorHandler(max_retries=3, circuit_breaker_threshold=5)

    # Simulate a flaky operation that succeeds after retries
    call_count = 0
    async def flaky_operation():
        nonlocal call_count
        call_count += 1
        print(f"  Attempt {call_count}: Simulating network operation...")

        if call_count <= 2:
            # Simulate different types of failures
            import aiohttp
            if call_count == 1:
                raise aiohttp.ClientConnectionError("Connection refused")
            else:
                raise asyncio.TimeoutError("Request timed out")

        print("  Operation succeeded!")
        return "Data downloaded successfully"

    try:
        result = await error_handler.execute_with_protection(
            operation=flaky_operation,
            endpoint_key="example_api",
            context={"operation": "download", "url": "https://example.com/data.zip"}
        )
        print(f"✅ Final result: {result}")
    except Exception as e:
        print(f"❌ Operation failed: {e}")

    # Show error statistics
    stats = error_statistics.get_error_summary()
    print(f"\n📊 Error Statistics:")
    print(f"   Total errors: {stats['total_errors']}")
    for category, count in stats['error_categories'].items():
        if count > 0:
            print(f"   {category}: {count}")


def example_circuit_breaker():
    """Demonstrate circuit breaker functionality"""
    print("\n" + "=" * 60)
    print("EXAMPLE 2: Circuit Breaker Pattern")
    print("=" * 60)

    circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=5)

    print("Initial state:")
    print(f"  Circuit state: {circuit_breaker.stats.state.value}")
    print(f"  Should allow request: {circuit_breaker.should_allow_request()}")

    print("\nSimulating failures:")
    for i in range(5):
        circuit_breaker.record_failure()
        print(f"  Failure {i+1}: State = {circuit_breaker.stats.state.value}, "
              f"Allow requests = {circuit_breaker.should_allow_request()}")

    print("\nSimulating recovery after timeout...")
    # Simulate time passage
    circuit_breaker.stats.last_failure_time = datetime.now() - timedelta(seconds=6)

    print(f"  After timeout: State = {circuit_breaker.stats.state.value}, "
          f"Allow requests = {circuit_breaker.should_allow_request()}")

    print("\nSimulating successful recovery:")
    for i in range(3):
        circuit_breaker.record_success()
        print(f"  Success {i+1}: State = {circuit_breaker.stats.state.value}")


def example_temporal_gap_detection():
    """Demonstrate temporal gap detection"""
    print("\n" + "=" * 60)
    print("EXAMPLE 3: Temporal Gap Detection")
    print("=" * 60)

    # Create temporary data structure for demonstration
    import tempfile
    import shutil

    with tempfile.TemporaryDirectory() as temp_dir:
        data_dir = Path(temp_dir) / "data"
        downloads_dir = Path(temp_dir) / "downloads"

        # Create sample data with intentional gaps
        print("Creating sample data structure with gaps...")

        # Daily klines data with gaps
        daily_dir = data_dir / "daily" / "klines" / "BTCUSDT" / "1m"
        daily_dir.mkdir(parents=True, exist_ok=True)

        # Create files for dates with intentional gaps
        sample_dates = [
            "2024-01-15", "2024-01-16",  # Continuous
            # Gap: 2024-01-17 missing
            "2024-01-18", "2024-01-19",  # Continuous
            # Gap: 2024-01-20, 2024-01-21, 2024-01-22 missing (weekend + Monday)
            "2024-01-23", "2024-01-24"   # Continuous
        ]

        for date_str in sample_dates:
            (daily_dir / f"BTCUSDT-1m-{date_str}.csv").write_text("timestamp,open,high,low,close,volume\n1234567890,100,105,95,102,1000\n")

        print(f"  Created {len(sample_dates)} sample files")

        # Run gap analysis
        detector = TemporalGapDetector(data_dir, downloads_dir)

        analysis_result = detector.analyze_symbol_timeline(
            symbol="BTCUSDT",
            data_type="klines",
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 24),
            interval="1m"
        )

        print(f"\n📊 Timeline Analysis Results:")
        print(f"   Symbol: {analysis_result.symbol}")
        print(f"   Data type: {analysis_result.data_type}")
        print(f"   Interval: {analysis_result.interval}")
        print(f"   Analysis period: {analysis_result.analysis_period_start} to {analysis_result.analysis_period_end}")
        print(f"   Completeness: {analysis_result.completeness_percentage:.1f}%")
        print(f"   Expected files: {analysis_result.total_expected_files}")
        print(f"   Existing files: {analysis_result.total_existing_files}")
        print(f"   Missing files: {analysis_result.total_missing_files}")

        print(f"\n🔍 Detected Gaps ({len(analysis_result.gaps)} total):")
        for i, gap in enumerate(analysis_result.gaps, 1):
            print(f"   Gap {i}:")
            print(f"      Type: {gap.gap_type.value}")
            print(f"      Severity: {gap.severity.value}")
            print(f"      Period: {gap.start_date} to {gap.end_date} ({gap.duration_days} days)")
            print(f"      Missing files: {len(gap.missing_files)}")

        print(f"\n💡 Recommendations ({len(analysis_result.recommendations)} total):")
        for i, recommendation in enumerate(analysis_result.recommendations, 1):
            print(f"   {i}. {recommendation}")


def example_convenience_functions():
    """Demonstrate convenience functions for batch analysis"""
    print("\n" + "=" * 60)
    print("EXAMPLE 4: Batch Gap Analysis")
    print("=" * 60)

    import tempfile

    with tempfile.TemporaryDirectory() as temp_dir:
        data_dir = Path(temp_dir) / "data"
        downloads_dir = Path(temp_dir) / "downloads"

        # Create minimal sample data for multiple symbols and data types
        symbols_data = {
            "BTCUSDT": ["klines", "trades"],
            "ETHUSDT": ["klines", "aggTrades"]
        }

        print("Creating sample data for multiple symbols...")

        for symbol, data_types in symbols_data.items():
            for data_type in data_types:
                if data_type == "klines":
                    # Create klines data with interval
                    dir_path = data_dir / "daily" / data_type / symbol / "1m"
                    dir_path.mkdir(parents=True, exist_ok=True)

                    # Create files with some gaps
                    dates = ["2024-01-15", "2024-01-16", "2024-01-18", "2024-01-19"]  # Missing 17th
                    for date_str in dates:
                        (dir_path / f"{symbol}-1m-{date_str}.csv").touch()
                else:
                    # Create other data types without interval
                    dir_path = data_dir / "daily" / data_type / symbol
                    dir_path.mkdir(parents=True, exist_ok=True)

                    # Create files with different gap patterns
                    dates = ["2024-01-15", "2024-01-17", "2024-01-19"]  # Gaps on even days
                    for date_str in dates:
                        (dir_path / f"{symbol}-{data_type}-{date_str}.csv").touch()

        print(f"  Created sample data for {len(symbols_data)} symbols")

        # Run batch analysis using convenience function
        results = analyze_temporal_gaps(
            symbols=list(symbols_data.keys()),
            data_types=["klines", "trades", "aggTrades"],
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 19),
            data_dir=data_dir,
            downloads_dir=downloads_dir,
            intervals={"klines": "1m"}  # Only klines needs interval
        )

        print(f"\n📊 Batch Analysis Results ({len(results)} combinations):")
        for result in results:
            status = "✅" if result.completeness_percentage >= 80 else "⚠️" if result.completeness_percentage >= 50 else "❌"
            print(f"   {status} {result.symbol} {result.data_type} {result.interval or ''}: "
                  f"{result.completeness_percentage:.1f}% complete "
                  f"({result.total_existing_files}/{result.total_expected_files} files)")

            if result.completeness_percentage < 80:
                critical_gaps = [g for g in result.gaps if g.severity in [GapSeverity.CRITICAL, GapSeverity.HIGH]]
                if critical_gaps:
                    print(f"      🚨 {len(critical_gaps)} high-priority gaps detected")


async def main():
    """Main example runner"""
    print("🚀 Enhanced Binance Data Downloader - Feature Examples")
    print("=" * 60)

    # Set up logging for examples
    logging.basicConfig(
        level=logging.WARNING,  # Reduce noise for examples
        format='%(levelname)s: %(message)s'
    )

    try:
        # Run all examples
        await example_error_handling()
        example_circuit_breaker()
        example_temporal_gap_detection()
        example_convenience_functions()

        print("\n" + "=" * 60)
        print("✅ All examples completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Example failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())