#!/usr/bin/env python3
"""
Comprehensive test suite for enhanced Binance History Data Downloader features.

This test suite validates the new error handling, temporal gap detection,
and smart retry logic implemented in the enhanced downloader system.
"""

import pytest
import asyncio
import aiohttp
import json
import tempfile
import shutil
from pathlib import Path
from datetime import date, datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock
from typing import Dict, List

# Import the modules we're testing
from error_handling import (
    ErrorCategory, ErrorClassifier, ErrorInfo, SmartRetryHandler,
    CircuitBreaker, CircuitBreakerState, EnhancedErrorHandler,
    error_statistics
)
from temporal_gap_detector import (
    TemporalGapDetector, GapType, GapSeverity, GapInfo,
    MarketCalendar, FilePatternAnalyzer, analyze_temporal_gaps
)


class TestErrorClassifier:
    """Test the error classification system"""

    def test_classify_network_timeout(self):
        """Test classification of network timeout errors"""
        error = asyncio.TimeoutError("Request timed out")
        error_info = ErrorClassifier.classify_exception(error)

        assert error_info.category == ErrorCategory.NETWORK_TIMEOUT
        assert error_info.is_retryable == True
        assert error_info.suggested_delay == 5.0

    def test_classify_connection_error(self):
        """Test classification of connection errors"""
        error = aiohttp.ClientConnectionError("Connection refused")
        error_info = ErrorClassifier.classify_exception(error)

        assert error_info.category == ErrorCategory.CONNECTION_ERROR
        assert error_info.is_retryable == True
        assert error_info.suggested_delay == 2.0

    def test_classify_http_404(self):
        """Test classification of 404 errors (expected for missing data)"""
        error = aiohttp.ClientResponseError(
            request_info=Mock(),
            history=Mock(),
            status=404,
            message="Not Found"
        )
        error_info = ErrorClassifier.classify_exception(error)

        assert error_info.category == ErrorCategory.API_NOT_FOUND
        assert error_info.is_retryable == False
        assert error_info.suggested_delay == 0.0

    def test_classify_http_429(self):
        """Test classification of rate limiting errors"""
        error = aiohttp.ClientResponseError(
            request_info=Mock(),
            history=Mock(),
            status=429,
            message="Too Many Requests"
        )
        error_info = ErrorClassifier.classify_exception(error)

        assert error_info.category == ErrorCategory.API_RATE_LIMIT
        assert error_info.is_retryable == True
        assert error_info.suggested_delay == 30.0

    def test_classify_http_5xx(self):
        """Test classification of server errors"""
        error = aiohttp.ClientResponseError(
            request_info=Mock(),
            history=Mock(),
            status=503,
            message="Service Unavailable"
        )
        error_info = ErrorClassifier.classify_exception(error)

        assert error_info.category == ErrorCategory.API_SERVER_ERROR
        assert error_info.is_retryable == True
        assert error_info.suggested_delay == 10.0

    def test_classify_permission_error(self):
        """Test classification of file system permission errors"""
        error = PermissionError("Permission denied")
        error_info = ErrorClassifier.classify_exception(error)

        assert error_info.category == ErrorCategory.FS_PERMISSION_DENIED
        assert error_info.is_retryable == False

    def test_classify_memory_error(self):
        """Test classification of memory errors"""
        error = MemoryError("Out of memory")
        error_info = ErrorClassifier.classify_exception(error)

        assert error_info.category == ErrorCategory.MEMORY_ERROR
        assert error_info.is_retryable == True
        assert error_info.suggested_delay == 5.0

    def test_classify_unknown_error(self):
        """Test classification of unknown errors"""
        error = ValueError("Unknown error")
        error_info = ErrorClassifier.classify_exception(error)

        assert error_info.category == ErrorCategory.UNKNOWN_ERROR
        assert error_info.is_retryable == True


class TestSmartRetryHandler:
    """Test the smart retry handling logic"""

    @pytest.fixture
    def retry_handler(self):
        return SmartRetryHandler(max_retries=3, base_delay=1.0, max_delay=60.0)

    @pytest.mark.asyncio
    async def test_successful_operation_no_retry(self, retry_handler):
        """Test that successful operations don't retry"""
        mock_operation = AsyncMock(return_value="success")

        result = await retry_handler.retry_with_backoff(
            operation=mock_operation,
            context={"operation": "test"}
        )

        assert result == "success"
        assert mock_operation.call_count == 1

    @pytest.mark.asyncio
    async def test_operation_with_retries(self, retry_handler):
        """Test operation that succeeds after retries"""
        mock_operation = AsyncMock()
        # Fail first two attempts, succeed on third
        mock_operation.side_effect = [
            aiohttp.ClientConnectionError("Connection failed"),
            aiohttp.ClientConnectionError("Connection failed"),
            "success"
        ]

        result = await retry_handler.retry_with_backoff(
            operation=mock_operation,
            context={"operation": "test"}
        )

        assert result == "success"
        assert mock_operation.call_count == 3

    @pytest.mark.asyncio
    async def test_non_retryable_error(self, retry_handler):
        """Test that non-retryable errors don't retry"""
        error = aiohttp.ClientResponseError(
            request_info=Mock(),
            history=Mock(),
            status=404,
            message="Not Found"
        )
        mock_operation = AsyncMock(side_effect=error)

        with pytest.raises(aiohttp.ClientResponseError):
            await retry_handler.retry_with_backoff(
                operation=mock_operation,
                context={"operation": "test"}
            )

        assert mock_operation.call_count == 1

    @pytest.mark.asyncio
    async def test_max_retries_exceeded(self, retry_handler):
        """Test behavior when max retries are exceeded"""
        error = aiohttp.ClientConnectionError("Connection failed")
        mock_operation = AsyncMock(side_effect=error)

        with pytest.raises(aiohttp.ClientConnectionError):
            await retry_handler.retry_with_backoff(
                operation=mock_operation,
                context={"operation": "test"}
            )

        assert mock_operation.call_count == 4  # initial + 3 retries

    def test_calculate_delay_exponential_backoff(self, retry_handler):
        """Test exponential backoff delay calculation"""
        error_info = ErrorInfo(
            category=ErrorCategory.CONNECTION_ERROR,
            message="Connection failed",
            timestamp=datetime.now(),
            context={},
            suggested_delay=2.0
        )

        # Test exponential progression
        delay0 = retry_handler.calculate_delay(error_info, 0)
        delay1 = retry_handler.calculate_delay(error_info, 1)
        delay2 = retry_handler.calculate_delay(error_info, 2)

        # Should increase exponentially (with jitter)
        assert 1.5 <= delay0 <= 2.5  # 2.0 ± 25%
        assert 3.0 <= delay1 <= 5.0  # 4.0 ± 25%
        assert 6.0 <= delay2 <= 10.0  # 8.0 ± 25%

    def test_calculate_delay_max_cap(self, retry_handler):
        """Test that delay is capped at maximum"""
        error_info = ErrorInfo(
            category=ErrorCategory.CONNECTION_ERROR,
            message="Connection failed",
            timestamp=datetime.now(),
            context={},
            suggested_delay=30.0
        )

        # High attempt number should be capped
        delay = retry_handler.calculate_delay(error_info, 10)
        assert delay <= retry_handler.max_delay


class TestCircuitBreaker:
    """Test the circuit breaker functionality"""

    @pytest.fixture
    def circuit_breaker(self):
        return CircuitBreaker(failure_threshold=3, recovery_timeout=30)

    def test_initial_state_closed(self, circuit_breaker):
        """Test circuit breaker starts in closed state"""
        assert circuit_breaker.stats.state == CircuitBreakerState.CLOSED
        assert circuit_breaker.should_allow_request() == True

    def test_failure_threshold_opens_circuit(self, circuit_breaker):
        """Test that failures open the circuit"""
        # Record failures up to threshold
        for _ in range(3):
            circuit_breaker.record_failure()

        assert circuit_breaker.stats.state == CircuitBreakerState.OPEN
        assert circuit_breaker.should_allow_request() == False

    def test_success_resets_failure_count(self, circuit_breaker):
        """Test that successes reset failure count"""
        # Record some failures
        circuit_breaker.record_failure()
        circuit_breaker.record_failure()
        assert circuit_breaker.stats.failure_count == 2

        # Record success
        circuit_breaker.record_success()
        assert circuit_breaker.stats.failure_count == 1  # Reduced by 1

    def test_recovery_timeout_transitions_to_half_open(self, circuit_breaker):
        """Test recovery timeout behavior"""
        # Open the circuit
        for _ in range(3):
            circuit_breaker.record_failure()
        assert circuit_breaker.stats.state == CircuitBreakerState.OPEN

        # Simulate time passage
        circuit_breaker.stats.last_failure_time = datetime.now() - timedelta(seconds=35)

        # Should transition to half-open
        assert circuit_breaker.should_allow_request() == True
        assert circuit_breaker.stats.state == CircuitBreakerState.HALF_OPEN

    def test_half_open_success_closes_circuit(self, circuit_breaker):
        """Test that successes in half-open state close the circuit"""
        # Set to half-open state
        circuit_breaker.stats.state = CircuitBreakerState.HALF_OPEN

        # Record enough successes
        for _ in range(3):
            circuit_breaker.record_success()

        assert circuit_breaker.stats.state == CircuitBreakerState.CLOSED
        assert circuit_breaker.stats.failure_count == 0

    def test_half_open_failure_reopens_circuit(self, circuit_breaker):
        """Test that failures in half-open state reopen the circuit"""
        # Set to half-open state
        circuit_breaker.stats.state = CircuitBreakerState.HALF_OPEN

        # Record failure
        circuit_breaker.record_failure()

        assert circuit_breaker.stats.state == CircuitBreakerState.OPEN


class TestMarketCalendar:
    """Test market calendar functionality"""

    def test_is_weekend(self):
        """Test weekend detection"""
        # Saturday
        saturday = date(2024, 1, 6)
        assert MarketCalendar.is_weekend(saturday) == True

        # Sunday
        sunday = date(2024, 1, 7)
        assert MarketCalendar.is_weekend(sunday) == True

        # Monday (weekday)
        monday = date(2024, 1, 8)
        assert MarketCalendar.is_weekend(monday) == False

    def test_expected_gap_type_crypto_no_weekend_gaps(self):
        """Test that crypto markets don't have expected weekend gaps"""
        saturday = date(2024, 1, 6)
        sunday = date(2024, 1, 7)

        # Crypto markets are 24/7 - no expected gaps for ANY data type on weekends
        for data_type in ['klines', 'trades', 'metrics', 'fundingRate', 'aggTrades']:
            assert MarketCalendar.get_expected_gap_type(saturday, data_type) is None
            assert MarketCalendar.get_expected_gap_type(sunday, data_type) is None

    def test_expected_gap_type_early_dates(self):
        """Test gap type for very early dates (before Binance launch)"""
        early_date = date(2017, 1, 1)  # Before Binance launch
        gap_type = MarketCalendar.get_expected_gap_type(early_date, 'klines')

        assert gap_type == GapType.API_LIMITATION

    def test_expected_gap_type_future_dates(self):
        """Test gap type for future dates"""
        from datetime import timedelta
        future_date = date.today() + timedelta(days=30)
        gap_type = MarketCalendar.get_expected_gap_type(future_date, 'klines')

        assert gap_type == GapType.API_LIMITATION


class TestFilePatternAnalyzer:
    """Test file pattern analysis functionality"""

    @pytest.fixture
    def temp_data_dir(self):
        """Create temporary directory structure for testing"""
        temp_dir = Path(tempfile.mkdtemp())
        data_dir = temp_dir / "data"
        downloads_dir = temp_dir / "downloads"

        # Create test file structure
        daily_dir = data_dir / "daily" / "klines" / "BTCUSDT" / "1m"
        daily_dir.mkdir(parents=True, exist_ok=True)

        monthly_dir = data_dir / "monthly" / "fundingRate" / "BTCUSDT"
        monthly_dir.mkdir(parents=True, exist_ok=True)

        # Create sample files
        (daily_dir / "BTCUSDT-1m-2024-01-15.csv").touch()
        (daily_dir / "BTCUSDT-1m-2024-01-16.csv").touch()
        (daily_dir / "BTCUSDT-1m-2024-01-18.csv").touch()  # Gap on 17th

        (monthly_dir / "BTCUSDT-fundingRate-2024-01.csv").touch()
        (monthly_dir / "BTCUSDT-fundingRate-2024-03.csv").touch()  # Gap in February

        yield data_dir, downloads_dir

        # Cleanup
        shutil.rmtree(temp_dir)

    def test_get_existing_files_daily(self, temp_data_dir):
        """Test getting existing daily files"""
        data_dir, downloads_dir = temp_data_dir
        analyzer = FilePatternAnalyzer(data_dir, downloads_dir)

        existing_files = analyzer.get_existing_files("BTCUSDT", "klines", "1m")

        assert len(existing_files) == 3
        assert date(2024, 1, 15) in existing_files
        assert date(2024, 1, 16) in existing_files
        assert date(2024, 1, 18) in existing_files
        assert date(2024, 1, 17) not in existing_files  # Missing date

    def test_get_existing_files_monthly(self, temp_data_dir):
        """Test getting existing monthly files"""
        data_dir, downloads_dir = temp_data_dir
        analyzer = FilePatternAnalyzer(data_dir, downloads_dir)

        existing_files = analyzer.get_existing_files("BTCUSDT", "fundingRate")

        assert len(existing_files) == 2
        assert date(2024, 1, 1) in existing_files
        assert date(2024, 3, 1) in existing_files
        assert date(2024, 2, 1) not in existing_files  # Missing month

    def test_generate_expected_dates_daily(self, temp_data_dir):
        """Test generating expected dates for daily data"""
        data_dir, downloads_dir = temp_data_dir
        analyzer = FilePatternAnalyzer(data_dir, downloads_dir)

        start_date = date(2024, 1, 15)
        end_date = date(2024, 1, 17)
        expected_dates = analyzer.generate_expected_dates(start_date, end_date, "klines")

        assert len(expected_dates) == 3
        assert expected_dates == [date(2024, 1, 15), date(2024, 1, 16), date(2024, 1, 17)]

    def test_generate_expected_dates_monthly(self, temp_data_dir):
        """Test generating expected dates for monthly data"""
        data_dir, downloads_dir = temp_data_dir
        analyzer = FilePatternAnalyzer(data_dir, downloads_dir)

        start_date = date(2024, 1, 15)
        end_date = date(2024, 3, 20)
        expected_dates = analyzer.generate_expected_dates(start_date, end_date, "fundingRate")

        assert len(expected_dates) == 3
        assert expected_dates == [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)]

    def test_extract_date_from_daily_filename(self, temp_data_dir):
        """Test date extraction from daily filenames"""
        data_dir, downloads_dir = temp_data_dir
        analyzer = FilePatternAnalyzer(data_dir, downloads_dir)

        # Test klines with interval
        date_result = analyzer._extract_date_from_filename("BTCUSDT-1m-2024-01-15.csv", "klines")
        assert date_result == date(2024, 1, 15)

        # Test non-interval data type
        date_result = analyzer._extract_date_from_filename("BTCUSDT-trades-2024-01-15.csv", "trades")
        assert date_result == date(2024, 1, 15)

    def test_extract_date_from_monthly_filename(self, temp_data_dir):
        """Test date extraction from monthly filenames"""
        data_dir, downloads_dir = temp_data_dir
        analyzer = FilePatternAnalyzer(data_dir, downloads_dir)

        date_result = analyzer._extract_date_from_filename("BTCUSDT-fundingRate-2024-01.csv", "fundingRate")
        assert date_result == date(2024, 1, 1)


class TestTemporalGapDetector:
    """Test temporal gap detection functionality"""

    @pytest.fixture
    def temp_data_dir_with_gaps(self):
        """Create temporary directory with intentional gaps for testing"""
        temp_dir = Path(tempfile.mkdtemp())
        data_dir = temp_dir / "data"
        downloads_dir = temp_dir / "downloads"

        # Create test file structure with gaps
        daily_dir = data_dir / "daily" / "klines" / "BTCUSDT" / "1m"
        daily_dir.mkdir(parents=True, exist_ok=True)

        # Create files with gaps: missing Jan 17 and Jan 20-22
        test_dates = [
            "2024-01-15", "2024-01-16", "2024-01-18", "2024-01-19", "2024-01-23", "2024-01-24"
        ]

        for date_str in test_dates:
            (daily_dir / f"BTCUSDT-1m-{date_str}.csv").touch()

        yield data_dir, downloads_dir

        # Cleanup
        shutil.rmtree(temp_dir)

    def test_analyze_symbol_timeline(self, temp_data_dir_with_gaps):
        """Test complete timeline analysis"""
        data_dir, downloads_dir = temp_data_dir_with_gaps
        detector = TemporalGapDetector(data_dir, downloads_dir)

        result = detector.analyze_symbol_timeline(
            symbol="BTCUSDT",
            data_type="klines",
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 24),
            interval="1m"
        )

        assert result.total_expected_files == 10  # 10 days total
        assert result.total_existing_files == 6   # 6 files exist
        assert result.total_missing_files == 4    # 4 files missing
        assert result.completeness_percentage == 60.0

        # Should detect 2 gaps: single day (17th) and multi-day (20-22)
        assert len(result.gaps) == 2

        # Check gap details
        gaps_by_duration = {gap.duration_days: gap for gap in result.gaps}
        assert 1 in gaps_by_duration  # Single day gap
        assert 3 in gaps_by_duration  # Three day gap

    def test_gap_severity_assessment(self, temp_data_dir_with_gaps):
        """Test gap severity assessment"""
        data_dir, downloads_dir = temp_data_dir_with_gaps
        detector = TemporalGapDetector(data_dir, downloads_dir)

        # Create gap info for testing
        short_gap = GapInfo(
            gap_type=GapType.MISSING_DATA,
            severity=GapSeverity.MEDIUM,  # Will be overridden by assessment
            start_date=date(2024, 1, 17),
            end_date=date(2024, 1, 17),
            duration_days=1,
            symbol="BTCUSDT",
            data_type="klines",
            interval="1m",
            expected_files=["BTCUSDT-1m-2024-01-17.csv"],
            missing_files=["BTCUSDT-1m-2024-01-17.csv"],
            context={},
            detected_at=datetime.now()
        )

        # Test severity assessment
        severity = detector._assess_gap_severity(GapType.MISSING_DATA, 1, 1)
        assert severity == GapSeverity.MEDIUM

        severity = detector._assess_gap_severity(GapType.MISSING_DATA, 3, 3)
        assert severity == GapSeverity.HIGH

        severity = detector._assess_gap_severity(GapType.MISSING_DATA, 8, 8)
        assert severity == GapSeverity.CRITICAL

        # Expected gaps should be low severity
        severity = detector._assess_gap_severity(GapType.WEEKEND_GAP, 2, 2)
        assert severity == GapSeverity.INFO

    def test_group_consecutive_dates(self, temp_data_dir_with_gaps):
        """Test grouping of consecutive missing dates"""
        data_dir, downloads_dir = temp_data_dir_with_gaps
        detector = TemporalGapDetector(data_dir, downloads_dir)

        missing_dates = [
            date(2024, 1, 17),      # Single gap
            date(2024, 1, 20),      # Start of consecutive gap
            date(2024, 1, 21),      # Middle of consecutive gap
            date(2024, 1, 22),      # End of consecutive gap
        ]

        groups = detector._group_consecutive_dates(missing_dates)

        assert len(groups) == 2
        assert groups[0] == [date(2024, 1, 17)]  # Single date group
        assert groups[1] == [date(2024, 1, 20), date(2024, 1, 21), date(2024, 1, 22)]  # Consecutive group

    def test_gap_classification(self, temp_data_dir_with_gaps):
        """Test gap type classification"""
        data_dir, downloads_dir = temp_data_dir_with_gaps
        detector = TemporalGapDetector(data_dir, downloads_dir)

        # Test classification of different gap scenarios (crypto 24/7 markets)
        weekday_gap = [date(2024, 1, 17)]  # Wednesday
        gap_type = detector._classify_gap_type(weekday_gap, "klines")
        assert gap_type == GapType.MISSING_DATA

        weekend_gap = [date(2024, 1, 20), date(2024, 1, 21)]  # Saturday, Sunday
        gap_type = detector._classify_gap_type(weekend_gap, "klines")
        # For crypto markets, weekend gaps are MISSING_DATA, not expected gaps
        assert gap_type == GapType.MISSING_DATA

        # Test early date classification
        early_gap = [date(2017, 1, 15)]  # Before Binance launch
        gap_type = detector._classify_gap_type(early_gap, "klines")
        assert gap_type == GapType.API_LIMITATION

    def test_generate_expected_filenames(self, temp_data_dir_with_gaps):
        """Test generation of expected filenames"""
        data_dir, downloads_dir = temp_data_dir_with_gaps
        detector = TemporalGapDetector(data_dir, downloads_dir)

        dates = [date(2024, 1, 17), date(2024, 1, 18)]

        # Test daily with interval
        filenames = detector._generate_expected_filenames(dates, "BTCUSDT", "klines", "1m")
        expected = ["BTCUSDT-1m-2024-01-17.csv", "BTCUSDT-1m-2024-01-18.csv"]
        assert filenames == expected

        # Test monthly
        monthly_dates = [date(2024, 1, 1), date(2024, 2, 1)]
        filenames = detector._generate_expected_filenames(monthly_dates, "BTCUSDT", "fundingRate", None)
        expected = ["BTCUSDT-fundingRate-2024-01.csv", "BTCUSDT-fundingRate-2024-02.csv"]
        assert filenames == expected


class TestIntegration:
    """Integration tests for the complete enhanced system"""

    @pytest.mark.asyncio
    async def test_enhanced_error_handler_integration(self):
        """Test complete error handler integration"""
        error_handler = EnhancedErrorHandler(max_retries=2, circuit_breaker_threshold=3)

        # Mock operation that fails then succeeds
        call_count = 0
        async def mock_operation():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise aiohttp.ClientConnectionError("Connection failed")
            return "success"

        # Should succeed after retry
        result = await error_handler.execute_with_protection(
            operation=mock_operation,
            endpoint_key="test_endpoint",
            context={"operation": "test"}
        )

        assert result == "success"
        assert call_count == 2

    def test_analyze_temporal_gaps_convenience_function(self):
        """Test the convenience function for gap analysis"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            data_dir = temp_path / "data"
            downloads_dir = temp_path / "downloads"

            # Create minimal file structure
            daily_dir = data_dir / "daily" / "klines" / "BTCUSDT" / "1m"
            daily_dir.mkdir(parents=True, exist_ok=True)
            (daily_dir / "BTCUSDT-1m-2024-01-15.csv").touch()

            # Test the convenience function
            results = analyze_temporal_gaps(
                symbols=["BTCUSDT"],
                data_types=["klines"],
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 16),
                data_dir=data_dir,
                downloads_dir=downloads_dir,
                intervals={"klines": "1m"}
            )

            assert len(results) == 1
            assert results[0].symbol == "BTCUSDT"
            assert results[0].data_type == "klines"
            assert results[0].interval == "1m"


if __name__ == "__main__":
    # Run the test suite
    pytest.main([__file__, "-v", "--tb=short"])