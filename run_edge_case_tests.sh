#!/bin/bash

# =============================================================================
# Binance Data Downloader - Edge Cases Test Suite
# =============================================================================
# Comprehensive test suite for edge cases with 60-second timeout protection
# Tests validation logic, error handling, and system robustness
#
# Usage: ./run_edge_case_tests.sh
# =============================================================================

set -e

# Configuration
TIMEOUT_SECONDS=60
PYTHON_SCRIPT="unified_downloader.py"
TEST_LOG="edge_case_test_results.log"
SUMMARY_LOG="edge_case_test_summary.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Counters
TOTAL_TESTS=0
VALIDATION_ERRORS=0  # Expected - validation caught invalid input
FUNCTIONAL_ERRORS=0  # Unexpected - system should handle these
TIMEOUTS=0          # Problematic - system hung or ran too long
SUCCESSES=0         # Unexpected for edge cases, but possible
CRASHED=0           # Very problematic - unhandled exceptions

# Initialize logs
echo "=== EDGE CASE TEST SUITE - $(date) ===" > "$TEST_LOG"
echo "" > "$SUMMARY_LOG"

# Function to run a single test
run_test() {
    local test_name="$1"
    local test_command="$2"
    local expected_result="$3"  # VALIDATION_ERROR, FUNCTIONAL_ERROR, SUCCESS, etc.

    echo -e "${BLUE}Testing: $test_name${NC}"
    echo "=== TEST: $test_name ===" >> "$TEST_LOG"
    echo "Command: $test_command" >> "$TEST_LOG"
    echo "Expected: $expected_result" >> "$TEST_LOG"

    TOTAL_TESTS=$((TOTAL_TESTS + 1))

    # Run the test with timeout
    timeout $TIMEOUT_SECONDS bash -c "$test_command" >> "$TEST_LOG" 2>&1
    local exit_code=$?

    # Analyze result
    if [ $exit_code -eq 124 ]; then
        echo -e "  ${RED}TIMEOUT${NC} (>60s)"
        echo "Result: TIMEOUT" >> "$TEST_LOG"
        TIMEOUTS=$((TIMEOUTS + 1))
    elif [ $exit_code -eq 1 ]; then
        # Check if it's a validation error (expected) or other error
        if grep -q "Invalid\|Did you mean\|cannot be in the future\|must be before" "$TEST_LOG" | tail -20; then
            echo -e "  ${GREEN}VALIDATION ERROR${NC} (Expected)"
            echo "Result: VALIDATION_ERROR (Expected)" >> "$TEST_LOG"
            VALIDATION_ERRORS=$((VALIDATION_ERRORS + 1))
        else
            echo -e "  ${YELLOW}FUNCTIONAL ERROR${NC} (Unexpected)"
            echo "Result: FUNCTIONAL_ERROR (Unexpected)" >> "$TEST_LOG"
            FUNCTIONAL_ERRORS=$((FUNCTIONAL_ERRORS + 1))
        fi
    elif [ $exit_code -eq 0 ]; then
        echo -e "  ${GREEN}SUCCESS${NC} (Unexpected for edge case)"
        echo "Result: SUCCESS" >> "$TEST_LOG"
        SUCCESSES=$((SUCCESSES + 1))
    else
        echo -e "  ${RED}CRASHED${NC} (Exit code: $exit_code)"
        echo "Result: CRASHED (Exit code: $exit_code)" >> "$TEST_LOG"
        CRASHED=$((CRASHED + 1))
    fi

    echo "" >> "$TEST_LOG"
    sleep 1  # Brief pause between tests
}

echo -e "${BLUE}=== BINANCE DATA DOWNLOADER - EDGE CASE TEST SUITE ===${NC}"
echo -e "${BLUE}Timeout per test: ${TIMEOUT_SECONDS}s${NC}"
echo

# =============================================================================
# A. INPUT VALIDATION EDGE CASES
# =============================================================================
echo -e "${YELLOW}=== A. INPUT VALIDATION EDGE CASES ===${NC}"

# Test case sensitivity and spaces
run_test "Case sensitivity in data types" \
    "python $PYTHON_SCRIPT --data-types 'KLINES,TRADES' --start-date 2020-01-01 --end-date 2020-01-02" \
    "VALIDATION_ERROR"

run_test "Extra spaces in data types" \
    "python $PYTHON_SCRIPT --data-types ' klines , trades ' --start-date 2020-01-01 --end-date 2020-01-02" \
    "SUCCESS"

run_test "Empty comma-separated values" \
    "python $PYTHON_SCRIPT --data-types 'klines,,trades' --start-date 2020-01-01 --end-date 2020-01-02" \
    "SUCCESS"

# Test special characters and unicode
run_test "Special characters in data types" \
    "python $PYTHON_SCRIPT --data-types 'klines@trades' --start-date 2020-01-01 --end-date 2020-01-02" \
    "VALIDATION_ERROR"

run_test "Unicode characters in data types" \
    "python $PYTHON_SCRIPT --data-types 'klìnes,tràdes' --start-date 2020-01-01 --end-date 2020-01-02" \
    "VALIDATION_ERROR"

# Test empty parameters
run_test "Empty data types parameter" \
    "python $PYTHON_SCRIPT --data-types '' --start-date 2020-01-01 --end-date 2020-01-02" \
    "SUCCESS"

run_test "Only spaces in data types" \
    "python $PYTHON_SCRIPT --data-types '   ' --start-date 2020-01-01 --end-date 2020-01-02" \
    "SUCCESS"

run_test "Empty symbols parameter" \
    "python $PYTHON_SCRIPT --symbols '' --start-date 2020-01-01 --end-date 2020-01-02" \
    "VALIDATION_ERROR"

# =============================================================================
# B. DATE EDGE CASES
# =============================================================================
echo
echo -e "${YELLOW}=== B. DATE EDGE CASES ===${NC}"

# Test invalid date formats
run_test "Slash date format" \
    "python $PYTHON_SCRIPT --start-date '2020/01/01' --end-date 2020-01-02" \
    "VALIDATION_ERROR"

run_test "US date format" \
    "python $PYTHON_SCRIPT --start-date '01-01-2020' --end-date 2020-01-02" \
    "VALIDATION_ERROR"

run_test "Short date format" \
    "python $PYTHON_SCRIPT --start-date '2020-1-1' --end-date 2020-01-02" \
    "VALIDATION_ERROR"

# Test impossible dates
run_test "February 30th" \
    "python $PYTHON_SCRIPT --start-date '2020-02-30' --end-date 2020-03-01" \
    "VALIDATION_ERROR"

run_test "13th month" \
    "python $PYTHON_SCRIPT --start-date '2020-13-01' --end-date 2020-12-01" \
    "VALIDATION_ERROR"

run_test "Zero month" \
    "python $PYTHON_SCRIPT --start-date '2020-00-01' --end-date 2020-01-01" \
    "VALIDATION_ERROR"

# Test problematic date ranges
run_test "Inverted date range" \
    "python $PYTHON_SCRIPT --start-date '2020-01-02' --end-date '2020-01-01'" \
    "VALIDATION_ERROR"

run_test "Same start and end date" \
    "python $PYTHON_SCRIPT --start-date '2020-01-01' --end-date '2020-01-01'" \
    "SUCCESS"

run_test "Very old date" \
    "python $PYTHON_SCRIPT --start-date '1900-01-01' --end-date '1900-01-02'" \
    "SUCCESS"

run_test "Future date" \
    "python $PYTHON_SCRIPT --start-date '2030-01-01' --end-date '2030-01-02'" \
    "VALIDATION_ERROR"

# Test special characters in dates
run_test "Date with special character" \
    "python $PYTHON_SCRIPT --start-date '2020-01-01@' --end-date 2020-01-02" \
    "VALIDATION_ERROR"

# =============================================================================
# C. DATA TYPES EDGE CASES
# =============================================================================
echo
echo -e "${YELLOW}=== C. DATA TYPES EDGE CASES ===${NC}"

# Test common typos (should trigger suggestion system)
run_test "Common typo: kline instead of klines" \
    "python $PYTHON_SCRIPT --data-types 'kline,trade' --start-date 2020-01-01 --end-date 2020-01-02" \
    "VALIDATION_ERROR"

run_test "Common typos: bookdepth, metricss" \
    "python $PYTHON_SCRIPT --data-types 'bookdepth,metricss' --start-date 2020-01-01 --end-date 2020-01-02" \
    "VALIDATION_ERROR"

run_test "Typos: aggtrade, fundinrate" \
    "python $PYTHON_SCRIPT --data-types 'aggtrade,fundinrate' --start-date 2020-01-01 --end-date 2020-01-02" \
    "VALIDATION_ERROR"

# Test case sensitivity
run_test "Mixed case data types" \
    "python $PYTHON_SCRIPT --data-types 'Klines,BookDepth' --start-date 2020-01-01 --end-date 2020-01-02" \
    "VALIDATION_ERROR"

run_test "All uppercase data types" \
    "python $PYTHON_SCRIPT --data-types 'TRADES,AGGTRADES' --start-date 2020-01-01 --end-date 2020-01-02" \
    "VALIDATION_ERROR"

# Test completely wrong data types
run_test "Cryptocurrency names as data types" \
    "python $PYTHON_SCRIPT --data-types 'bitcoin,ethereum' --start-date 2020-01-01 --end-date 2020-01-02" \
    "VALIDATION_ERROR"

run_test "Random strings as data types" \
    "python $PYTHON_SCRIPT --data-types 'xyz,abc,def' --start-date 2020-01-01 --end-date 2020-01-02" \
    "VALIDATION_ERROR"

run_test "Numbers as data types" \
    "python $PYTHON_SCRIPT --data-types '123,456' --start-date 2020-01-01 --end-date 2020-01-02" \
    "VALIDATION_ERROR"

# =============================================================================
# D. INTERVALS EDGE CASES
# =============================================================================
echo
echo -e "${YELLOW}=== D. INTERVALS EDGE CASES ===${NC}"

# Test common interval format mistakes
run_test "Minutes with 'min' suffix" \
    "python $PYTHON_SCRIPT --interval '5min' --start-date 2020-01-01 --end-date 2020-01-02" \
    "VALIDATION_ERROR"

run_test "Hour with 'hour' suffix" \
    "python $PYTHON_SCRIPT --interval '1hour' --start-date 2020-01-01 --end-date 2020-01-02" \
    "VALIDATION_ERROR"

run_test "Day with 'day' suffix" \
    "python $PYTHON_SCRIPT --interval '1day' --start-date 2020-01-01 --end-date 2020-01-02" \
    "VALIDATION_ERROR"

# Test case sensitivity in intervals
run_test "Uppercase minute interval" \
    "python $PYTHON_SCRIPT --interval '1M' --start-date 2020-01-01 --end-date 2020-01-02" \
    "VALIDATION_ERROR"

run_test "Uppercase hour interval" \
    "python $PYTHON_SCRIPT --interval '5H' --start-date 2020-01-01 --end-date 2020-01-02" \
    "VALIDATION_ERROR"

# Test special characters in intervals
run_test "Interval with special character" \
    "python $PYTHON_SCRIPT --interval '5m@' --start-date 2020-01-01 --end-date 2020-01-02" \
    "VALIDATION_ERROR"

run_test "Interval with spaces" \
    "python $PYTHON_SCRIPT --interval ' 1h ' --start-date 2020-01-01 --end-date 2020-01-02" \
    "VALIDATION_ERROR"

run_test "Empty interval" \
    "python $PYTHON_SCRIPT --interval '' --start-date 2020-01-01 --end-date 2020-01-02" \
    "VALIDATION_ERROR"

# Test numeric-only intervals
run_test "Pure number as interval" \
    "python $PYTHON_SCRIPT --interval '5' --start-date 2020-01-01 --end-date 2020-01-02" \
    "VALIDATION_ERROR"

run_test "Large number as interval" \
    "python $PYTHON_SCRIPT --interval '60' --start-date 2020-01-01 --end-date 2020-01-02" \
    "VALIDATION_ERROR"

# =============================================================================
# E. SYMBOLS EDGE CASES
# =============================================================================
echo
echo -e "${YELLOW}=== E. SYMBOLS EDGE CASES ===${NC}"

# Test symbols with special characters
run_test "Symbol with @ character" \
    "python $PYTHON_SCRIPT --symbols 'BTC@USDT' --start-date 2020-01-01 --end-date 2020-01-02" \
    "SUCCESS"

run_test "Symbols with dash and underscore" \
    "python $PYTHON_SCRIPT --symbols 'BTC-USDT,ETH_USDT' --start-date 2020-01-01 --end-date 2020-01-02" \
    "SUCCESS"

# Test very long symbol names
run_test "Very long symbol name" \
    "python $PYTHON_SCRIPT --symbols 'VERYLONGSYMBOLNAMETHATDOESNOTEXIST' --start-date 2020-01-01 --end-date 2020-01-02" \
    "SUCCESS"

# Test symbols with numbers and strange characters
run_test "Symbols with numbers" \
    "python $PYTHON_SCRIPT --symbols '123ABC,XYZ999' --start-date 2020-01-01 --end-date 2020-01-02" \
    "SUCCESS"

run_test "Symbol with slash" \
    "python $PYTHON_SCRIPT --symbols 'BTC/USDT' --start-date 2020-01-01 --end-date 2020-01-02" \
    "SUCCESS"

# Test case sensitivity in symbols (should be converted to uppercase)
run_test "Lowercase symbols" \
    "python $PYTHON_SCRIPT --symbols 'btcusdt,ethusdt' --start-date 2020-01-01 --end-date 2020-01-02" \
    "SUCCESS"

# =============================================================================
# F. FUNCTIONAL EDGE CASES (SAFE - SHORT DATE RANGES)
# =============================================================================
echo
echo -e "${YELLOW}=== F. FUNCTIONAL EDGE CASES (SAFE) ===${NC}"

# Test with non-existent symbols (safe - 1 day range)
run_test "Non-existent symbol with klines" \
    "python $PYTHON_SCRIPT --symbols 'NONEXISTENTSYMBOL' --data-types 'klines' --start-date 2020-01-01 --end-date 2020-01-01" \
    "SUCCESS"

run_test "Non-existent data type" \
    "python $PYTHON_SCRIPT --symbols 'BTCUSDT' --data-types 'nonexistenttype' --start-date 2020-01-01 --end-date 2020-01-01" \
    "VALIDATION_ERROR"

# Test with dates where probably no data exists
run_test "Very old date where no data exists" \
    "python $PYTHON_SCRIPT --symbols 'BTCUSDT' --data-types 'klines' --start-date 2010-01-01 --end-date 2010-01-01" \
    "SUCCESS"

run_test "bookDepth on old date" \
    "python $PYTHON_SCRIPT --symbols 'BTCUSDT' --data-types 'bookDepth' --start-date 2019-01-01 --end-date 2019-01-01" \
    "SUCCESS"

# Test incompatible combinations
run_test "fundingRate with interval (should ignore interval)" \
    "python $PYTHON_SCRIPT --data-types 'fundingRate' --interval '1m' --start-date 2020-01-01 --end-date 2020-01-02" \
    "SUCCESS"

# =============================================================================
# G. SYSTEM EDGE CASES
# =============================================================================
echo
echo -e "${YELLOW}=== G. SYSTEM EDGE CASES ===${NC}"

# Test very long parameters (but still reasonable)
run_test "Many symbols" \
    "python $PYTHON_SCRIPT --symbols 'BTCUSDT,ETHUSDT,ADAUSDT,BNBUSDT,XRPUSDT' --data-types 'klines' --start-date 2020-01-01 --end-date 2020-01-01" \
    "SUCCESS"

# Test escape characters
run_test "Data types with tab character" \
    $'python '"$PYTHON_SCRIPT"$' --data-types \'klines\\ttrades\' --start-date 2020-01-01 --end-date 2020-01-02' \
    "VALIDATION_ERROR"

# Test multiple verbose flags (should work fine)
run_test "Multiple verbose flags" \
    "python $PYTHON_SCRIPT --verbose --verbose --data-types 'klines' --start-date 2020-01-01 --end-date 2020-01-01" \
    "SUCCESS"

# =============================================================================
# TEST RESULTS SUMMARY
# =============================================================================
echo
echo -e "${BLUE}=== TEST RESULTS SUMMARY ===${NC}"
echo
echo "Total Tests Run: $TOTAL_TESTS"
echo -e "${GREEN}Validation Errors: $VALIDATION_ERRORS${NC} (Expected - system caught invalid input)"
echo -e "${YELLOW}Functional Errors: $FUNCTIONAL_ERRORS${NC} (Unexpected - should investigate)"
echo -e "${RED}Timeouts: $TIMEOUTS${NC} (Problematic - system hung or too slow)"
echo -e "${GREEN}Successes: $SUCCESSES${NC} (Some edge cases may legitimately succeed)"
echo -e "${RED}Crashes: $CRASHED${NC} (Very problematic - unhandled exceptions)"

# Write summary to log file
{
    echo "=== EDGE CASE TEST SUITE SUMMARY - $(date) ==="
    echo "Total Tests Run: $TOTAL_TESTS"
    echo "Validation Errors: $VALIDATION_ERRORS (Expected)"
    echo "Functional Errors: $FUNCTIONAL_ERRORS (Unexpected)"
    echo "Timeouts: $TIMEOUTS (Problematic)"
    echo "Successes: $SUCCESSES"
    echo "Crashes: $CRASHED (Very problematic)"
    echo
    echo "SUCCESS RATE: $(echo "scale=1; ($VALIDATION_ERRORS + $SUCCESSES) * 100 / $TOTAL_TESTS" | bc -l)%"
    echo "(Validation errors + successes are both acceptable outcomes)"
    echo
    if [ $TIMEOUTS -gt 0 ] || [ $CRASHED -gt 0 ] || [ $FUNCTIONAL_ERRORS -gt 3 ]; then
        echo "⚠️  ATTENTION NEEDED: System has problematic behaviors that should be investigated"
    else
        echo "✅ SYSTEM STATUS: Good - validation working as expected"
    fi
} >> "$SUMMARY_LOG"

echo
echo -e "${BLUE}Detailed logs saved to:${NC}"
echo -e "  Full test log: ${BLUE}$TEST_LOG${NC}"
echo -e "  Summary log: ${BLUE}$SUMMARY_LOG${NC}"
echo

# Final status
if [ $TIMEOUTS -gt 0 ] || [ $CRASHED -gt 0 ] || [ $FUNCTIONAL_ERRORS -gt 3 ]; then
    echo -e "${RED}❌ Some tests revealed issues that need investigation${NC}"
    exit 1
else
    echo -e "${GREEN}✅ Edge case testing completed successfully${NC}"
    exit 0
fi