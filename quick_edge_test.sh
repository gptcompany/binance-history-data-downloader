#!/bin/bash

# =============================================================================
# Quick Edge Case Tester
# =============================================================================
# Utility script per testare rapidamente edge cases specifici
# Usage: ./quick_edge_test.sh [category]
# Categories: validation, dates, datatypes, intervals, symbols, functional, all
# =============================================================================

TIMEOUT_SECONDS=60
PYTHON_SCRIPT="unified_downloader.py"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Quick test function
quick_test() {
    local test_name="$1"
    local command="$2"

    echo -e "${BLUE}Testing: $test_name${NC}"
    echo "Command: $command"
    echo "---"

    timeout $TIMEOUT_SECONDS bash -c "$command"
    local exit_code=$?

    echo ""
    case $exit_code in
        0)   echo -e "Result: ${GREEN}SUCCESS${NC}" ;;
        1)   echo -e "Result: ${YELLOW}ERROR${NC} (possibly validation)" ;;
        124) echo -e "Result: ${RED}TIMEOUT${NC}" ;;
        *)   echo -e "Result: ${RED}CRASHED${NC} (exit code: $exit_code)" ;;
    esac
    echo "=============================================="
    echo
}

# Parse category argument
CATEGORY="${1:-validation}"

case "$CATEGORY" in
    "validation"|"v")
        echo -e "${YELLOW}=== QUICK VALIDATION TESTS ===${NC}"
        quick_test "Typos in data types" \
            "python $PYTHON_SCRIPT --data-types 'kline,trade' --start-date 2020-01-01 --end-date 2020-01-02"

        quick_test "Case sensitivity" \
            "python $PYTHON_SCRIPT --data-types 'KLINES,TRADES' --start-date 2020-01-01 --end-date 2020-01-02"

        quick_test "Special characters" \
            "python $PYTHON_SCRIPT --data-types 'klines@trades' --start-date 2020-01-01 --end-date 2020-01-02"
        ;;

    "dates"|"d")
        echo -e "${YELLOW}=== QUICK DATE TESTS ===${NC}"
        quick_test "Future date" \
            "python $PYTHON_SCRIPT --start-date '2030-01-01' --end-date '2030-01-02'"

        quick_test "Invalid format" \
            "python $PYTHON_SCRIPT --start-date '2020/01/01' --end-date 2020-01-02"

        quick_test "Inverted range" \
            "python $PYTHON_SCRIPT --start-date '2020-01-02' --end-date '2020-01-01'"
        ;;

    "datatypes"|"dt")
        echo -e "${YELLOW}=== QUICK DATA TYPES TESTS ===${NC}"
        quick_test "Common typos" \
            "python $PYTHON_SCRIPT --data-types 'bookdepth,metricss' --start-date 2020-01-01 --end-date 2020-01-02"

        quick_test "Wrong names" \
            "python $PYTHON_SCRIPT --data-types 'bitcoin,ethereum' --start-date 2020-01-01 --end-date 2020-01-02"

        quick_test "Numbers as types" \
            "python $PYTHON_SCRIPT --data-types '123,456' --start-date 2020-01-01 --end-date 2020-01-02"
        ;;

    "intervals"|"i")
        echo -e "${YELLOW}=== QUICK INTERVAL TESTS ===${NC}"
        quick_test "Wrong format: 5min" \
            "python $PYTHON_SCRIPT --interval '5min' --start-date 2020-01-01 --end-date 2020-01-02"

        quick_test "Wrong format: 1hour" \
            "python $PYTHON_SCRIPT --interval '1hour' --start-date 2020-01-01 --end-date 2020-01-02"

        quick_test "Case sensitivity" \
            "python $PYTHON_SCRIPT --interval '1M' --start-date 2020-01-01 --end-date 2020-01-02"
        ;;

    "symbols"|"s")
        echo -e "${YELLOW}=== QUICK SYMBOL TESTS ===${NC}"
        quick_test "Special characters" \
            "python $PYTHON_SCRIPT --symbols 'BTC@USDT,ETH/USDT' --start-date 2020-01-01 --end-date 2020-01-02"

        quick_test "Very long symbol" \
            "python $PYTHON_SCRIPT --symbols 'VERYLONGSYMBOLNAME' --start-date 2020-01-01 --end-date 2020-01-02"

        quick_test "Lowercase symbols" \
            "python $PYTHON_SCRIPT --symbols 'btcusdt' --start-date 2020-01-01 --end-date 2020-01-02"
        ;;

    "functional"|"f")
        echo -e "${YELLOW}=== QUICK FUNCTIONAL TESTS (SAFE) ===${NC}"
        quick_test "Non-existent symbol" \
            "python $PYTHON_SCRIPT --symbols 'FAKESYMBOL' --data-types 'klines' --start-date 2020-01-01 --end-date 2020-01-01"

        quick_test "Very old date" \
            "python $PYTHON_SCRIPT --symbols 'BTCUSDT' --data-types 'klines' --start-date 2010-01-01 --end-date 2010-01-01"

        quick_test "fundingRate with interval" \
            "python $PYTHON_SCRIPT --data-types 'fundingRate' --interval '1m' --start-date 2020-01-01 --end-date 2020-01-02"
        ;;

    "all"|"a")
        echo -e "${YELLOW}=== RUNNING ALL QUICK TESTS ===${NC}"
        "$0" validation
        "$0" dates
        "$0" datatypes
        "$0" intervals
        "$0" symbols
        "$0" functional
        ;;

    "help"|"h"|*)
        echo "Quick Edge Case Tester"
        echo "Usage: $0 [category]"
        echo ""
        echo "Categories:"
        echo "  validation, v  - Input validation tests"
        echo "  dates, d       - Date format and range tests"
        echo "  datatypes, dt  - Data type validation tests"
        echo "  intervals, i   - Interval format tests"
        echo "  symbols, s     - Symbol format tests"
        echo "  functional, f  - Safe functional tests"
        echo "  all, a         - Run all categories"
        echo "  help, h        - Show this help"
        echo ""
        echo "Examples:"
        echo "  $0 validation  # Test validation edge cases"
        echo "  $0 d          # Test date edge cases"
        echo "  $0 all        # Run all quick tests"
        ;;
esac