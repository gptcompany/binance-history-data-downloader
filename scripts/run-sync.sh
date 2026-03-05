#!/bin/bash
# Binance Futures data sync — replaces N8N workflow PuSb49WQ8ypsOGEX
# Runs 7 sequential download calls (idempotent, skips existing files)
set -euo pipefail

cd /media/sam/3TB-WDC/binance-history-data-downloader

# Disable tqdm progress bars (no terminal in systemd)
export N8N_NO_PROGRESS=1

START_DATE="2021-12-01"
END_DATE=$(date -d "yesterday" +%Y-%m-%d)
PYTHON=python3
SYMBOLS="BTCUSDT,ETHUSDT"

echo "[$(date -Iseconds)] Binance sync: ${START_DATE} → ${END_DATE}"

# Klines at multiple intervals
$PYTHON src/unified_downloader.py --symbols "$SYMBOLS" --data-types klines --interval 1m --start-date "$START_DATE" --end-date "$END_DATE"
$PYTHON src/unified_downloader.py --symbols "$SYMBOLS" --data-types klines --interval 5m --start-date "$START_DATE" --end-date "$END_DATE"
$PYTHON src/unified_downloader.py --symbols "$SYMBOLS" --data-types klines --interval 15m --start-date "$START_DATE" --end-date "$END_DATE"

# Mark price klines at multiple intervals
$PYTHON src/unified_downloader.py --symbols "$SYMBOLS" --data-types markPriceKlines --interval 1m --start-date "$START_DATE" --end-date "$END_DATE"
$PYTHON src/unified_downloader.py --symbols "$SYMBOLS" --data-types markPriceKlines --interval 5m --start-date "$START_DATE" --end-date "$END_DATE"
$PYTHON src/unified_downloader.py --symbols "$SYMBOLS" --data-types markPriceKlines --interval 15m --start-date "$START_DATE" --end-date "$END_DATE"

# Trades, aggTrades, bookDepth, metrics, fundingRate (no interval)
$PYTHON src/unified_downloader.py --symbols "$SYMBOLS" --data-types trades,aggTrades,bookDepth,metrics,fundingRate --start-date "$START_DATE" --end-date "$END_DATE"

echo "[$(date -Iseconds)] Binance sync completed"
