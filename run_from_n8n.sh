#!/bin/bash
# Wrapper script to run Python downloader from N8N
# This script is executed from inside N8N container but runs Python on host

cd /workspace/3TB-WDC/binance-history-data-downloader

# Use system Python3 (available in N8N container or host)
/usr/bin/python3 src/unified_downloader.py "$@"
