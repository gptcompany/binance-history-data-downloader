# Binance Downloader Refactor Plan

## Objective

Create a unified, simplified script for Binance data download, verification, and reporting, with minimal arguments and robust automation.

---

## 1. Script Arguments & Entry Point

- `--symbol` (default: BTCUSDT,BTCUSDC; comma-separated)
- `--start-date` (default: 2020-01-01; format: YYYY-MM-DD)
- No other arguments required for basic operation.

---

## 2. Discovery Phase

- For each symbol and each data type:
  - If a config file (e.g., `data_availability.json`) exists, load the earliest available date for that symbol/type.
  - If not, run the discovery logic (reuse or adapt from `discover_data_ranges.py`) to find the true earliest date, and save it to the config file for future runs.

---

## 3. Verification & Download Loop

- For each symbol/type:
  - Determine the effective start date (argument or config, whichever is later).
  - For each expected file from start date to today:
    - Check if the file exists and is valid.
    - If missing or invalid, attempt download (up to 2 retries).
    - After download, verify integrity (e.g., CSV not empty, checksum if available).

---

## 4. Reporting

- At the end, produce a CSV report listing any missing or invalid files that could not be recovered after retries.

---

## 5. Configuration Management

- Use a single config file (e.g., `data_availability.json`) to cache earliest available dates per symbol/type.
- If config exists, skip discovery for those symbol/types.

---

## 6. Fallbacks & Logging

- Maintain robust logging (console + file).
- If a download fails after retries, log the error and include it in the final report.

---

## Workflow Diagram

```mermaid
flowchart TD
    A[Start Script] --> B{Config Exists?}
    B -- Yes --> C[Load Earliest Dates]
    B -- No --> D[Run Discovery]
    D --> E[Save to Config]
    C & E --> F[Determine Start Date]
    F --> G[For Each Symbol/Type/Date]
    G --> H{File Valid?}
    H -- Yes --> I[Skip]
    H -- No --> J[Download (up to 2x)]
    J --> K{Valid After Download?}
    K -- Yes --> I
    K -- No --> L[Log as Missing]
    I & L --> M[Next File]
    M --> G
    G --> N[All Done?]
    N -- Yes --> O[Produce Final Report]
    O --> P[End]
```

---

## Key Points

- **Single script**: All logic in one place, minimal arguments.
- **Config file**: Avoids repeated discovery, speeds up future runs.
- **Automatic verification and retry**: No manual intervention needed.
- **Final report**: CSV with all unresolved issues.
- **Keep fallback and verification logic**: Reuse robust parts from current scripts.