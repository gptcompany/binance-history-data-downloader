# F-649: one cold scheduler

Decision approved by B5 on 2026-11-10, following the operator's SÌ.
The only scheduler is a UTC systemd timer versioned in this repository.
The Dell holds historical cold data only, with an explicit mount at
`/data/cold/12TB/binance-history-data-downloader/data`. No live component belongs there.
A separate runtime release pins a full commit SHA; the historical checkout at
`/media/sam/1TB/binance-history-data-downloader` remains read-only.

The repository's Python installer will render units and configuration from that
release. Inventory delivery belongs to network-infra (host sam7670), separately
reviewed, without running Ansible during development. Drift comparison must run
before every download and periodically, fail visibly in the journal, and compare
the installed units/configuration with the documented rendering of the pinned SHA.
Each run records outcome and counts. Activation and downloads belong to B5 after merge.

The progressive-deploy HTTP Rollout/trigger and monitoring-stack legacy Binance
units must be disabled in versioned follow-up PRs before activation. Their continued
existence is not evidence of a persistent cold pipeline. This decision alone does
not disable them or certify the scheduler migration complete.

## Backfill contract

`python src/backfill.py --start-date 2026-06-29 --end-date 2026-11-09
--funding-start 2026-05-01 --symbols BTCUSDT,ETHUSDT
--data-types klines,markPriceKlines,trades,aggTrades,bookDepth,metrics,fundingRate
--intervals 1m,5m,15m --dry-run` prints JSON metadata only: days, files, compressed
bytes (distinct archives), CHECKSUM availability, monthly fallbacks and unpublished holes.
It makes HEAD requests, writes no files and never fetches an archive. Availability
does not certify records or checksum contents. Funding selects months starting no
later than the requested end date, but excludes the current UTC month. A closed
month is available only after its monthly ZIP and CHECKSUM are published; missing
metadata remains an explicit hole. The planner receives a reference UTC date; only
the CLI defaults to the real UTC clock. A later executor must verify SHA256
per ZIP, validate CSV schemas/UTC dates/unique keys and the complete kline grid,
filter monthly fallback records, atomically publish under a lock and retain provenance,
CSV hash and counts. Existing CSV presence alone must never imply verified completion.
The two bookDepth holes on 2026-10-08 remain explicit; no synthetic rows or complete-day claim.
