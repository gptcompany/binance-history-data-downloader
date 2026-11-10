# Boundary and reuse

Mapping: `scripts/run-sync.sh` calls `src/unified_downloader.py`; `process_file`
skips existing CSVs when `verify_csv_file` finds nonzero bytes. The latter does not
validate schema, UTC dates or uniqueness. Imports also create log directories.
`temporal_gap_detector.py` inventories filenames and cannot certify CSV contents.

Selection: add a separate metadata-only planning boundary before touching runtime
or existing download behavior. Existing downloader functions are not extracted,
wrapped or changed in this slice; their behavior and current tests remain intact.
Reuse: standard-library urllib supplies verified-TLS HEAD, datetime supplies UTC,
and argparse/json supply the command and serialization. No new dependency or copied
external code is required. Offline fixtures characterize the new boundary RED/GREEN
in CI; no local test or build is authorized. Content validation and execution are
subsequent slices with reproducing fixtures committed before implementation.
