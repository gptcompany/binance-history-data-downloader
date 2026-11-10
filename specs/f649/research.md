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


Cold contract mapping: Dockerfile still starts the legacy shell sync. The new
container argv must explicitly override it; a characterization fixture passes
on that unchanged Dockerfile before implementation. Existing systemd Docker units
are not installed or edited by this pure-rendering slice.
Selected boundary: strict JSON inventory, four deterministic unit strings and one
Docker argv. Standard library has no JSON Schema validator; reuse maintained
jsonschema rather than a custom key/type validator. Bybit PRs #7/#8 are design
references only (license endpoint returned 404; no code copied). Installer source
byte checks and one-result logging remain explicit prerequisites, not implemented
claims. RED fixtures precede implementation; CI is the only test authority.
