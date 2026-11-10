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

## Rendering contract

`scripts/cold_deploy.py` renders `binance-cold.service/.timer` (03:30 UTC)
and `binance-cold-drift.service/.timer` (03:00 UTC). The second timer checks
configuration only; it never schedules downloads. Both are persistent, with
journal output. The downloader requires the explicit cold path and Docker.
Inventory is strict JSON: image digest, image source full SHA, user, numeric UID/GID
and normalized cold root are mandatory; no unknown keys or mobile tags.
Documented Dell inventory defaults are user `sam`, UID/GID 1000 and cold root
`/data/cold/12TB/binance-history-data-downloader/data`; they are not host overrides.
The release defaults to `/opt/binance-cold/<approved-full-SHA>`.

This slice only renders strings and container argv. It cannot install units or
run downloads. The final inventory must contain the real build digest and source
SHA before installation. Subsequent installer/runner slices must verify approved
bytes independently of Git index flags and emit exactly one JSON result per run,
with UTC and counts. No unit may be enabled before these slices are merged.

## Record validation boundary

`backfill_records.validated_rows` consumes the whole source archive and yields
only the requested UTC period. The caller must exhaust it before atomically
publishing any CSV: the final completeness check runs at end of iteration.
Klines require the full 1m/5m/15m grid and exact close time. All seven USD-M
schemas require finite numeric fields, ordered unique record keys and source
period membership. Funding remains monthly; missing files remain unpublished.
Metrics uses `(00:00, next 00:00]`, confirmed on both symbols for June 27/28;
other schemas use `[00:00, next 00:00)`. Naive strings mean UTC, nonzero offsets
fail. Monthly fallback validates every source row before selecting a day.
These checks establish consistency with the checksum-verified archive, not
independent proof that Binance published every trade. No executor is added here.

## Archive integrity boundary

`backfill_archives.prepare_archive` first validates the bounded official SHA256
CHECKSUM document and exact archive name, then streams ZIP bytes to staging,
requiring both planned byte count and hash. Failed staging is removed.
`archive_rows` accepts one member with the expected CSV basename and never
extracts ZIP paths. Its iterator must be exhausted before publication, including
ZIP CRC and all source record checks. The executor will own the temporary
directory and atomic CSV/manifest publication; this library has no CLI caller.

## Idempotent execution

Without `--dry-run`, `--data-root` is mandatory. An exclusive filesystem lock
protects the run. Each source ZIP is fetched once per run; monthly sources feed
separate daily outputs. After exhausting validation, CSV and strict JSON manifest
are fsynced and replaced, then their parent directory is fsynced. A crash between
the two replacements leaves mismatched provenance; the next run re-verifies.
A skip requires matching selection/provenance schema and actual CSV SHA256,
never mere existence. The manifest records source ZIP hash, CSV hash and rows.
One JSON result reports UTC, outcome and counts; incomplete/unpublished days
return nonzero and never enter complete_days. A day also requires each selected
symbol’s funding target for that UTC month; missing funding in another month
does not invalidate that day. Completion covers requested series,
not an independent assertion that Binance published every trade. No scheduler
activation, image build or real backfill is performed by this change.

## Approved deploy snapshot

`scripts/cold_snapshot.py REPOSITORY FULL_SHA ACTION ...` obtains only that
commit's `git archive`, extracts a temporary data-filtered snapshot, and executes
its `scripts/cold_deploy.py` with Python isolated mode. It never imports deploy
code from the mutable repository, even when index flags hide changes. Branches,
abbreviated SHAs and missing objects fail without fallback; temporary bytes are
removed on success or failure. The Python environment and Git installation are
host-managed prerequisites. Delivery must install this bootstrap from approved
Git bytes outside the mutable checkout; invoking a modified checkout copy is not
a trust boundary. The following installer/runner slice will wire this entrypoint
and compare installed bootstrap, units and inventory against approved rendering.
No installation or action is performed by introducing this bootstrap.

## Host reconciliation boundary

Inventory has one source, `deploy/cold.json` in this owner repository;
network-infra delivers the approved owner SHA, without a second inventory copy.
The service uses isolated Python to read `scripts/cold_snapshot.py` with
`git show FULL_SHA:path`, then runs the action from that approved snapshot.
It never executes deploy code from the release working tree.
`cold_host.install` renders four units and masks both legacy service/timer pairs
with `/dev/null`; it does not reload, enable, start or stop systemd. B5 must
reconcile loaded state after merge. Drift compares every approved source file's
bytes, installed unit bytes, FragmentPath, DropInPaths and NeedDaemonReload, plus
legacy masks and inactive state. No service-rewritten keys are exempt here.
Python environment and Git remain host-managed trusted prerequisites.
The runner/CLI follows separately; no activation is permitted before it is merged.

## Legacy owner unit retirement

The repository no longer distributes `deploy/systemd/binance-sync-docker.service`
or its timer. README instructions now point to the sole cold authority. The final
installer must mask both names, plus monitoring-stack's `binance-sync.service`
and timer, and drift must verify they are masked and inactive before any download.
Source retirement alone neither stops nor certifies host state; B5 reconciles it
only after all migration PRs merge.

Approved-object reads use the shared `git_command` helper with
`--no-replace-objects`; local `refs/replace` cannot substitute another commit.
The unit bootstrap must use the same helper when reading its first Git blob.
