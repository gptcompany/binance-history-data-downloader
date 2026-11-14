# Binance cold deployment and backfill (F-649)

The sole inventory is `deploy/cold.json`; the scheduler decision is
[the owner decision record](../decisions/cold-scheduler.md). Network-infra delivers
one approved owner release SHA to `/opt/binance-cold/<full-release-SHA>` on
sam7670. That SHA includes this inventory and differs from the image source SHA.
The historical `/media/sam/1TB/binance-history-data-downloader` checkout stays
read-only. The only data mount is the inventory's cold `data` directory.

## Immutable image provenance

Build-only progressive-deploy run
[42961958615](https://github.com/gptcompany/progressive-deploy/actions/runs/42961958615)
succeeded on rerun from owner source `d979c2c47d9a87f5e7d17a9a9ecc45299d836270`.
Its published image is
`ghcr.io/gptcompany/binance-downloader@sha256:beca5d7b878a0e0889014080cbd4a47dccc4a46970b43cec1ff5f0e2ab6a34cf`.
The `containerimage.digest` in the Build and push log confirms that digest.
The source includes #18: metrics uses [start, end), sorts out-of-order rows,
and preserves error context fields.
The container runs as UID/GID 1000 (host user sam), with UTC explicit and
`--pull=never`; B5 must provision that exact image before execution.

## Operator reconciliation after merge

B5 first retires the progressive-deploy Rollout/trigger and monitoring-stack's
legacy scheduler from their merged versioned changes. Network-infra prepares the
approved release and its Python environment; it does not activate anything.
Use the approved Git-object bootstrap below for `install RELEASE`, then reconcile
systemd loaded state, the four legacy masks and the two new persistent timers.
Installation alone never reloads, enables, starts or stops units. Drift must pass
before enabling `binance-cold.timer` (06:30 UTC) and
`binance-cold-drift.timer` (03:00 UTC). The latter never downloads.
No live component runs on this Dell.

For each action, replace RELEASE and FULL_RELEASE_SHA with the approved immutable
release path and its complete commit SHA. Invoke the host-managed release Python
and obtain the bootstrap directly from the approved Git object, never a working
tree script. This dry-run prints the plan without fetching archives:

```console
RELEASE/.venv/bin/python -I -B -c 'import subprocess,sys;repo,sha=sys.argv[1:3];sys.argv=["cold_snapshot",repo,sha,*sys.argv[3:]];exec(subprocess.check_output(["git","--no-replace-objects","-C",repo,"show",sha+":scripts/cold_snapshot.py"]))' RELEASE FULL_RELEASE_SHA -- backfill RELEASE --start-date 2026-06-29 --end-date 2026-11-09 --dry-run
```

The same bootstrap accepts `-- drift RELEASE` or `-- install RELEASE` instead
of the backfill suffix. Installation requires the operator's authority to write
system units; development performs none of these host actions. Run dry-run only
after reconciliation: preflight drift intentionally blocks even metadata planning
on a divergent host. B5 reviews its UTC days, file selections, distinct archive
count and expected compressed bytes before removing `--dry-run` for backfill.
Defaults select BTCUSDT/ETHUSDT, klines/markPriceKlines at 1m/5m/15m,
trades/aggTrades/bookDepth/metrics, plus closed funding months since May 2026.

Every archive requires the published CHECKSUM, exact compressed size, SHA256,
ZIP CRC, strict CSV schema, ordered unique keys and UTC source-period validation.
Klines additionally require a complete interval grid. Dry-run marks monthly
fallbacks pending until execution validates the archive and checks for the UTC
day. An absent day is reported in `monthly_gaps` and the run continues with
outcome `incomplete`. New missing files block the cursor. The inventory declares
the October 8 bookDepth and October 28 BTCUSDT/ETHUSDT kline absences, which
can settle the cursor and are retried on subsequent runs.
A filesystem lock, atomic CSV/manifest replacement and provenance plus actual
CSV hash checks make reruns idempotent; existing CSV presence alone cannot skip.

Every invocation emits one UTC JSON outcome with phase and counts; unknown
progress stays null. Retain journal records and distinguish settled from complete
days. Pre-run and daily drift compare all approved source bytes, rendered units,
loaded manager properties and inactive masked legacy schedulers. Investigate any
finding before further runs. Rollback requires a reviewed owner/inventory SHA
change and operator reconciliation; do not edit host configuration independently
or delete cold data to roll back code.


## Daily publication window

The target remains yesterday (D-1), never D-2. Start at 06:30 UTC and poll
HEAD plus CHECKSUM metadata every 15 minutes until 14:00 UTC, as directed by
the operator. On 2026-11-13, ETHUSDT November 12 metrics were published at
06:42:05 UTC, bookDepth at 07:20:36 and 1m klines at 07:59:05; the old 03:30
schedule preceded all three. Binance documents next-day daily publication,
without promising an hour: https://github.com/binance/binance-public-data.

The host reuses the pinned image's dry-run planner and cursor. New available
archives trigger an idempotent download immediately; unchanged plans cause only
metadata probes. The initial incomplete run can receive one additional verified
pass while establishing the availability baseline. At cutoff, the JSON includes
`unresolved_targets` for the probed range and monthly empty selections;
historical declared absences remain in the separate `unavailable` counter;
exit 1 remains a systemd failure. Container, schema and non-publication errors
fail immediately. An in-flight download finishes normally; no polling replay
starts at or after 14:00. A manual start or persistent timer catch-up at or after
14:00 still attempts D-1 once and returns immediately, with exit 1 if incomplete;
it never silently skips the day. The existing immutable image remains unchanged.

The final JSON keeps `written` cumulative across cursor passes. `skipped` and
`records` describe the last pass; `attempt_totals` sums `written`, `skipped` and
`records` across every validated pass, including repeated verification of the same
files (include le riverifiche). These are operation counts, not distinct files or
rows. `missing`, `unavailable`, cursor and outcome always describe the latest
state, so the two declared bookDepth absences remain visible without multiplying
them by the number of passes.


The host owns one daily cycle state and finalizes its summary in `finally`.
A failed/invalid metadata probe, invalid cursor replay, exception, SIGINT or
SIGTERM preserves counters from every validated cursor pass and exits failed.
`final_state_status=unknown` marks that the final filesystem state could not be
confirmed; missing/unavailable and last-pass counts remain the last validated
observations, while `attempt_totals` retains all validated attempts. Probe error
JSON contributes no counts. Before any validated pass, existing unknown or
not-started count semantics apply. SIGKILL cannot produce a process summary.
