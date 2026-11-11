# Binance cold deployment and backfill (F-649)

The sole inventory is `deploy/cold.json`; the scheduler decision is
[the owner decision record](../decisions/cold-scheduler.md). Network-infra delivers
one approved owner release SHA to `/opt/binance-cold/<full-release-SHA>` on
sam7670. That SHA includes this inventory and differs from the image source SHA.
The historical `/media/sam/1TB/binance-history-data-downloader` checkout stays
read-only. The only data mount is the inventory's cold `data` directory.

## Immutable image provenance

Build-only progressive-deploy run
[42884609178](https://github.com/gptcompany/progressive-deploy/actions/runs/42884609178)
succeeded from owner source `d3e01f4160207a88494021e71b340b805c700b86`.
Its published image is
`ghcr.io/gptcompany/binance-downloader@sha256:c2447ba39f30b7e6ec80bb0e925258d05f9e51998a151255f5cb6ac3f6c70c09`.
The container runs as UID/GID 1000 (host user sam), with UTC explicit and
`--pull=never`; B5 must provision that exact image before execution.

## Operator reconciliation after merge

B5 first retires the progressive-deploy Rollout/trigger and monitoring-stack's
legacy scheduler from their merged versioned changes. Network-infra prepares the
approved release and its Python environment; it does not activate anything.
Use the approved Git-object bootstrap below for `install RELEASE`, then reconcile
systemd loaded state, the four legacy masks and the two new persistent timers.
Installation alone never reloads, enables, starts or stops units. Drift must pass
before enabling `binance-cold.timer` (03:30 UTC) and
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
