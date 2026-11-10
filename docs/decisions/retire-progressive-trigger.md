# F-649: retire automatic progressive deployment dispatch

B5 required this retirement before further source merges on 2026-11-10.
The source repository no longer dispatches builds/deployments to progressive-deploy
on push or manual workflow invocation: the legacy trigger workflow is removed.
CI remains active. A regression parses workflow YAML and rejects the retired
repository dispatch endpoint or a progressive-deploy reusable workflow call.

The cold downloader belongs on the Dell alongside its cold data. The chosen
scheduler is systemd UTC versioned in this repository, with a separately pinned
runtime, explicit cold mount, Python installer and drift verification. The
historical runtime checkout remains read-only. Installation, activation and
backfill execution belong to B5 after review and merge of the dependent slices.

The companion progressive-deploy change must keep only image building and digest
reporting, and retire its Binance Rollout before the replacement is activated.
This source change does not alter cluster state or certify that retirement.
Rollback is a revert, which would restore automatic deployment dispatch and
therefore needs an explicit operator decision.
