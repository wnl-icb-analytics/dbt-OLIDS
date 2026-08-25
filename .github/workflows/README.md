# GitHub Actions workflows

`dbt-scheduled.yml` starts at 02:30 UTC each day (03:30 BST). It waits for
current OLIDS source snapshots, skips an already-processed batch, and runs dbt
before the downstream `dbt-analytics` 04:00 UTC schedule.

Scheduled runs allow 15 minutes for every snapshot to meet the six-hour
freshness SLA. Manual dispatches use the latest available snapshot immediately;
the `force` input also rebuilds an already-processed snapshot.

No-op detection uses the source watermark set recorded after the last
successful full build. A failed partial build is retried.

Every attempted dbt build appends its step outcomes, source and landing
watermarks, and current `AUDIT.AUDIT_*` metrics to the audit history tables.
This includes models built before a test failure. Only a verified run advances
the no-op baseline.

After a successful build, the workflow publishes `OLIDS_ENGINEERING.STABLE`
through `DATA_LAKE.OLIDS` using the single-source deployment procedure. It runs
as `DATA_PLATFORM_MANAGER`, so replaced views retain platform ownership. The
workflow verifies that every stable object has one DPM-owned data-lake view.
It then reconciles inherited object comments without replacing unchanged views.
The watermark is recorded only after publication and verification succeed.

The workflow fails before dbt when the live source schema differs from
`models/sources.yml`. Scheduled failures create or update one GitHub issue and
the next successful scheduled run closes it.
