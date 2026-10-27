# GitHub Actions workflows

`dbt-scheduled.yml` runs only by `workflow_dispatch`. A Snowflake task
(`ACCOUNT_ADMIN.DBT_MANAGEMENT.TASK_DISPATCH_DBT_OLIDS_DAILY`, defined in
Snowflake-Deployment) dispatches it at 04:00 Europe/London with
`scheduled: true`; GitHub cron started runs hours late. OLIDS snapshots finish
by about 02:20 London. The run waits for current snapshots, skips an
already-processed batch, and runs dbt before the downstream `dbt-analytics`
06:00 run.

Scheduled runs allow 15 minutes for every snapshot watermark to advance past
the last successful build, which waits for a partially published snapshot to
finish. Manual dispatches without `scheduled` use the latest available snapshot
immediately; the `force` input also rebuilds an already-processed snapshot.

No-op detection uses the source watermark set recorded after the last
successful full build. A failed partial build is retried.

Every non-cancelled run that reaches dbt appends its step outcomes, source and
landing watermarks, and current `AUDIT.AUDIT_*` metrics to the audit history
tables. This includes models built before a test failure. Only a verified run
advances the no-op baseline.

After a successful build, the workflow publishes `OLIDS_ENGINEERING.STABLE`
through `DATA_LAKE.OLIDS` using the single-source deployment procedure. It runs
as `DATA_PLATFORM_MANAGER`, so replaced views retain platform ownership. The
workflow verifies that every stable object has one DPM-owned data-lake view.
It then reconciles inherited object comments without replacing unchanged views.
The watermark is recorded only after publication and verification succeed.

Missing declared source tables and column differences fail the workflow before
dbt. Undeclared upstream tables warn and open one GitHub issue per table, on both
scheduled and manual runs. Existing open issues prevent repeat notifications.
Issue reporting failures do not block dbt.

Scheduled build failures (`scheduled: true`) create or update one separate
GitHub issue. The next successful scheduled run closes that failure issue.
