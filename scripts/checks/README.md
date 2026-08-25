# OLIDS checks

Runtime checks that compare the new WNL pseudonymised feed against the legacy
`Data_Store_OLIDS` feed, and verify source freshness. Run against Snowflake outside
dbt.

## Running

```bash
source venv/Scripts/activate && python scripts/checks/run_checks.py
```

Options:

- `--check <name>` run a single check (name without `.sql`)
- `--verbose` show all rows, including `PASS`/`INFO` (hidden by default)
- `--csv <dir>` also write one CSV per check to `<dir>`

By default only `WARN`/`FAIL` rows print. The runner exits `1` if any check returns a
`FAIL` row, else `0`.

Credentials come from `.env` at the repo root: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`,
`SNOWFLAKE_PASSWORD` (or external browser SSO if unset), `SNOWFLAKE_AUTHENTICATOR`,
`SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_TARGET_DATABASE`. The checks read
the new feed from the landing cache `<TARGET_DATABASE>.LANDING`, so
build it first: `dbt build -s tag:landing`.

## Output contract

Every check returns these columns, in order (extra columns may follow):

| Column | Description |
|---|---|
| `check_name` | Check identifier |
| `table_name` | Table or domain checked |
| `test_subject` | What specifically is checked (e.g. a practice code) |
| `status` | `PASS`, `WARN`, `FAIL` or `INFO` |
| `metric_value` | Measured value |
| `threshold` | Target the metric is compared against |

## Checks

| File | What it does |
|---|---|
| `compare_legacy_row_counts.sql` | New vs legacy row count per table pair; legacy restricted to new-feed practices for attributable tables |
| `compare_legacy_practice_counts.sql` | New vs legacy row count per practice per table (full outer join, so practices missing from the new feed surface) |
| `practice_row_counts.sql` | New-feed row counts by practice over the 20 attributable tables, plus a per-table total |
| `source_freshness.sql` | Days since latest `source_extraction_date` per landing table |

Comparisons are aggregate only: patient IDs are rotated between feeds, so row-level
matching is not possible.

## Source contract drift

```bash
source venv/Scripts/activate && python scripts/checks/compare_sources_to_information_schema.py
```

Compares the `olids_pseudo` source in `models/sources.yml` against live
`INFORMATION_SCHEMA`. Run it after upstream releases to spot contract drift, and
regenerate `sources.yml` when it reports differences. Exits `1` on any difference.

## Audit history

The scheduled workflow retains current audit metrics in `AUDIT.AUDIT_*` and
appends each non-cancelled run that reaches dbt to:

- `AUDIT.PIPELINE_RUN_HISTORY`: run identity and step outcomes
- `AUDIT.WATERMARK_HISTORY`: source and landing watermarks by table
- `AUDIT.METRIC_HISTORY`: metric rows copied from each `AUDIT.AUDIT_*` table

Failed dbt tests are retained with a failed outcome because their models may
already have built. `AUDIT.PIPELINE_STATE` advances only after the build, data
lake publication and landing verification pass.
