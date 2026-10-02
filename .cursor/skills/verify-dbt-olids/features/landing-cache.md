# Landing cache

Landing cache is the daily source snapshot used by conformed and stable models. Users build it with the `tag:landing` selector and rely on column-contract tests before spending warehouse time on downstream layers.

## Sub-features

- `landing-build` materialises landing tables for NCL-scoped sources.
- `landing-qa-hint` is the checks-README reminder to build landing before `run_checks.py`.
- `landing-column-tests` runs the singular tests `assert_landing_columns_exist_in_source` and `assert_source_columns_captured_in_landing`.

## How to get to it (user POV)

- Run `dbt build -s tag:landing --target stable --profiles-dir .`.
- Run `dbt build -s tag:landing` as documented in `scripts/checks/README.md` before deep QA.
- Run `dbt test -s assert_landing_columns_exist_in_source assert_source_columns_captured_in_landing --target stable --profiles-dir .`.

## Driving it with control-dbt-olids

Preconditions:

- `control-dbt-olids doctor` reports `warehouse_ok=true`.
- Source `olids_pseudo` is reachable with the configured role.
- Do not `--full-refresh` landing unless proving a broken incremental cache.

- **Build landing.** Materialise the cache. Run `control-dbt-olids cli --label landing-build -- dbt build -s tag:landing --target stable --profiles-dir .`. Exit code `0` and the summary shows `landing_*` models `success`.
- **Column contracts.** Run the two singular tests. Run `control-dbt-olids cli --label landing-column-tests -- dbt test -s assert_landing_columns_exist_in_source assert_source_columns_captured_in_landing --target stable --profiles-dir .`. Exit code `0` for the error-severity missing-source-column test; the captured-in-landing test is configured `warn` and may return warnings without failing.
- **QA precondition.** After landing exists, deep QA is `python scripts/checks/run_checks.py`. Run that only under the scheduled-gates or checks README path, with aggregate output only.
- **Proof.** Keep `run_results.summary.json` for `landing-build` or `landing-column-tests`. Model unique_ids start with `model.dbt_olids.landing_`.

## Gotchas

- Landing aliases match source table names (`ORGANISATION`, not `landing_organisation`) inside Snowflake. Selectors still use the dbt model name `landing_organisation`.
- `min_row_count` tests on large clinical landing tables are error-severity. A lab database with a subset will fail them; say so rather than lowering thresholds.
- `scripts/checks/run_checks.py` reads `<TARGET_DATABASE>.LANDING`. Building conformed only does not satisfy that path.
- Incremental landing is a single daily scan. A second concurrent landing build on the same target fights the scheduled job.
