# Nightly OLIDS build

Nightly build compiles and runs the current OLIDS tree (landing, conformed, stable, audit, indexes) and its tests, excluding the frozen Synapse tree.

## Sub-features

- `nightly-full` runs the weekday scheduled selector.
- `nightly-changed` builds one model and its downstream tests.
- `nightly-list` lists the same graph without running it.

## How to get to it (user POV)

- Run `dbt build --exclude tag:synapse --target stable --profiles-dir .` from the repo root after `dbt deps`.
- Run `dbt build -s <model> --target stable --profiles-dir .` for a local change.
- Inspect the graph with `dbt ls --exclude tag:synapse --resource-type model --target stable --profiles-dir .`.

## Driving it with control-dbt-olids

Preconditions:

- `control-dbt-olids doctor` reports `warehouse_ok=true` for build entry points.
- `project_ok=true` is enough for a skipped warehouse report.
- No other `dbt-olids-prod` scheduled run is building the same target database.
- Synapse models stay excluded unless the change is in `models/synapse`.

- **List graph.** Show the nightly selector. Run `control-dbt-olids cli --label nightly-list -- dbt ls --exclude tag:synapse --resource-type model --target stable --profiles-dir .`. Exit code `0` and stdout include `landing_`, `conformed_`, and `stable_` names and do not include `synapse_`.
- **Changed model.** Build one model. Run `control-dbt-olids cli --label nightly-changed -- dbt build -s landing_organisation --target stable --profiles-dir .`. Exit code `0` and `run_results.summary.json` show that model `success` with no error tests.
- **Full nightly.** Run the scheduled command. Run `control-dbt-olids cli --label nightly-full -- dbt build --exclude tag:synapse --target stable --profiles-dir .`. Exit code `0` and the summary counts have no `error` status. First of month CI also full-refreshes `stable_healthcare_event` and `stable_clinical_record`; do not do that from this recipe.
- **Proof.** Keep `command.txt`, `exit_code.txt`, and `run_results.summary.json` for the entry point used. The summary identifies the selector's models by `unique_id` and `status`.

## Gotchas

- README's `dbt run --exclude tag:synapse` skips tests. The scheduled workflow uses `dbt build`.
- A full production build shares `OLIDS_ENGINEERING` and warehouse `WH_WNL_OLIDS_L`. Prefer `-s <model>` unless the change is the selector itself.
- `dbt build` on the first of the month uses `--full-refresh` for two large stables. Verification must not copy that unless explicitly proving that path.
- Listing the graph with `dbt ls` still needs a valid profile; dummy credentials are not a pass.
- Do not follow a successful build with `deploy_data_lake_views.py`.
