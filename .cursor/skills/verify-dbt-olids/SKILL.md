---
name: verify-dbt-olids
description: >-
  Verify dbt-OLIDS from the CLI: launch dbt, doctor the project and warehouse
  connection, then drive dbt build/test and repo CI check scripts. Use when
  proving model, test, or scheduled-pipeline behaviour; not for a web UI and
  not for Snowflake MCP connectors.
---

# Verify dbt-OLIDS

Drive the OLIDS dbt project the way an analyst or the scheduled workflow does: install, connect, run selectors, capture pass/fail evidence. There is no application server. Do not add Snowflake MCP, skills, or connectors. Do not query or store patient-level rows. Do not print secrets.

## Launch

Install dependencies once per machine, then run each drive as a one-shot CLI command. Nothing stays listening.

Cloud and GitHub Actions (preferred when `dbt` is missing):

```bash
curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --target x86_64-unknown-linux-gnu --update
export PATH="$HOME/.local/bin:$PATH"
python3 -m venv venv
venv/bin/pip install -r requirements.txt
cp -n profiles.yml.template profiles.yml
dbt deps
```

Local Windows (README):

```bash
python -m venv venv
pip install -r requirements.txt
cp profiles.yml.template profiles.yml
cp env.example .env
# fill .env; never commit it
.\start_dbt.ps1
dbt deps
```

Ready when `dbt --version` prints a version and `dbt_packages/dbt_utils` exists. There is no port check.

Teardown is the Cleanup section. Do not leave a copied `profiles.yml` if this run created it; the file is gitignored.

Warehouse commands need process env (or `.env`) with `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_TARGET_DATABASE`, plus one of `SNOWFLAKE_PRIVATE_KEY_PATH`, `SNOWFLAKE_PAT`, or `SNOWFLAKE_PASSWORD`. CI uses key-pair auth and `--target stable --profiles-dir .`.

## Doctor

Run this first whenever anything looks off. It is read-only.

```bash
.cursor/skills/verify-dbt-olids/scripts/control-dbt-olids doctor
```

Require `project_ok=true` (parsed `sources.yml`) before offline CI unit tests. Require `warehouse_ok=true` (and therefore `debug_ok=true`) before `dbt build`, `dbt test`, or live check scripts. `worth_driving` is the warehouse verdict: if it is false, do not run warehouse commands. `dbt_ok` and `packages_ok` must also be true before any `dbt` command.

Refuse to drive a warehouse session this run did not authenticate via `dbt debug`. The scheduled job uses concurrency group `dbt-olids-prod` against shared `OLIDS_ENGINEERING`; do not start a second full production build beside it.

## Drive

Put `control-dbt-olids` on `PATH` or invoke it by repo-relative path. Treat every command as literal.

```bash
export CONTROL_DBT_OLIDS_RUN_ID=<run-id>
export CONTROL_DBT_OLIDS_ARTEFACT_DIR=.cursor/skills/verify-dbt-olids/artefacts/<run-id>
.cursor/skills/verify-dbt-olids/scripts/control-dbt-olids cli --label <feature-id> -- <command>
```

Documented warehouse commands (need `warehouse_ok`):

```bash
dbt debug --target stable --profiles-dir .
dbt build --exclude tag:synapse --target stable --profiles-dir .
dbt build -s tag:landing --target stable --profiles-dir .
dbt test -s tag:stable --target stable --profiles-dir .
dbt test -s stable_patient --target stable --profiles-dir .
python scripts/checks/compare_sources_to_information_schema.py
python scripts/checks/check_source_watermarks.py
python scripts/checks/run_checks.py
```

README still shows `dbt run` for layer refreshes. Prefer `dbt build` as in `.github/workflows/dbt-scheduled.yml`. Prefer a changed-model selector (`-s <model>`) over a full nightly build.

Offline command (need `project_ok` only):

```bash
python -m unittest discover -s scripts/checks/tests -v
```

Do not run `scripts/checks/deploy_data_lake_views.py` from verification: it publishes `OLIDS_ENGINEERING.STABLE` to `DATA_LAKE.OLIDS`. Do not `--full-refresh` unless the mapped feature says so. Do not `dbt run -s tag:synapse` unless proving the frozen legacy tree.

Read the feature file before driving. A proof that uses one convenient selector is incomplete when the map lists others; report unused entry points as skipped with the unmet precondition.

## Evidence

Write under `.cursor/skills/verify-dbt-olids/artefacts/<run-id>/`. Cleanup must not delete that tree.

Proof standards:

- Exercise the real CLI path (`dbt` or `python scripts/checks/...`), not an internal setter.
- Capture the command, stdout, stderr, and exit code from `control-dbt-olids cli`.
- For dbt, also keep `run_results.summary.json` (status and unique_id only). That is the second view of stored test/model outcomes. Do not copy `target/compiled` SQL or failing-row dumps; `store_failures` is off, and stdout may still print keys.
- Live checks are aggregate-only (practice codes, counts, watermarks). Discard any accidental patient identifier.
- Record the feature ID and entry point on the artefact folder name (`--label`).
- Redaction is mandatory: secrets, PEM blocks, and UUID-shaped tokens are stripped by the helper.

A dry-run or skipped warehouse command is not a pass. Record `warehouse_ok=false` and the attempted command.

## Cleanup

Kill only processes this run started. dbt and the check scripts are short-lived; if a drive is still running, stop that PID, not every `dbt` on the machine.

Remove scratch this run created (`profiles.yml` only if the run copied it, leftover `target/run_results.json` is optional). Keep `venv/` and `dbt_packages/` for the next drive. Never delete `.cursor/skills/verify-dbt-olids/artefacts/`.

## Helpers

`scripts/control-dbt-olids` is executable:

```bash
.cursor/skills/verify-dbt-olids/scripts/control-dbt-olids doctor
.cursor/skills/verify-dbt-olids/scripts/control-dbt-olids cli --label stable-tests -- dbt test -s tag:stable --target stable --profiles-dir .
.cursor/skills/verify-dbt-olids/scripts/control-dbt-olids summarise-run-results target/run_results.json artefacts/run_results.summary.json
```
