---
name: working-in-dbt-olids
description: >-
  How to work safely in the dbt-OLIDS repository: project layout, the
  production-only Snowflake target, how to validate a change without
  rebuilding production, tests, the nightly workflow and conventions. Use
  when changing, reviewing or debugging models, sources, macros, tests, check
  scripts or the scheduled workflow in this repository.
---

# Working in dbt-OLIDS

dbt-OLIDS builds the foundational OLIDS (One London Integrated Data Set) layers
on Snowflake. Its stable tables are published as `DATA_LAKE.OLIDS` views, which
dbt-analytics and other consumers read. This repository is public.

## The rule that matters most: there is no development target

`profiles.yml.template` has one target, `stable`. It writes to
`SNOWFLAKE_TARGET_DATABASE`, which is `OLIDS_ENGINEERING` in `env.example` and in
the nightly workflow. That is production. `DATA_LAKE.OLIDS` views read
`OLIDS_ENGINEERING.STABLE`, so a rebuilt stable table reaches consumers
straight away. Git worktrees do not isolate warehouse objects.

So:

- Do not run `dbt run`, `dbt build`, `dbt seed` or `dbt snapshot` unless the user
  has asked for that build in this conversation. Name the selection and say that
  it writes production before running it.
- Never add `--full-refresh` on your own initiative. `stable_healthcare_event`
  and `stable_clinical_record` are incremental, and the workflow fully refreshes
  them on the first of each month. The pseudonymisation indexes are incremental
  and are not routinely rebuilt from scratch.
- Never run `scripts/checks/deploy_data_lake_views.py`. It republishes the data
  lake views and belongs to the workflow.
- Do not start a build while the nightly run is active. It starts at 02:30 UTC
  in concurrency group `dbt-olids-prod`, on `WH_WNL_OLIDS_L`, and can wait for
  source snapshots for some time.
- Don't create ad hoc databases or schemas to get a sandbox.

Validate changes with the steps that don't write models (see below). This is
how recent pull requests were checked: "The repository has no development
target, so validation used dbt parse, compile and read-only aggregate queries
against production tables." (#323)

## Patient data

OLIDS is pseudonymised GP data, but rows are still patient-level. Output from
commands an agent runs is visible to its provider, and this repository is
public.

- Ad hoc queries and `dbt show` return only non-identifying aggregates: counts,
  rates, distributions, practice-level totals with no small cells. Never preview
  model rows.
- If row-level inspection is needed, write the query and give it to the user to
  run in their own Snowflake session.
- Fixtures, docs, commit messages and PR descriptions use synthetic values only.
  Never copy real identifiers, dates of birth or record IDs into them.

## Layout

Two model trees:

| Tree | Feed | State |
|---|---|---|
| `models/olids` | `Data_Store_OLIDS_WNL` (source `olids_pseudo` in `models/sources.yml`) | Live, built nightly |
| `models/synapse` | Legacy Synapse `Data_Store_OLIDS` | Frozen since 16 July 2026, `tag:synapse`. Leave it alone unless asked |

Layers in `models/olids`, with the schemas set by `macros/get_custom_schema.sql`:

| Folder | Schema | Role |
|---|---|---|
| `landing` | `LANDING` | Cache tables, one scan of each source table per day, aliased to the upper-case source name. `tag:landing` |
| `conformed` | `CONFORMED` | NCL practice filtering, patient spine and test-patient exclusion, concept mapping |
| `intermediate` | `CONFORMED` | Shared helpers: `int_ncl_practices`, `int_enriched_concept_map`, GP2GP copy flags |
| `stable` | `STABLE` | Published analytical interface, with person and patient indexes applied. `tag:stable` |
| `audit` | `AUDIT` | Pipeline quality metrics (`AUDIT_*`) |
| `models/indexes` | `PSEUDONYMISATION` | Incremental patient and person ID indexes and crosswalk |

Folder config in `dbt_project.yml` sets materialisation, schema, tags and the
`add_model_comment()` post-hook. Check neighbouring models before adding a local
config override.

Topic docs in `docs/` explain the harder areas. Read the relevant one before
changing that area, and update it in the same change:

- `olids-longitudinal-outputs.md` and `longitudinal-incremental-loading.md` for
  `stable_healthcare_event` and `stable_clinical_record`
- `appointment-clinical-links.md`
- `expanded-observations.md` and `expanded-observation-completeness.md`
- `person-id-cutover.md`

## Setup (Windows)

```powershell
python -m venv venv
pip install -r requirements.txt
cp profiles.yml.template profiles.yml
cp env.example .env   # fill in credentials; never commit .env or profiles.yml
.\start_dbt.ps1       # every session: activates venv and loads .env
dbt deps
dbt debug --target stable --profiles-dir .
```

CI uses dbt Fusion with `--target stable --profiles-dir .`; use the same flags.
The README still shows `dbt run` for routine builds. The workflow uses
`dbt build`.

## Validating a change

Work from the lightest check up. None of these rebuild models:

1. `dbt parse` and `dbt compile -s <model>` to check SQL, refs and Jinja.
2. `dbt ls -s <model>+ --resource-type model` to see what a change reaches.
   Check stable consumers too (step 7).
3. Unit tests: `dbt test -s test_type:unit`. Unit tests live beside the models,
   for example `models/olids/intermediate/gp2gp/gp2gp_unit.yml`, and read
   synthetic fixtures from `tests/fixtures/` (`fixture: gp2gp_observation`).
   Add or extend one when you change rule logic.
4. Offline Python tests for the check scripts:
   `python -m unittest discover -s scripts/checks/tests`.
5. Fixture checks for the longitudinal and appointment-link models: compile the
   model, then run `scripts/checks/check_*_fixtures.py` with the compiled SQL
   path. Each prints a query that wraps the model in synthetic inputs and returns
   a failure count. Run that query read-only.
6. Read-only aggregate queries against production, comparing the compiled
   query's counts with the current table. Keep them aggregate (see Patient data).
7. Downstream: dbt-analytics reads the published views through its generated
   `models/raw/olids/raw_olids_*` models (`source('olids', ...)` on
   `DATA_LAKE.OLIDS`, mapped in `scripts/sources/source_mappings.yml`). Renaming,
   removing or retyping a stable column, or changing a stable grain, breaks those
   models and everything built on them. Search dbt-analytics before changing a
   stable interface and say in the PR what consumers must change.

`dbt test` on existing models and singular tests (`tests/assert_*.sql`) also
leave model tables untouched. Elementary's run-end hook may still record run
results in `DBT_ELEMENTARY`. Run data tests on the selection you changed, not
the whole project.

Live check scripts, which read production and return aggregates:

- `python scripts/checks/run_checks.py` compares the new feed with the legacy
  feed and checks freshness. It reads `LANDING`.
- `python scripts/checks/compare_sources_to_information_schema.py` compares
  `models/sources.yml` with the live source schema.

## Sources and drift

`models/sources.yml` must match the upstream feed. The workflow fails on a
missing declared table or a column difference, and opens one GitHub issue for
each undeclared new upstream table. When the feed changes:

- update `sources.yml` and the `landing_` model's explicit column list together;
- `tests/assert_landing_columns_exist_in_source.sql` and
  `tests/assert_source_columns_captured_in_landing.sql` check that contract;
- carry the column through conformed and stable only when consumers need it.

## The nightly workflow

`.github/workflows/dbt-scheduled.yml` (documented in
`.github/workflows/README.md`) runs these steps:

1. Check the source schema.
2. Wait up to 15 minutes for every source watermark to advance, and skip a
   batch that was already processed.
3. Run `dbt build --exclude tag:synapse`. On the first of the month it also
   fully refreshes the two longitudinal stables.
4. Publish `STABLE` to `DATA_LAKE.OLIDS`.
5. Record the watermark.
6. Append outcomes to `AUDIT.PIPELINE_RUN_HISTORY`, `WATERMARK_HISTORY` and
   `METRIC_HISTORY`.

A scheduled failure opens or updates one GitHub issue. The next successful
scheduled run closes it. To investigate a failure, read the run log with
`gh run view <id> --log-failed` and the audit tables. Manual dispatch has a
`force` input; only trigger it when the user asks.

## Conventions

- Conventional commits, enforced by commitlint: `feat`, `fix`, `docs`, `style`,
  `refactor`, `test`, `chore`, `perf`, `ci`, with a header of 100 characters at
  most. Scope with `(olids)` or `(scheduled)` where it fits.
- Pre-commit runs `sqlfluff fix` (`.sqlfluff`), YAML checks and merge-conflict
  checks.
- Signed commits are required. Recent pull requests are squash-merged into `main`; force
  pushes are blocked.
- Model descriptions become Snowflake comments through `add_model_comment()`,
  and `persist_docs` publishes column descriptions. Write them for analysts.
- Write in plain British English. Say what changed, why, and how it was
  checked. Include the read-only checks you ran and what you didn't build.
