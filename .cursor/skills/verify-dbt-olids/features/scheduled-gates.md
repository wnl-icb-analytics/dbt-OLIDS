# Scheduled pipeline gates

Scheduled pipeline gates decide whether dbt should run and record the watermark baseline. The logic is covered by in-repo unit tests; the live script talks to Snowflake as the workflow does.

## Sub-features

- `gates-units` runs the CI Python tests with no warehouse.
- `gates-watermark` runs `check_source_watermarks.py` as a live skip/run decision.
- `gates-verify` is the post-build `--verify --record-success` path (mutates `AUDIT.PIPELINE_STATE`).

## How to get to it (user POV)

- Run `python -m unittest discover -s scripts/checks/tests -v`.
- Run `python scripts/checks/check_source_watermarks.py` (manual dispatch equivalent).
- Run `python scripts/checks/check_source_watermarks.py --verify --record-success` only after a real successful build and publish.

## Driving it with control-dbt-olids

Preconditions:

- `project_ok=true` for `gates-units`.
- `warehouse_ok=true` for `gates-watermark` and `gates-verify`.
- Do not pass `--record-success` unless this run built and published successfully.

- **Unit tests.** Prove watermark matching, deploy procedure parsing, and audit history inserts. Run `control-dbt-olids cli --label gates-units -- python -m unittest discover -s scripts/checks/tests -v`. Exit code `0` and stdout include `OK` with every test passing (`test_find_unadvanced_requires_strictly_newer_watermarks`, `test_matches_previous_requires_the_full_watermark_set`, `test_returns_valid_response`, `test_build_run_context_uses_github_identity`).
- **Live watermark.** Decide whether dbt should run. Run `control-dbt-olids cli --label gates-watermark -- python scripts/checks/check_source_watermarks.py`. Exit code `0`. Stdout either lists tables whose processed watermark differs from source, or `Processed watermarks match source.` Table names and timestamps only.
- **Record success.** Forbidden on a verification skip. If proving the workflow end state after a real build, run `control-dbt-olids cli --label gates-verify -- python scripts/checks/check_source_watermarks.py --verify --record-success`. Exit code `0` and `Landing snapshot match source.`
- **Proof.** For `gates-units`, keep `stdout.txt` showing `OK` and `exit_code.txt` of `0`. That is the offline proof. Do not call `gates-watermark` verified because units passed.

## Gotchas

- Unit tests import modules from `scripts/checks` and do not need Snowflake. A missing `snowflake-connector-python` can still fail imports in `deploy_data_lake_views` tests; install `requirements.txt`.
- `--wait-for-advance` and `--timeout-seconds 900` are for scheduled CI, not an interactive agent loop.
- `--record-success` overwrites `AUDIT.PIPELINE_STATE` for `pipeline_name = 'dbt-olids'`. Skip it unless the full workflow path succeeded.
- `run_checks.py` is aggregate QA, not a watermark gate. Use `--check source_freshness` if driving freshness; never `--verbose` into patient-level data (these checks have none; keep it that way).
