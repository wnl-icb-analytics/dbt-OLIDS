# dbt-OLIDS verification map

This directory is the maintained source for verifying the CLI behaviour of dbt-OLIDS. Read the index before driving, then use the matching feature file as the recipe.

## Baseline preconditions

- Work from the repo root with `profiles.yml` copied from `profiles.yml.template` when warehouse commands will run.
- Put `control-dbt-olids` at `.cursor/skills/verify-dbt-olids/scripts/control-dbt-olids`.
- Set `CONTROL_DBT_OLIDS_RUN_ID` and write artefacts under `.cursor/skills/verify-dbt-olids/artefacts/<run-id>/`.
- Run `control-dbt-olids doctor`. Require `project_ok` for offline gates. Require `warehouse_ok` for `dbt build`, `dbt test`, and live Snowflake check scripts.
- Never drive a warehouse session this run did not authenticate. Never publish data-lake views. Never dump patient-level rows or secrets.

## Driving conventions

- Start every recipe from the baseline unless its preconditions say otherwise.
- Treat every command as literal. Keep selectors and flags unchanged.
- Run captured commands through `control-dbt-olids cli --label <feature-id> -- <command>`.
- Prefer CI's `dbt build` / `dbt test` with `--target stable --profiles-dir .` over undocumented wrappers.
- Restore nothing in Snowflake after a read-only test. Do not remove proof artefacts during cleanup.

## Proof and skip reporting

- Capture the command, streams, and exit code, not only the final status line.
- dbt proof includes `run_results.summary.json` with model/test unique_id and status.
- Mutation proof (a build) includes a following `dbt test` or `dbt ls` of the same selector, or the run_results rows for tests bundled in `dbt build`.
- Record the feature ID and entry point used with every artefact.
- Report an unreachable path with the attempted command and the unmet precondition.
- Do not report a skipped entry point as verified through a different path.

## Feature entry contract

Each feature file starts with an H1 title and one paragraph describing the user-visible behaviour. It then uses exactly four H2 sections in this order.

1. `Sub-features` lists short IDs with one line for each behaviour.
2. `How to get to it (user POV)` lists every user entry point.
3. `Driving it with control-dbt-olids` starts with `Preconditions:` and uses labeled bullets that pair each user action with an exact command and observable result.
4. `Gotchas` lists traps that can waste or invalidate a verification run.

Keep implementation details out of the map. Name only user paths, stable selectors, required state, commands, and observable proof.

## Features

- [Nightly OLIDS build](./nightly-build.md) covers the scheduled `dbt build --exclude tag:synapse` path and changed-model selectors.
- [Stable-layer tests](./stable-tests.md) covers `dbt test -s tag:stable` and a single-model test such as `stable_patient`.
- [Landing cache](./landing-cache.md) covers `dbt build -s tag:landing` and landing column-contract tests.
- [Source schema contract](./source-contract.md) covers the live `sources.yml` comparison and parse-only doctor check.
- [Scheduled pipeline gates](./scheduled-gates.md) covers watermark unit tests and the live watermark script.
