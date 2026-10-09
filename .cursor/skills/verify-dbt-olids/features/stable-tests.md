# Stable-layer tests

Stable-layer tests assert uniqueness and not-null contracts on published OLIDS tables, plus project tests such as `assert_sk_patient_id_unique_per_person`.

## Sub-features

- `stable-tag` runs every test selected by `tag:stable`.
- `stable-patient` runs the documented single-model tests on `stable_patient`.
- `stable-build-tests` is the test portion bundled inside `dbt build -s tag:stable`.

## How to get to it (user POV)

- Run `dbt test -s tag:stable --target stable --profiles-dir .`.
- Run `dbt test -s stable_patient --target stable --profiles-dir .`.
- Run `dbt build -s tag:stable --target stable --profiles-dir .` when models and tests should run together.

## Driving it with control-dbt-olids

Preconditions:

- `control-dbt-olids doctor` reports `warehouse_ok=true`.
- The target database already has the stable models under test, or the drive uses `dbt build` so models are built first.
- `tests.store_failures` remains false.

- **Tag entry.** Test the stable layer. Run `control-dbt-olids cli --label stable-tag -- dbt test -s tag:stable --target stable --profiles-dir .`. Exit code `0` and `run_results.summary.json` counts have no `fail` or `error`.
- **Patient model.** Test one published table. Run `control-dbt-olids cli --label stable-patient -- dbt test -s stable_patient --target stable --profiles-dir .`. Exit code `0` and the summary includes unique/not-null tests on `stable_patient`.
- **Build-with-tests.** Build and test the tag together. Run `control-dbt-olids cli --label stable-build-tests -- dbt build -s tag:stable --target stable --profiles-dir .`. Exit code `0`; summary rows include both models and tests.
- **Proof.** Re-read statuses from `run_results.summary.json`. The file lists `unique_id` and `status` only. Stdout must not be copied into chat if it printed failure tables.

## Gotchas

- Failed unique/not-null tests can print identifier values. The helper redacts UUID tokens; still prefer the summary JSON over stdout.
- `dbt test -s tag:stable` does not rebuild stale tables. If the change is in a model, use `dbt build -s <model>` first or `stable-build-tests`.
- Elementary anomaly tests may warn without failing the run. Treat `warn` as observed, not as `success` unless the feature asked only for a non-error exit.
- Do not enable `store_failures` to "see the rows".
