# Source schema contract

Source schema contract compares `models/sources.yml` (`olids_pseudo`) with live `INFORMATION_SCHEMA`. The scheduled workflow fails before dbt when they differ.

## Sub-features

- `contract-parse` loads `sources.yml` without connecting.
- `contract-live` runs the CI comparison against Snowflake.
- `contract-verbose` lists matching tables as well as diffs.

## How to get to it (user POV)

- Run `python scripts/checks/compare_sources_to_information_schema.py`.
- Run `python scripts/checks/compare_sources_to_information_schema.py --verbose`.
- Run `control-dbt-olids doctor` and read `sources_contract_ok`.

## Driving it with control-dbt-olids

Preconditions:

- `project_ok=true` for `contract-parse` (doctor).
- `warehouse_ok=true` for `contract-live` and `contract-verbose`.
- Check dependencies include PyYAML (`pip install -r requirements.txt` or the workflow's `uv pip install pyyaml snowflake-connector-python`).

- **Parse contract.** Confirm the YAML loads. Run `control-dbt-olids doctor`. `sources_contract_ok=true` and `sources_contract` names `Data_Store_OLIDS_WNL.OLIDS_PSEUDO` with a non-zero table count.
- **Live compare.** Gate as CI does. Run `control-dbt-olids cli --label contract-live -- python scripts/checks/compare_sources_to_information_schema.py`. Exit code `0` and stdout end with `Summary:` reporting `0 difference(s)`. Treat exit code `1` as drift only when that Summary line reports a non-zero difference count; exit code `1` without a Summary line is a connection, authentication, or comparison failure, not drift. Keep the table/column names, not row data.
- **Verbose compare.** Include matches. Run `control-dbt-olids cli --label contract-verbose -- python scripts/checks/compare_sources_to_information_schema.py --verbose`. Shared tables print `OK` with a column count.
- **Proof.** Doctor output plus `stdout.txt` from the live command. The summary line is the observable end state. Do not regenerate `sources.yml` from verification.

## Gotchas

- The script connects before it diffs. Missing auth is not "no drift".
- Identifiers in `sources.yml` are quoted (`'"Data_Store_OLIDS_WNL"'`). Doctor's parse output is unquoted.
- Live-only columns are drift even when dbt models ignore them. The sibling dbt test `assert_source_columns_captured_in_landing` is warn-only; this script is a hard CI gate.
- Do not paste account, user, or key material from connection errors. The helper redacts known env values.
