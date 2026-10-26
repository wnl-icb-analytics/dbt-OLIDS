# Observation result headers

`stable_observation.is_result_header` identifies an empty native observation
whose numeric result is held by a direct child. The flag preserves every row;
latest-result consumers can exclude headers before ranking.

Both observations must be live, on the same patient record and clinical calendar
date. The parent has neither `result_value` nor `result_text`. The child has a
numeric `result_value`. Either their mapped concept codes match, or their source
codes and source systems match. Missing dates or concepts do not establish a
match. Problem status does not establish or prevent a match.

Equal save times are not required. Profiling on 26 October 2026 found about
8.07 million empty, same-concept parents with same-date numeric children.
About 214,000 had a child saved in a different second. The explicit child link
and matching clinical date retain these delayed or amended results while
excluding cross-date links. A newer empty entry without a matching child remains
eligible, even when an older numeric result exists.

The intermediate model has one row per flagged ID. STABLE joins by ID and defaults
the boolean to false. Added allergy and referral records are never headers.

## Validation

The synthetic fixture covers deletion, text, concept matching, patient and date
boundaries, later saves, multiple children, missing fields and genuine empty
entries. Fusion cannot infer native Snowflake UUID unit-test output schemas.
Run the compiled model against the fixture with
`scripts/checks/check_result_header_fixtures.py` instead. It returns aggregate
failure counts only.

## Deploy the paired change

1. Merge this change and build `int_observation_result_headers` and
   `stable_observation` with the established OLIDS production workflow.
2. Immediately republish `DATA_LAKE.OLIDS` views with
   `scripts/checks/deploy_data_lake_views.py`. A STABLE column change requires
   republication before consumers can use the column.
3. Confirm that `DATA_LAKE.OLIDS.OBSERVATION` exposes a non-null boolean
   `IS_RESULT_HEADER`.
4. Merge and deploy the paired dbt-analytics change. Build its generated raw and
   staging observation models before the affected LTC LCS consumers.

This repository currently has only a production `stable` target. Do not run a
production build as development validation or create task-specific warehouse
schemas. Validate the compiled flag with session temporary tables, and use the
existing analytics `dev` target and production-state deferral where available.
