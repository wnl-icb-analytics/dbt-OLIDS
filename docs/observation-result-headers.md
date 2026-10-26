# Observation result headers

`stable_observation.is_result_header` identifies an empty native observation
whose numeric result is held by a direct child. The flag preserves every row;
latest-result consumers can exclude headers before ranking.

Both observations must be live, on the same patient record and clinical calendar
date. The parent has neither `result_value` nor `result_text`. The child has a
numeric `result_value`. Either their mapped concept codes match, or their source
codes and source systems match. Missing dates or concepts do not establish a
match. Problem status does not establish or prevent a match.

Equal save times are not required. Most children share the header's save second;
the rest were saved shortly after it. Requiring the same second would split one
save sequence, so the rule uses the direct link and clinical date. Cross-date
links stay unflagged. A newer empty entry without a matching child remains
eligible, even when an older numeric result exists.

The intermediate model has one row per flagged ID. STABLE joins by ID and defaults
the boolean to false. Added allergy and referral records are never headers.

## Validation

The synthetic fixture covers deletion, text, concept matching, patient and date
boundaries, later saves, multiple children, missing fields and genuine empty
entries. It also covers a problem-flagged empty parent with a valued child.
Fusion cannot infer native Snowflake UUID unit-test output schemas.
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
