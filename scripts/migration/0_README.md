# Person Id Cutover Scripts

Status: prepared only. Do not run before the #257 cutover.

Preconditions:

1. dbt-analytics runs are frozen.
2. `OLIDS_ENGINEERING.PSEUDONYMISATION.person_id_crosswalk` has been built.
3. Target tables are backed up.
4. The cutover owner has accepted the many-to-one collapse risk.

Run order:

1. `001_rotate_dim_person_care_home_snapshot.sql`
2. `002_rotate_dim_person_ccms_snapshot.sql`
3. `003_rotate_dim_person_conditions_snapshot.sql`
4. `004_rotate_dim_person_demographics_snapshot.sql`
5. `005_rotate_fct_person_behavioural_risk_factors_snapshot.sql`
6. `006_rotate_fct_person_ltc_lcs_case_finding_snapshot.sql`
7. `007_rotate_fct_person_ltc_lcs_risk_summary_snapshot.sql`
8. `008_rotate_fct_person_polypharmacy_current_snapshot.sql`

Each script is idempotent because it only updates rows still matching `old_person_id`.
After running, check for duplicate logical keys where collapsed ids merged rows.

Incremental dbt models are not rotated here: rebuild them with --full-refresh after repointing sources. These scripts exist only for snapshots, whose history cannot be regenerated.

Preferred execution: run_all_rotations.sql as DBT_ADMIN does everything in one pass
(backups, eight snapshots, the AIC eFI2 table we consume, verification). Do not run
until the data_lake repoint is done. The numbered scripts remain for selective re-runs.
