# Person Id Cutover Runbook

Status: NOT YET EXECUTED.

This notes the read-only survey of `C:\Projects\wnl-icb-analytics\dbt-analytics` for #257/#263. The OLIDS crosswalk is `OLIDS_ENGINEERING.PSEUDONYMISATION.person_id_crosswalk`.

Many-to-one note: collapsed identities mean several old 14-digit ids can rotate to one new id. Scripts only update ids. Objects marked as needing dedupe review may contain duplicate logical rows after rotation and before rebuild.

## Survey Results

| Object | Kind | How person_id is stored | Rotation approach | Dedupe handling | Risk notes |
| --- | --- | --- | --- | --- | --- |
| `MODELLING.DBT_SNAPSHOTS.dim_person_care_home_snapshot` | snapshot | `person_id` snapshot key in SCD2 rows | Run script `001_rotate_dim_person_care_home_snapshot.sql` | Review overlapping versions for collapsed ids | Current and history rows may merge under one person. |
| `MODELLING.DBT_SNAPSHOTS.dim_person_ccms_snapshot` | snapshot | `person_id` snapshot key in SCD2 rows | Run script `002_rotate_dim_person_ccms_snapshot.sql` | Review overlapping versions for collapsed ids | Score history may contain two prior persons under one id. |
| `MODELLING.DBT_SNAPSHOTS.dim_person_conditions_snapshot` | snapshot | `person_id` snapshot key in SCD2 rows | Run script `003_rotate_dim_person_conditions_snapshot.sql` | Review overlapping versions for collapsed ids | Condition history may need coalescing by validity window. |
| `MODELLING.DBT_SNAPSHOTS.dim_person_demographics_snapshot` | snapshot | `person_id` snapshot key in SCD2 rows | Run script `004_rotate_dim_person_demographics_snapshot.sql` | Review overlapping versions for collapsed ids | Demographic history can conflict where source persons collapse. |
| `MODELLING.DBT_SNAPSHOTS.fct_person_behavioural_risk_factors_snapshot` | snapshot | `person_id` snapshot key in SCD2 rows | Run script `005_rotate_fct_person_behavioural_risk_factors_snapshot.sql` | Review overlapping versions for collapsed ids | Risk factor rows may merge by person. |
| `MODELLING.DBT_SNAPSHOTS.fct_person_ltc_lcs_case_finding_snapshot` | snapshot | `person_id` snapshot key in SCD2 rows | Run script `006_rotate_fct_person_ltc_lcs_case_finding_snapshot.sql` | Review overlapping versions for collapsed ids | Indicator stints may overlap after rotation. |
| `MODELLING.DBT_SNAPSHOTS.fct_person_ltc_lcs_risk_summary_snapshot` | snapshot | `person_id` snapshot key in SCD2 rows | Run script `007_rotate_fct_person_ltc_lcs_risk_summary_snapshot.sql` | Review overlapping versions for collapsed ids | Risk group stints may overlap after rotation. |
| `MODELLING.DBT_SNAPSHOTS.fct_person_polypharmacy_current_snapshot` | snapshot | `person_id` snapshot key in SCD2 rows | Run script `008_rotate_fct_person_polypharmacy_current_snapshot.sql` | Review overlapping versions for collapsed ids | Medication burden history may merge by person. |
| `MODELLING.OLIDS_OBSERVATIONS.int_blood_pressure_observations_base` | incremental model | `person_id` column | Rebuild with `--full-refresh` after repointing sources | Not needed on full refresh | No prepared script; regeneration replaces state. |
| `REPORTING.OLIDS_PERSON_ANALYTICS.person_month_analysis_base` | incremental model | `person_id` column | Rebuild with `--full-refresh` after repointing sources | Collapsed identities merge naturally in the rebuild | No prepared script; regeneration replaces state. |

## Surveyed Out Of Scope

| Object or group | Kind | Finding | Cutover action |
| --- | --- | --- | --- |
| `MODELLING.DBT_SNAPSHOTS.cltcs_cohort_membership_snapshot` | snapshot | Key is commissioning `patient_id`, sourced from `sk_patient_id` | Do not rotate with the person crosswalk. |
| `MODELLING.DBT_SNAPSHOTS.cltcs_score_frailty_snapshot` | snapshot | Key is `sk_patient_id` | Do not rotate with the person crosswalk. |
| `MODELLING.DBT_SNAPSHOTS.cltcs_score_treatment_snapshot` | snapshot | Key is `sk_patient_id` | Do not rotate with the person crosswalk. |
| `MODELLING.DBT_SNAPSHOTS.fct_person_myria_control_group_published_snapshot` | snapshot | Key is commissioning `patient_id`, sourced from `sk_patient_id` | Do not rotate with the person crosswalk. |
| `MODELLING.DBT_SNAPSHOTS.fct_person_myria_high_risk_patients_published_snapshot` | snapshot | Key is commissioning `patient_id`, sourced from `sk_patient_id` | Do not rotate with the person crosswalk. |
| `REPORTING.COMMISSIONING_REPORTING.cltcs_activity_monthly_capture` | incremental model | Stores `sk_patient_id`, not OLIDS `person_id` | Do not rotate with the person crosswalk. |
| Seeds | seed | No seed column or content carrying `person_id`, `patient_id`, or `sk_patient_id` was found | No action. |
| Analyses | analysis | 25 files reference person or patient ids | No persisted state. Re-run only if used during cutover QA. |
| Macros | macro | 41 files reference person or patient ids | No persisted state. Rebuilt models receive rotated ids after source cutover. |
| 14-digit literals | code search | Matches were SNOMED or dm+d concept codes, not person ids | No action. |
| Non-incremental table models | model | 603 table models reference `person_id` in compiled model SQL | Rebuild after repointing sources. No prepared update scripts. |

## External Person Id Estates (outside dbt-analytics)

Swept DATA_LAKE__NCL for PERSON_ID columns and shape-checked content. These are owned by
other teams; we do not rotate them. The crosswalk is available to any owner who cannot
regenerate.

| Estate | Scale | Finding | Cutover action |
| --- | --- | --- | --- |
| `AIC_DEV` (efi2, CCMS, base copies, ~40 tables) | 2.3M rows in each key table | All person ids are old 14-digit values. `__BACKUP_OLIDS2026` tables show the team is already preparing. | dbt-analytics calculates CCMS itself now, so the only live dependency is `INT_EFI2_SCORES`: we back it up and rotate it via the crosswalk in the consolidated script (AIC informed). The rest of the AIC estate is theirs to regenerate. |
| `PHENOLAB_DEV.DEV_MEASUREMENTS` | 948M rows | All person ids are old 14-digit values. Largest single external store of old ids. | Owner regenerates after cutover; offer the crosswalk if any state cannot be rebuilt. |
| `SDL.DA`, `SDL.OUM` and variants | 0.7M to 1.7M rows | Ids are 6 to 8 digits: sk-family, not OLIDS person ids. | No action. |

Sequencing consequence for the main runbook: notify AIC and PhenoLab at the repoint. No
snapshot is sequenced behind AIC any more (CCMS is calculated in dbt-analytics), and the
eFI2 rotation is included in our consolidated script.

## Cutover Sequence

NOT YET EXECUTED.

1. Freeze dbt-analytics runs and consumer writes that depend on OLIDS person ids.
2. Confirm `OLIDS_ENGINEERING.PSEUDONYMISATION.person_id_crosswalk` exists and covers legacy archive ids.
3. Back up every target relation listed in the migration README.
4. Run `scripts/migration/run_all_rotations.sql` as DBT_ADMIN: zero-copy backups, all eight snapshot rotations, the AIC eFI2 rotation, and the verification queries in one pass. (The numbered per-table scripts remain for selective re-runs.)
5. Review collapsed-id outputs for objects marked as needing dedupe review.
6. Repoint dbt-analytics sources to the new OLIDS person ids.
7. Rebuild dbt-analytics non-incremental models that carry `person_id`.
8. Rebuild the two incremental models with `--full-refresh` (no in-place rotation needed).
9. Resume scheduled runs.
10. Verify row counts, old-id count, duplicate keys, and representative person histories.

## Checks To Run At Cutover

Use these checks after the scripts run, with relation names adjusted where needed.

```sql
select count(*) as remaining_old_ids
from MODELLING.DBT_SNAPSHOTS.dim_person_demographics_snapshot t
join OLIDS_ENGINEERING.PSEUDONYMISATION.person_id_crosswalk x
  on t.person_id = x.old_person_id;
```

```sql
select person_id, analysis_month, count(*) as row_count
from REPORTING.OLIDS_PERSON_ANALYTICS.person_month_analysis_base
group by person_id, analysis_month
having count(*) > 1;
```

The prepared scripts have not been run.
