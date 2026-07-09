-- =============================================================================
-- Person id cutover: consolidated backup and rotation script (dbt-OLIDS#263/#257)
--
-- DO NOT RUN until the data_lake repoint to the new OLIDS outputs is done.
-- Run as DBT_ADMIN, top to bottom, in one session.
--
-- What it does:
--   1. Zero-copy backups of every target (suffix __PRE_ID_ROTATION).
--   2. Rotates person ids via OLIDS_ENGINEERING.PSEUDONYMISATION.person_id_crosswalk
--      in the eight dbt snapshots (history cannot be regenerated) and the AIC
--      eFI2 scores table (we consume it; AIC informed, their own backups exist).
--   3. Verification: remaining old ids and collapsed-id duplicate checks.
--
-- Idempotent: updates only rows still holding an old id; backups use IF NOT EXISTS
-- so a re-run never overwrites the original backup.
-- Incremental dbt models are NOT here: rebuild them with --full-refresh.
-- =============================================================================

use role "DBT_ADMIN";

-- -----------------------------------------------------------------------------
-- 0. Preconditions
-- -----------------------------------------------------------------------------

-- crosswalk exists and is populated (expect ~2.7M rows)
select count(*) as crosswalk_rows,
       count(distinct new_person_id) as new_ids,
       count_if(is_collapsed) as collapsed_member_rows
from "OLIDS_ENGINEERING"."PSEUDONYMISATION"."PERSON_ID_CROSSWALK";

-- MANUAL GATE: confirm the data_lake mapping now points at the new OLIDS outputs
-- before continuing past this line.

-- -----------------------------------------------------------------------------
-- 1. Backups (zero-copy clones; kept until #257 is signed off)
-- -----------------------------------------------------------------------------

create table if not exists MODELLING.DBT_SNAPSHOTS.dim_person_care_home_snapshot__PRE_ID_ROTATION clone MODELLING.DBT_SNAPSHOTS.dim_person_care_home_snapshot;
create table if not exists MODELLING.DBT_SNAPSHOTS.dim_person_ccms_snapshot__PRE_ID_ROTATION clone MODELLING.DBT_SNAPSHOTS.dim_person_ccms_snapshot;
create table if not exists MODELLING.DBT_SNAPSHOTS.dim_person_conditions_snapshot__PRE_ID_ROTATION clone MODELLING.DBT_SNAPSHOTS.dim_person_conditions_snapshot;
create table if not exists MODELLING.DBT_SNAPSHOTS.dim_person_demographics_snapshot__PRE_ID_ROTATION clone MODELLING.DBT_SNAPSHOTS.dim_person_demographics_snapshot;
create table if not exists MODELLING.DBT_SNAPSHOTS.fct_person_behavioural_risk_factors_snapshot__PRE_ID_ROTATION clone MODELLING.DBT_SNAPSHOTS.fct_person_behavioural_risk_factors_snapshot;
create table if not exists MODELLING.DBT_SNAPSHOTS.fct_person_ltc_lcs_case_finding_snapshot__PRE_ID_ROTATION clone MODELLING.DBT_SNAPSHOTS.fct_person_ltc_lcs_case_finding_snapshot;
create table if not exists MODELLING.DBT_SNAPSHOTS.fct_person_ltc_lcs_risk_summary_snapshot__PRE_ID_ROTATION clone MODELLING.DBT_SNAPSHOTS.fct_person_ltc_lcs_risk_summary_snapshot;
create table if not exists MODELLING.DBT_SNAPSHOTS.fct_person_polypharmacy_current_snapshot__PRE_ID_ROTATION clone MODELLING.DBT_SNAPSHOTS.fct_person_polypharmacy_current_snapshot;
create table if not exists DATA_LAKE__NCL.AIC_DEV.INT_EFI2_SCORES__PRE_ID_ROTATION clone DATA_LAKE__NCL.AIC_DEV.INT_EFI2_SCORES;

-- -----------------------------------------------------------------------------
-- 2. Rotations
-- -----------------------------------------------------------------------------

update MODELLING.DBT_SNAPSHOTS.dim_person_care_home_snapshot as t
set person_id = x.new_person_id
from "OLIDS_ENGINEERING"."PSEUDONYMISATION"."PERSON_ID_CROSSWALK" as x
where t.person_id = x.old_person_id;

update MODELLING.DBT_SNAPSHOTS.dim_person_ccms_snapshot as t
set person_id = x.new_person_id
from "OLIDS_ENGINEERING"."PSEUDONYMISATION"."PERSON_ID_CROSSWALK" as x
where t.person_id = x.old_person_id;

update MODELLING.DBT_SNAPSHOTS.dim_person_conditions_snapshot as t
set person_id = x.new_person_id
from "OLIDS_ENGINEERING"."PSEUDONYMISATION"."PERSON_ID_CROSSWALK" as x
where t.person_id = x.old_person_id;

update MODELLING.DBT_SNAPSHOTS.dim_person_demographics_snapshot as t
set person_id = x.new_person_id
from "OLIDS_ENGINEERING"."PSEUDONYMISATION"."PERSON_ID_CROSSWALK" as x
where t.person_id = x.old_person_id;

update MODELLING.DBT_SNAPSHOTS.fct_person_behavioural_risk_factors_snapshot as t
set person_id = x.new_person_id
from "OLIDS_ENGINEERING"."PSEUDONYMISATION"."PERSON_ID_CROSSWALK" as x
where t.person_id = x.old_person_id;

update MODELLING.DBT_SNAPSHOTS.fct_person_ltc_lcs_case_finding_snapshot as t
set person_id = x.new_person_id
from "OLIDS_ENGINEERING"."PSEUDONYMISATION"."PERSON_ID_CROSSWALK" as x
where t.person_id = x.old_person_id;

update MODELLING.DBT_SNAPSHOTS.fct_person_ltc_lcs_risk_summary_snapshot as t
set person_id = x.new_person_id
from "OLIDS_ENGINEERING"."PSEUDONYMISATION"."PERSON_ID_CROSSWALK" as x
where t.person_id = x.old_person_id;

update MODELLING.DBT_SNAPSHOTS.fct_person_polypharmacy_current_snapshot as t
set person_id = x.new_person_id
from "OLIDS_ENGINEERING"."PSEUDONYMISATION"."PERSON_ID_CROSSWALK" as x
where t.person_id = x.old_person_id;

update DATA_LAKE__NCL.AIC_DEV.INT_EFI2_SCORES as t
set person_id = x.new_person_id
from "OLIDS_ENGINEERING"."PSEUDONYMISATION"."PERSON_ID_CROSSWALK" as x
where t.person_id = x.old_person_id;

-- -----------------------------------------------------------------------------
-- 3. Verification
-- -----------------------------------------------------------------------------

-- remaining old ids per table (expect 0 in every row)
select 'dim_person_care_home_snapshot' as t, count(*) as remaining_old_ids from MODELLING.DBT_SNAPSHOTS.dim_person_care_home_snapshot s join "OLIDS_ENGINEERING"."PSEUDONYMISATION"."PERSON_ID_CROSSWALK" x on s.person_id = x.old_person_id
union all select 'dim_person_ccms_snapshot', count(*) from MODELLING.DBT_SNAPSHOTS.dim_person_ccms_snapshot s join "OLIDS_ENGINEERING"."PSEUDONYMISATION"."PERSON_ID_CROSSWALK" x on s.person_id = x.old_person_id
union all select 'dim_person_conditions_snapshot', count(*) from MODELLING.DBT_SNAPSHOTS.dim_person_conditions_snapshot s join "OLIDS_ENGINEERING"."PSEUDONYMISATION"."PERSON_ID_CROSSWALK" x on s.person_id = x.old_person_id
union all select 'dim_person_demographics_snapshot', count(*) from MODELLING.DBT_SNAPSHOTS.dim_person_demographics_snapshot s join "OLIDS_ENGINEERING"."PSEUDONYMISATION"."PERSON_ID_CROSSWALK" x on s.person_id = x.old_person_id
union all select 'fct_person_behavioural_risk_factors_snapshot', count(*) from MODELLING.DBT_SNAPSHOTS.fct_person_behavioural_risk_factors_snapshot s join "OLIDS_ENGINEERING"."PSEUDONYMISATION"."PERSON_ID_CROSSWALK" x on s.person_id = x.old_person_id
union all select 'fct_person_ltc_lcs_case_finding_snapshot', count(*) from MODELLING.DBT_SNAPSHOTS.fct_person_ltc_lcs_case_finding_snapshot s join "OLIDS_ENGINEERING"."PSEUDONYMISATION"."PERSON_ID_CROSSWALK" x on s.person_id = x.old_person_id
union all select 'fct_person_ltc_lcs_risk_summary_snapshot', count(*) from MODELLING.DBT_SNAPSHOTS.fct_person_ltc_lcs_risk_summary_snapshot s join "OLIDS_ENGINEERING"."PSEUDONYMISATION"."PERSON_ID_CROSSWALK" x on s.person_id = x.old_person_id
union all select 'fct_person_polypharmacy_current_snapshot', count(*) from MODELLING.DBT_SNAPSHOTS.fct_person_polypharmacy_current_snapshot s join "OLIDS_ENGINEERING"."PSEUDONYMISATION"."PERSON_ID_CROSSWALK" x on s.person_id = x.old_person_id
union all select 'AIC INT_EFI2_SCORES', count(*) from DATA_LAKE__NCL.AIC_DEV.INT_EFI2_SCORES s join "OLIDS_ENGINEERING"."PSEUDONYMISATION"."PERSON_ID_CROSSWALK" x on s.person_id = x.old_person_id;

-- collapsed-identity review queue: persons whose history now merges (231 crosswalk rows)
select x.new_person_id, count(distinct x.old_person_id) as merged_old_ids
from "OLIDS_ENGINEERING"."PSEUDONYMISATION"."PERSON_ID_CROSSWALK" x
where x.is_collapsed
group by x.new_person_id
order by merged_old_ids desc;
