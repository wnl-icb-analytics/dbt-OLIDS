{{
    config(
        materialized='table',
        alias='person')
}}

/*
Base PERSON View
Sources native OLIDS_MASKED.PERSON, filtered to persons linked to NCL patients
via the PATIENT_PERSON bridge.
Pattern: id is assigned by the person id index; person_uuid = native UUID.

Gender backfill: native PERSON.gender is currently 100% null upstream, so we
fall back to the gender_source_concept_id from the person's most recently
registered PATIENT row, resolved via OLIDS_TERMINOLOGY.CONCEPT.display. When
native gender is populated upstream it takes precedence via COALESCE.

Note: as_at_date / gp_registration_date are TIMESTAMP_NTZ at source â€” passed
through unchanged.
*/

WITH gender_fallback AS (
    SELECT
        pp.person_uuid,
        c.display AS gender
    FROM {{ ref('synapse_base_olids_patient_person') }} AS pp
    INNER JOIN {{ ref('synapse_base_olids_patient') }} AS pat
        ON pp.patient_id = pat.id
    LEFT JOIN {{ ref('synapse_base_olids_concept') }} AS c
        ON pat.gender_source_concept_id = c.concept_id
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pp.person_uuid
        ORDER BY pat.lds_start_datetime DESC NULLS LAST
    ) = 1
)

SELECT
    person_idx.person_id AS id,
    per.id AS person_uuid,
    per.person_version_id,
    per.person_record_type,
    per.matched_nhs_no_hash,
    per.sk_patient_id,
    per.birth_year,
    per.birth_month,
    per.death_year,
    per.death_month,
    per.death_notification_status,
    per.postcode_hash,
    per.preferred_contact_method,
    per.nominated_pharmacy,
    per.dispensing_doctor,
    per.medical_appliance_supplier,
    per.gp_practice_code,
    per.gp_registration_date,
    per.as_at_date,
    per.sensitivity_flag,
    per.error_success_code,
    per.person_shard_id,
    per.lds_source_record_id,
    per.lds_source_record_shard_id,
    per.lds_id,
    per.lds_source_dataset_id,
    per.lds_cdm_event_id,
    per.lds_datetime_first_acquired_person,
    per.lds_datetime_update_acquired_person,
    per.lds_is_deleted,
    per.lds_start_datetime,
    per.lds_lakehouse_date_processed,
    per.lds_lakehouse_datetime_updated,
    COALESCE(per.gender, gf.gender) AS gender
FROM {{ source('olids_masked', 'PERSON') }} AS per
LEFT JOIN gender_fallback AS gf
    ON per.id = gf.person_uuid
LEFT JOIN {{ ref('person_id_index') }} AS person_idx
    ON per.id = person_idx.source_person_id
WHERE EXISTS (
    SELECT 1
    FROM {{ ref('synapse_base_olids_patient_person') }} AS pp
    WHERE pp.person_uuid = per.id
)
