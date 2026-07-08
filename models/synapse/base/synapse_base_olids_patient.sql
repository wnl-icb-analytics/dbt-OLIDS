{{
    config(
        secure=true,
        alias='patient')
}}

/*
Base Filtered Patient View
Filters out sensitive patients and restricts to WNL practices only.
Used as the foundation for all patient-related clinical data filtering.

Exclusions:
- Patients with is_spine_sensitive = TRUE
- Patients with is_confidential = TRUE
- Patients with is_test_patient = TRUE
- Patients from non-WNL practices
*/

SELECT
    src.lds_source_record_id,
    src.id,
    src.person_id,
    src.nhs_number_hash,
    src.sk_patient_id,
    src.local_patient_id,
    src.title,
    src.gender_source_concept_id,
    gender_map.source_code AS gender_source_code,
    gender_map.source_display AS gender_source_display,
    gender_map.target_code AS gender_code,
    gender_map.target_display AS gender_display,
    src.publisher_organisation_id,
    src.registered_practice_organisation_id,
    src.birth_year,
    src.birth_month,
    src.death_year,
    src.death_month,
    src.is_confidential,
    src.is_test_patient,
    src.is_spine_sensitive,
    src.publisher_organisation_code,
    src.patient_shard_id,
    src.person_shard_id,
    src.lds_source_record_shard_id,
    src.lds_id,
    src.lds_business_key,
    src.lds_source_dataset_id,
    src.lds_cdm_event_id,
    src.lds_versioner_event_id,
    src.lds_datetime_first_acquired,
    src.lds_datetime_update_acquired,
    src.lds_is_deleted,
    src.lds_start_datetime,
    src.lds_lakehouse_date_processed,
    src.lds_lakehouse_datetime_updated
FROM {{ source('olids_masked', 'PATIENT') }} AS src
INNER JOIN {{ ref('synapse_int_wnl_practices') }} AS wnl_practices
    ON src.publisher_organisation_code = wnl_practices.practice_code
LEFT JOIN {{ ref('synapse_int_enriched_concept_map') }} AS gender_map
    ON src.gender_source_concept_id = gender_map.source_concept_id
WHERE
    src.sk_patient_id IS NOT NULL
    AND src.is_spine_sensitive = FALSE
    AND src.is_confidential = FALSE
    AND src.is_test_patient = FALSE
