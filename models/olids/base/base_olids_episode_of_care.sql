{{
    config(
        secure=true,
        alias='episode_of_care')
}}

/*
Base EPISODE_OF_CARE View
Filters to NCL practices and excludes sensitive patients.
Pattern: Clinical table with patient_id + record_owner_organisation_code
Uses native person_id from source table.
*/

SELECT
    src.lds_record_id,
    src.id,
    src.organisation_id,
    src.patient_id,
    src.person_id,
    src.episode_type_source_concept_id,
    episode_type_map.source_code AS episode_type_source_code,
    episode_type_map.source_display AS episode_type_source_display,
    episode_type_map.target_code AS episode_type_code,
    episode_type_map.target_display AS episode_type_display,
    src.episode_status_source_concept_id,
    episode_status_map.source_code AS episode_status_source_code,
    episode_status_map.source_display AS episode_status_source_display,
    episode_status_map.target_code AS episode_status_code,
    episode_status_map.target_display AS episode_status_display,
    src.episode_of_care_start_date,
    src.episode_of_care_end_date,
    src.care_manager_practitioner_id,
    src.lds_id,
    src.lds_business_key,
    src.lds_dataset_id,
    src.lds_cdm_event_id,
    src.lds_versioner_event_id,
    src.record_owner_organisation_code,
    src.lds_datetime_data_acquired,
    src.lds_initial_data_received_date,
    src.lds_is_deleted,
    src.lds_start_date_time,
    src.lds_lakehouse_date_processed,
    src.lds_lakehouse_datetime_updated
FROM {{ source('olids_common', 'EPISODE_OF_CARE') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src.patient_id = patients.id
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src.record_owner_organisation_code = ncl_practices.practice_code
LEFT JOIN {{ ref('base_olids_concept_map') }} episode_type_map
    ON src.episode_type_source_concept_id = episode_type_map.source_code_id
LEFT JOIN {{ ref('base_olids_concept_map') }} episode_status_map
    ON src.episode_status_source_concept_id = episode_status_map.source_code_id
WHERE src.patient_id IS NOT NULL
    AND src.lds_start_date_time IS NOT NULL