{{
    config(
        secure=true,
        alias='episode_of_care')
}}

/*
Base EPISODE_OF_CARE View
Filters to WNL practices and excludes sensitive patients.
Pattern: Clinical table with patient_id + publisher_organisation_code
Surfaces indexed person and patient keys.
*/

SELECT
    src.lds_source_record_id,
    src.id,
    src.publisher_organisation_id,
    src.provider_organisation_id,
    src.author_organisation_id,
    src.care_manager_organisation_id,
    patient_idx.patient_id,
    person_idx.person_id,
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
    src.care_manager_practitioner_in_role_id,
    src.publisher_organisation_code,
    src.care_manager_organisation_code,
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
FROM {{ source('olids_common', 'EPISODE_OF_CARE') }} AS src
INNER JOIN {{ ref('synapse_base_olids_patient') }} AS patients
    ON src.patient_id = patients.source_id
LEFT JOIN {{ ref('patient_id_index') }} AS patient_idx
    ON src.patient_id = patient_idx.source_patient_id
LEFT JOIN {{ ref('person_id_index') }} AS person_idx
    ON src.person_id = person_idx.source_person_id
INNER JOIN {{ ref('synapse_int_wnl_practices') }} AS wnl_practices
    ON src.publisher_organisation_code = wnl_practices.practice_code
LEFT JOIN {{ ref('synapse_int_concept_map_best') }} AS episode_type_map
    ON src.episode_type_source_concept_id = episode_type_map.source_concept_id
LEFT JOIN {{ ref('synapse_int_concept_map_best') }} AS episode_status_map
    ON
        src.episode_status_source_concept_id
        = episode_status_map.source_concept_id
WHERE
    src.patient_id IS NOT NULL
    AND src.lds_start_datetime IS NOT NULL
