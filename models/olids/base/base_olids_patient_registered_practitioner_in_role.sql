{{
    config(
        secure=true,
        alias='patient_registered_practitioner_in_role')
}}

/*
Base PATIENT_REGISTERED_PRACTITIONER_IN_ROLE View
Filters to WNL practices and excludes sensitive patients.
Pattern: Clinical-style table with patient_id + publisher_organisation_code.
Note: source-side practitioner_id was replaced by practitioner_in_role_id.
*/

SELECT
    src.lds_source_record_id,
    src.id,
    {{ generate_person_id('src.person_id') }} AS person_id,
    src.patient_id,
    src.publisher_organisation_id,
    src.practitioner_in_role_id,
    src.episode_of_care_id,
    src.start_date,
    src.end_date,
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
FROM {{ source('olids_common', 'PATIENT_REGISTERED_PRACTITIONER_IN_ROLE') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src.patient_id = patients.id
INNER JOIN {{ ref('int_wnl_practices') }} wnl_practices
    ON src.publisher_organisation_code = wnl_practices.practice_code
WHERE src.patient_id IS NOT NULL
    AND src.lds_start_datetime IS NOT NULL
