{{
    config(
        secure=true,
        alias='patient_person')
}}

/*
Base PATIENT_PERSON View
Filters to NCL practices through patient relationships.
Pattern: Bridge table with numeric person_id hashed from source UUID
*/

SELECT
    src.lds_record_id,
    src.lds_record_id_person,
    src.id,
    src.patient_id,
    {{ generate_person_id('src.person_id') }} AS person_id,
    src.person_id AS person_uuid,
    src.lds_id,
    src.lds_business_key,
    src.lds_dataset_id,
    src.lds_cdm_event_id,
    src.lds_datetime_data_acquired,
    src.lds_is_deleted,
    src.lds_start_date_time,
    src.lds_lakehouse_date_processed,
    src.lds_lakehouse_datetime_updated
FROM {{ source('olids_common', 'PATIENT_PERSON') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src.patient_id = patients.id
WHERE src.lds_start_date_time IS NOT NULL