{{
    config(
        secure=true,
        alias='patient_contact')
}}

/*
Base PATIENT_CONTACT View
Filters to NCL practices and excludes sensitive patients.
Pattern: Clinical table with patient_id + record_owner_organisation_code
Uses native person_id from source table.
*/

SELECT
    src.lds_record_id,
    src.id,
    src.person_id,
    src.patient_id,
    src.description,
    src.contact_type_concept_id,
    src.start_date,
    src.end_date,
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
FROM {{ source('olids_masked', 'PATIENT_CONTACT') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src.patient_id = patients.id
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src.record_owner_organisation_code = ncl_practices.practice_code
