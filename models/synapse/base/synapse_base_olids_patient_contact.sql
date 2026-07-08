{{
    config(
        secure=true,
        alias='patient_contact')
}}

/*
Base PATIENT_CONTACT View
Filters to WNL practices and excludes sensitive patients.
Pattern: Clinical table with patient_id + publisher_organisation_code
Uses native person_id from source table.
*/

SELECT
    src.lds_source_record_id,
    src.id,
    {{ generate_person_id_legacy('src.person_id') }} AS person_id,
    src.patient_id,
    src.publisher_organisation_id,
    src.provider_organisation_id,
    src.author_organisation_id,
    src.contact_type,
    src.contact_type_source_concept_id,
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
FROM {{ source('olids_masked', 'PATIENT_CONTACT') }} src
INNER JOIN {{ ref('synapse_base_olids_patient') }} patients
    ON src.patient_id = patients.id
INNER JOIN {{ ref('synapse_int_wnl_practices') }} wnl_practices
    ON src.publisher_organisation_code = wnl_practices.practice_code
