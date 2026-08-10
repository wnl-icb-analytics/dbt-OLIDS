{{
    config(alias='PATIENT_PERSON')
}}

/*
Single daily scan through source policies. Downstream models read this cache.
*/

SELECT
    id,
    lds_source_record_id,
    patient_id,
    person_id,
    lds_business_id_person,
    lds_source_record_id_person,
    publisher_organisation_code,
    lds_is_deleted,
    lds_source_dataset,
    lds_transform_datetime
FROM {{ source('olids_pseudo', 'PATIENT_PERSON') }}
