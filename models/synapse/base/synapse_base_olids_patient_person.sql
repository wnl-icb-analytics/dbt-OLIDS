{{
    config(
        secure=true,
        alias='patient_person')
}}

/*
Base PATIENT_PERSON View
Filters to NCL practices through patient relationships.
Pattern: Bridge table with indexed person and patient keys.
Note: source-side `id` column was removed in the latest OLIDS release.
*/

SELECT
    src.lds_source_record_id,
    src.lds_source_record_id_person,
    patient_idx.patient_id,
    person_idx.person_id,
    src.person_id AS person_uuid,
    src.publisher_organisation_code,
    src.patient_shard_id,
    src.person_shard_id,
    src.lds_source_record_shard_id,
    src.lds_id,
    src.lds_business_key,
    src.lds_source_dataset_id,
    src.lds_cdm_event_id,
    src.lds_registrar_event_id,
    src.lds_datetime_update_acquired,
    src.lds_datetime_update_acquired_person,
    src.lds_is_deleted,
    src.lds_start_datetime,
    src.lds_end_datetime,
    src.lds_lakehouse_date_processed,
    src.lds_lakehouse_datetime_updated
FROM {{ source('olids_common', 'PATIENT_PERSON') }} AS src
INNER JOIN {{ ref('synapse_base_olids_patient') }} AS patients
    ON src.patient_id = patients.source_id
LEFT JOIN {{ ref('patient_id_index') }} AS patient_idx
    ON src.patient_id = patient_idx.source_patient_id
LEFT JOIN {{ ref('person_id_index') }} AS person_idx
    ON src.person_id = person_idx.source_person_id
WHERE src.lds_start_datetime IS NOT NULL
