{{
    config(
        secure=true,
        alias='patient_uprn')
}}

/*
Base PATIENT_UPRN View
Reference data - no filtering applied.
Pattern: Global reference table.
masked_uprn / masked_usrn / masked_postcode are now BINARY.
*/

SELECT
    src.lds_source_record_id,
    src.id,
    patient_idx.patient_id,
    person_idx.person_id,
    src.patient_address_id,
    src.publisher_organisation_id,
    src.provider_organisation_id,
    src.author_organisation_id,
    src.masked_uprn,
    src.masked_usrn,
    src.masked_postcode,
    src.address_format_quality,
    src.postcode_quality,
    src.matched_with_assign,
    src.qualifier,
    src.classification,
    src.algorithm,
    src.match_pattern,
    src.publisher_organisation_code,
    src.patient_shard_id,
    src.person_shard_id,
    src.lds_source_record_shard_id,
    src.lds_id,
    src.lds_source_dataset_id,
    src.lds_cdm_event_id,
    src.lds_registrar_event_id,
    src.lds_datetime_update_acquired,
    src.lds_is_deleted,
    src.lds_start_datetime,
    src.lds_lakehouse_date_processed,
    src.lds_lakehouse_datetime_updated
FROM {{ source('olids_masked', 'PATIENT_UPRN') }} AS src
LEFT JOIN {{ ref('patient_id_index') }} AS patient_idx
    ON src.patient_id = patient_idx.source_patient_id
LEFT JOIN {{ ref('person_id_index') }} AS person_idx
    ON src.person_id = person_idx.source_person_id
