{{
    config(
        secure=true,
        alias='location_contact')
}}

/*
Base LOCATION_CONTACT View
Reference data - no filtering applied.
Pattern: Global reference table.
*/

SELECT
    src.lds_source_record_id,
    src.id,
    src.location_id,
    src.is_primary_contact,
    src.contact_type,
    src.contact_type_source_concept_id,
    src.value,
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
FROM {{ source('olids_common', 'LOCATION_CONTACT') }} src
