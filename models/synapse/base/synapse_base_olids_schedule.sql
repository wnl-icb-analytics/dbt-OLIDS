{{
    config(
        secure=true,
        alias='schedule')
}}

/*
Base SCHEDULE View
Filters to WNL practices only.
Pattern: Infrastructure table with publisher_organisation_code.
*/

SELECT
    src.lds_source_record_id,
    src.id,
    src.location_id,
    src.location_name,
    src.practitioner_id,
    src.publisher_organisation_id,
    src.start_date,
    src.end_date,
    src.type,
    src.name,
    src.is_private,
    src.publisher_organisation_code,
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
FROM {{ source('olids_common', 'SCHEDULE') }} AS src
INNER JOIN {{ ref('synapse_int_wnl_practices') }} AS wnl_practices
    ON src.publisher_organisation_code = wnl_practices.practice_code
