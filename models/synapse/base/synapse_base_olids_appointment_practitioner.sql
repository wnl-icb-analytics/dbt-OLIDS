{{
    config(
        secure=true,
        alias='appointment_practitioner')
}}

/*
Base APPOINTMENT_PRACTITIONER View
Filters to WNL practices only.
Pattern: Infrastructure table with publisher_organisation_code.
Note: this table keeps older-style lds_record_id / lds_record_shard_id naming.
*/

SELECT
    src.lds_record_id,
    src.lds_record_shard_id,
    src.id,
    src.appointment_id,
    src.practitioner_id,
    src.publisher_organisation_id,
    src.provider_organisation_id,
    src.publisher_organisation_code,
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
FROM {{ source('olids_common', 'APPOINTMENT_PRACTITIONER') }} AS src
INNER JOIN {{ ref('synapse_int_wnl_practices') }} AS wnl_practices
    ON src.publisher_organisation_code = wnl_practices.practice_code
