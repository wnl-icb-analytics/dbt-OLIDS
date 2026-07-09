{{
    config(
        secure=true,
        alias='location')
}}

/*
Base LOCATION View
Reference data - no filtering applied.
Pattern: Global reference table.
*/

SELECT
    src.lds_source_record_id,
    src.id,
    src.name,
    src.location_type_source_concept_id,
    src.type_description,
    src.is_primary_location,
    src.house_name,
    src.house_number,
    src.house_name_flat_number,
    src.street,
    src.address_line_1,
    src.address_line_2,
    src.address_line_3,
    src.address_line_4,
    src.postcode,
    src.managing_organisation_id,
    src.open_date,
    src.close_date,
    src.is_obsolete,
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
FROM {{ source('olids_common', 'LOCATION') }} AS src
