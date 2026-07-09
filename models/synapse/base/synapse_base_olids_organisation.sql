{{
    config(
        secure=true,
        alias='organisation')
}}

/*
Base ORGANISATION View
Reference data - no filtering applied.
Pattern: Global reference table.
Note: source column ORGANISATION_CODE_ASSINGING_AUTHORITY contains the upstream
typo (was ASSIGNING_AUTHORITY_CODE). Kept as-is for source fidelity.
*/

SELECT
    src.lds_source_record_id,
    src.id,
    src.organisation_code,
    src.organisation_code_assinging_authority,
    src.name,
    src.description,
    src.location_type_source_concept_id,
    src.postcode,
    src.parent_organisation_id,
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
FROM {{ source('olids_common', 'ORGANISATION') }} AS src
WHERE
    src.organisation_code IS NOT NULL
    AND src.lds_start_datetime IS NOT NULL
