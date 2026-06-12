{{
    config(
        secure=true,
        alias='practitioner')
}}

/*
Base PRACTITIONER View
Filters to practitioners employed by WNL practices via PRACTITIONER_IN_ROLE.
The source-side record_owner_organisation_code field was removed in the latest
OLIDS release, so we filter by practitioner_id appearing in our WNL-filtered
PRACTITIONER_IN_ROLE.
*/

SELECT
    src.lds_source_record_id,
    src.id,
    src.gmc_code,
    src.title,
    src.first_name,
    src.surname,
    src.name,
    src.is_obsolete,
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
FROM {{ source('olids_common', 'PRACTITIONER') }} src
WHERE src.id IN (
    SELECT DISTINCT practitioner_id
    FROM {{ ref('base_olids_practitioner_in_role') }}
    WHERE practitioner_id IS NOT NULL
)
