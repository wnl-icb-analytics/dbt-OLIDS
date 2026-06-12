{{
    config(
        secure=true,
        alias='practitioner_in_role')
}}

/*
Base PRACTITIONER_IN_ROLE View
Filters to WNL practices via employer_organisation_id.
Pattern: Infrastructure table — the source-side record_owner_organisation_code
field was removed in the latest OLIDS release, so we filter via the employer
organisation's code instead.
*/

SELECT
    src.lds_source_record_id,
    src.id,
    src.practitioner_id,
    src.employer_organisation_id,
    src.role_code,
    src.role,
    src.date_employment_start,
    src.date_employment_end,
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
FROM {{ source('olids_common', 'PRACTITIONER_IN_ROLE') }} src
INNER JOIN {{ ref('base_olids_organisation') }} org
    ON src.employer_organisation_id = org.id
INNER JOIN {{ ref('int_wnl_practices') }} wnl_practices
    ON org.organisation_code = wnl_practices.practice_code
