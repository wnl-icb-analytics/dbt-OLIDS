{{
    config(alias='PRACTITIONER_IN_ROLE')
}}

/*
Single daily scan through source policies. Downstream models read this cache.
*/

SELECT
    id,
    lds_source_record_id,
    publisher_organisation_id,
    author_organisation_id,
    practitioner_id,
    employer_organisation_id,
    role_code,
    role,
    date_employment_start,
    date_employment_end,
    lds_is_deleted,
    source_extraction_date,
    lds_transform_datetime
FROM {{ source('olids_pseudo', 'PRACTITIONER_IN_ROLE') }}
