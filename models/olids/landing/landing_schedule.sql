{{
    config(alias='SCHEDULE')
}}

/*
Single daily scan through source policies, scoped to NCL practices.
Downstream models read this cache.
*/

SELECT
    id,
    lds_source_record_id,
    publisher_organisation_id,
    provider_organisation_id,
    author_organisation_id,
    location_id,
    location_name,
    practitioner_id,
    start_datetime,
    end_datetime,
    type,
    name,
    is_private,
    lds_is_deleted,
    publisher_organisation_code,
    source_extraction_date,
    lds_source_dataset,
    lds_transform_datetime
FROM {{ source('olids_pseudo', 'SCHEDULE') }}
WHERE publisher_organisation_code IN (
    SELECT ncl.practice_code FROM {{ ref('int_ncl_practices') }} AS ncl
)
