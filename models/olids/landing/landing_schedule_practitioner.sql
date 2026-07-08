{{
    config(alias='SCHEDULE_PRACTITIONER')
}}

/*
Single daily scan through source policies. Downstream models read this cache.
*/

SELECT
    id,
    lds_source_record_id,
    publisher_organisation_id,
    provider_organisation_id,
    author_organisation_id,
    schedule_id,
    practitioner_id,
    lds_is_deleted,
    publisher_organisation_code,
    source_extraction_date,
    lds_transform_datetime
FROM {{ source('olids_pseudo', 'SCHEDULE_PRACTITIONER') }}
