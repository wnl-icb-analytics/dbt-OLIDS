{{
    config(alias='PRACTITIONER')
}}

/*
Single daily scan through source policies. Downstream models read this cache.
*/

SELECT
    id,
    lds_source_record_id,
    publisher_organisation_id,
    author_organisation_id,
    gmc_code,
    title,
    first_name,
    surname,
    name,
    is_obsolete,
    lds_is_deleted,
    source_extraction_date,
    lds_transform_datetime
FROM {{ source('olids_pseudo', 'PRACTITIONER') }}
