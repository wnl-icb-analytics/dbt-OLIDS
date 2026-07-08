{{
    config(
        cluster_by=['id'],
        transient=false,
        alias='practitioner'
    )
}}

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
    publisher_organisation_code,
    source_extraction_date,
    lds_transform_datetime
FROM {{ ref('conformed_practitioner') }}
