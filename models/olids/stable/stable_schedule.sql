{{
    config(
        cluster_by=['start_datetime'],
        transient=false,
        alias='schedule'
    )
}}

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
    lds_transform_datetime
FROM {{ ref('conformed_schedule') }}
