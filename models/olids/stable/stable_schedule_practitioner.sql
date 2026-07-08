{{
    config(
        cluster_by=['schedule_id'],
        transient=false,
        alias='schedule_practitioner'
    )
}}

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
FROM {{ ref('base_olids_schedule_practitioner') }}
