{{
    config(
        cluster_by=['id'],
        transient=false,
        alias='practitioner_in_role'
    )
}}

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
    publisher_organisation_code,
    source_extraction_date,
    lds_transform_datetime
FROM {{ ref('conformed_practitioner_in_role') }}
