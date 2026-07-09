{{
    config(
        secure=true,
        alias='practitioner')
}}

/*
Conformed PRACTITIONER view.
Keeps practitioners linked to WNL roles.
*/

SELECT
    src.id,
    src.lds_source_record_id,
    src.publisher_organisation_id,
    src.author_organisation_id,
    src.gmc_code,
    src.title,
    src.first_name,
    src.surname,
    src.name,
    src.is_obsolete,
    src.lds_is_deleted,
    src.publisher_organisation_code,
    src.source_extraction_date,
    src.lds_transform_datetime
FROM {{ ref('landing_practitioner') }} AS src
WHERE src.id IN (
    SELECT DISTINCT practitioner_id
    FROM {{ ref('conformed_practitioner_in_role') }}
    WHERE practitioner_id IS NOT NULL
)
