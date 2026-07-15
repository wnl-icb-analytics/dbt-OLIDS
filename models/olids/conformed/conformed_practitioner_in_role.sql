{{
    config(
        secure=true,
        alias='practitioner_in_role')
}}

/*
Conformed PRACTITIONER_IN_ROLE view.
Restricts to WNL practices. The feed no longer carries
publisher_organisation_code, so it is derived via publisher_organisation_id
to keep the published interface unchanged.
*/

SELECT
    src.id,
    src.lds_source_record_id,
    src.publisher_organisation_id,
    src.author_organisation_id,
    src.practitioner_id,
    src.employer_organisation_id,
    src.role_code,
    src.role,
    src.date_employment_start,
    src.date_employment_end,
    src.lds_is_deleted,
    pub.organisation_code AS publisher_organisation_code,
    src.source_extraction_date,
    src.lds_transform_datetime
FROM {{ ref('landing_practitioner_in_role') }} AS src
INNER JOIN {{ ref('landing_organisation') }} AS pub
    ON src.publisher_organisation_id = pub.id
INNER JOIN {{ ref('int_wnl_practices') }} AS wnl_practices
    ON pub.organisation_code = wnl_practices.practice_code
