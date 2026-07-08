{{
    config(
        secure=true,
        alias='schedule_practitioner')
}}

/*
Base SCHEDULE_PRACTITIONER view.
Restricts to WNL practices.
*/

SELECT
    src.id,
    src.lds_source_record_id,
    src.publisher_organisation_id,
    src.provider_organisation_id,
    src.author_organisation_id,
    src.schedule_id,
    src.practitioner_id,
    src.lds_is_deleted,
    src.publisher_organisation_code,
    src.source_extraction_date,
    src.lds_transform_datetime
FROM {{ ref('landing_schedule_practitioner') }} AS src
INNER JOIN {{ ref('int_wnl_practices') }} AS wnl_practices
    ON src.publisher_organisation_code = wnl_practices.practice_code
