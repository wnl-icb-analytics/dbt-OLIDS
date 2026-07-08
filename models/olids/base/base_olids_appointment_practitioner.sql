{{
    config(
        secure=true,
        alias='appointment_practitioner')
}}

/*
Base APPOINTMENT_PRACTITIONER view.
Restricts to WNL practices.
*/

SELECT
    src.id,
    src.lds_source_record_id,
    src.patient_id,
    {{ generate_person_id('src.person_id') }} AS person_id,
    src.publisher_organisation_id,
    src.provider_organisation_id,
    src.author_organisation_id,
    src.lds_source_record_id_practitioner,
    src.appointment_id,
    src.practitioner_id,
    src.lds_is_deleted,
    src.publisher_organisation_code,
    src.source_extraction_date,
    src.lds_transform_datetime
FROM {{ ref('landing_appointment_practitioner') }} src
INNER JOIN {{ ref('int_wnl_practices') }} wnl_practices
    ON src.publisher_organisation_code = wnl_practices.practice_code
