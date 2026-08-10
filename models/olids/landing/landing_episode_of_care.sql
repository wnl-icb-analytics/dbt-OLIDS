{{
    config(alias='EPISODE_OF_CARE')
}}

/*
Single daily scan through source policies, scoped to NCL practices.
Downstream models read this cache.
*/

SELECT
    id,
    lds_source_record_id,
    patient_id,
    person_id,
    publisher_organisation_id,
    managing_organisation_id,
    author_organisation_id,
    managing_organisation_code,
    usual_gp_practitioner_in_role_id,
    episode_of_care_start_date,
    episode_of_care_end_date,
    type,
    episode_type_source_concept_id,
    status,
    episode_status_source_concept_id,
    lds_is_deleted,
    publisher_organisation_code,
    source_extraction_date,
    lds_transform_datetime
FROM {{ source('olids_pseudo', 'EPISODE_OF_CARE') }}
WHERE publisher_organisation_code IN (
    SELECT ncl.practice_code FROM {{ ref('int_ncl_practices') }} AS ncl
)
