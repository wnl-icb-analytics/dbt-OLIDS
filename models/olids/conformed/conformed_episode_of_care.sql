{{
    config(
        secure=true,
        alias='episode_of_care')
}}

/*
Conformed EPISODE_OF_CARE view.
Filters to WNL practices and excludes patients outside the filtered spine.
*/

SELECT
    src.id,
    src.lds_source_record_id,
    src.patient_id,
    {{ generate_person_id('src.person_id') }} AS person_id,
    src.publisher_organisation_id,
    src.managing_organisation_id,
    src.author_organisation_id,
    src.managing_organisation_code,
    src.usual_gp_practitioner_in_role_id,
    src.episode_of_care_start_date,
    src.episode_of_care_end_date,
    src.type,
    src.episode_type_source_concept_id,
    episode_type_map.source_code AS episode_type_source_code,
    episode_type_map.source_display AS episode_type_source_display,
    episode_type_map.target_code AS episode_type_code,
    episode_type_map.target_display AS episode_type_display,
    src.status,
    src.episode_status_source_concept_id,
    episode_status_map.source_code AS episode_status_source_code,
    episode_status_map.source_display AS episode_status_source_display,
    episode_status_map.target_code AS episode_status_code,
    episode_status_map.target_display AS episode_status_display,
    src.lds_is_deleted,
    src.publisher_organisation_code,
    src.source_extraction_date,
    src.lds_transform_datetime
FROM {{ ref('landing_episode_of_care') }} src
INNER JOIN {{ ref('conformed_patient') }} patients
    ON src.patient_id = patients.id
INNER JOIN {{ ref('int_wnl_practices') }} wnl_practices
    ON src.publisher_organisation_code = wnl_practices.practice_code
LEFT JOIN {{ ref('int_enriched_concept_map') }} episode_type_map
    ON src.episode_type_source_concept_id = episode_type_map.source_concept_id
LEFT JOIN {{ ref('int_enriched_concept_map') }} episode_status_map
    ON src.episode_status_source_concept_id = episode_status_map.source_concept_id
WHERE src.patient_id IS NOT NULL
