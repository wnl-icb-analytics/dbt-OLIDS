{{
    config(
        materialized='table',
        alias='patient')
}}

/*
Conformed filtered patient view.
Excludes sensitive, confidential and test patients, then restricts to WNL practices.
*/

SELECT
    patient_idx.patient_id AS id,
    src.id AS source_id,
    src.lds_source_record_id,
    person_idx.person_id,
    src.publisher_organisation_id,
    src.provider_organisation_id,
    src.author_organisation_id,
    src.registered_practice_organisation_id,
    src.local_patient_id,
    src.title,
    src.gender_source_concept_id,
    gender_map.source_code AS gender_source_code,
    gender_map.source_display AS gender_source_display,
    gender_map.target_code AS gender_code,
    gender_map.target_display AS gender_display,
    src.birth_date,
    src.birth_year,
    src.birth_month,
    src.birth_week_iso,
    src.birth_day,
    src.death_date,
    src.death_year,
    src.death_month,
    src.death_week_iso,
    src.is_confidential,
    src.is_test_patient,
    src.is_spine_sensitive,
    src.lds_source_dataset,
    src.lds_is_deleted,
    src.publisher_organisation_code,
    src.source_extraction_date,
    src.lds_transform_datetime,
    TRY_TO_NUMBER(src.sk_patient_id) AS sk_patient_id
FROM {{ ref('landing_patient') }} AS src
INNER JOIN {{ ref('int_wnl_practices') }} AS wnl_practices
    ON src.publisher_organisation_code = wnl_practices.practice_code
LEFT JOIN {{ ref('patient_id_index') }} AS patient_idx
    ON src.id = patient_idx.source_patient_id
LEFT JOIN {{ ref('person_id_index') }} AS person_idx
    ON src.person_id = person_idx.source_person_id
LEFT JOIN {{ ref('int_concept_map_best') }} AS gender_map
    ON src.gender_source_concept_id = gender_map.source_concept_id
WHERE
    TRY_TO_NUMBER(src.sk_patient_id) IS NOT NULL
    -- strict = FALSE: rows with NULL flags are excluded (unknown sensitivity treated as sensitive)
    AND src.is_spine_sensitive = FALSE
    AND src.is_confidential = FALSE
    AND src.is_test_patient = FALSE
