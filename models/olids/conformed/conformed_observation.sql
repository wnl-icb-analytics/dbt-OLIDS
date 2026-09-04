{{
    config(
        materialized='table',
        transient=true,
        alias='observation')
}}

/*
Conformed OBSERVATION view.
Filters to NCL practices and excludes patients outside the filtered spine.
*/

SELECT
    src.id,
    src.lds_source_record_id,
    patient_idx.patient_id,
    person_idx.person_id,
    src.publisher_organisation_id,
    src.provider_organisation_id,
    src.author_organisation_id,
    src.encounter_id,
    src.practitioner_id,
    src.parent_observation_id,
    src.clinical_effective_date,
    src.clinical_effective_date_precision_source_concept_id,
    date_precision_map.source_code AS date_precision_source_code,
    date_precision_map.source_display AS date_precision_source_display,
    date_precision_map.mapped_concept_code AS date_precision_code,
    date_precision_map.mapped_concept_display AS date_precision_display,
    src.result_value,
    src.result_value_units_source_concept_id,
    result_unit_map.source_code AS result_unit_source_code,
    result_unit_map.source_display AS result_unit_source_display,
    result_unit_map.mapped_concept_code AS result_unit_code,
    result_unit_map.mapped_concept_display AS result_unit_display,
    src.result_date,
    src.result_text,
    src.is_problem,
    src.is_review,
    src.problem_end_date,
    src.observation_source_concept_id,
    concept_map.source_code,
    concept_map.source_display,
    concept_map.source_system,
    concept_map.mapped_concept_id,
    concept_map.mapped_concept_code,
    concept_map.mapped_concept_display,
    concept_map.mapped_concept_system AS target_system,
    src.age_at_event,
    src.age_at_event_baby,
    src.age_at_event_neonate,
    src.episodicity_source_concept_id,
    src.is_primary,
    src.date_recorded,
    src.is_problem_deleted,
    src.is_confidential,
    src.lds_is_deleted,
    src.publisher_organisation_code,
    patients.clinical_system,
    src.source_extraction_date,
    src.lds_transform_datetime
FROM {{ ref('landing_observation') }} AS src
INNER JOIN {{ ref('conformed_patient') }} AS patients
    ON src.patient_id = patients.source_id
LEFT JOIN {{ ref('patient_id_index') }} AS patient_idx
    ON src.patient_id = patient_idx.source_patient_id
LEFT JOIN {{ ref('person_id_index') }} AS person_idx
    ON src.person_id = person_idx.source_person_id
INNER JOIN {{ ref('int_ncl_practices') }} AS ncl_practices
    ON src.publisher_organisation_code = ncl_practices.practice_code
LEFT JOIN {{ ref('conformed_concept_map') }} AS concept_map
    ON src.observation_source_concept_id = concept_map.source_concept_id
LEFT JOIN {{ ref('conformed_concept_map') }} AS date_precision_map
    ON
        src.clinical_effective_date_precision_source_concept_id
        = date_precision_map.source_concept_id
LEFT JOIN {{ ref('conformed_concept_map') }} AS result_unit_map
    ON
        src.result_value_units_source_concept_id
        = result_unit_map.source_concept_id
WHERE src.observation_source_concept_id IS NOT NULL
-- the feed occasionally ships exact duplicate rows; keep one deterministically
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY src.id
    ORDER BY src.lds_transform_datetime DESC, src.lds_source_record_id ASC
) = 1
