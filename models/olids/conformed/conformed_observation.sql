{{
    config(
        materialized='table',
        transient=true,
        alias='observation')
}}

-- Grain: one record per source entity and source ID in the filtered patient spine.
-- Native observation IDs stay unchanged. Added entities use deterministic UUIDs;
-- their original IDs remain available for joins to the corresponding entity table.
WITH native_observations AS (
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
        date_precision_map.target_code AS date_precision_code,
        date_precision_map.target_display AS date_precision_display,
        src.result_value,
        src.result_value_units_source_concept_id,
        result_unit_map.source_code AS result_unit_source_code,
        result_unit_map.source_display AS result_unit_source_display,
        result_unit_map.target_code AS result_unit_code,
        result_unit_map.target_display AS result_unit_display,
        src.result_date,
        src.result_text,
        src.is_problem,
        src.is_review,
        src.problem_end_date,
        src.observation_source_concept_id,
        concept_map.source_code,
        concept_map.source_display,
        concept_map.source_system,
        concept_map.target_concept_id AS mapped_concept_id,
        concept_map.target_code AS mapped_concept_code,
        concept_map.target_display AS mapped_concept_display,
        concept_map.target_system,
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
        src.lds_transform_datetime,
        'observation'::VARCHAR(32) AS source_entity,
        src.id AS source_record_id,
        NULL::VARCHAR(16777216) AS allergy_medication_name
    FROM {{ ref('landing_observation') }} AS src
    INNER JOIN {{ ref('conformed_patient') }} AS patients
        ON src.patient_id = patients.source_id
    LEFT JOIN {{ ref('patient_id_index') }} AS patient_idx
        ON src.patient_id = patient_idx.source_patient_id
    LEFT JOIN {{ ref('person_id_index') }} AS person_idx
        ON src.person_id = person_idx.source_person_id
    INNER JOIN {{ ref('int_ncl_practices') }} AS ncl_practices
        ON src.publisher_organisation_code = ncl_practices.practice_code
    LEFT JOIN {{ ref('int_enriched_concept_map') }} AS concept_map
        ON src.observation_source_concept_id = concept_map.source_concept_id
    LEFT JOIN {{ ref('int_enriched_concept_map') }} AS date_precision_map
        ON
            src.clinical_effective_date_precision_source_concept_id
            = date_precision_map.source_concept_id
    LEFT JOIN {{ ref('int_enriched_concept_map') }} AS result_unit_map
        ON
            src.result_value_units_source_concept_id
            = result_unit_map.source_concept_id
    WHERE src.observation_source_concept_id IS NOT NULL
    -- the feed occasionally ships exact duplicate rows; keep one deterministically
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY src.id
        ORDER BY src.lds_transform_datetime DESC, src.lds_source_record_id ASC
    ) = 1
)

SELECT * FROM native_observations

UNION ALL

SELECT
    UUID_STRING('6ba7b811-9dad-11d1-80b4-00c04fd430c8', 'olids:allergy_intolerance:' || src.id::VARCHAR)::UUID AS id,
    src.lds_source_record_id,
    src.patient_id,
    src.person_id,
    src.publisher_organisation_id,
    src.provider_organisation_id,
    src.author_organisation_id,
    src.encounter_id,
    src.practitioner_id,
    NULL::UUID AS parent_observation_id,
    src.clinical_effective_date,
    src.clinical_effective_date_precision_source_concept_id,
    src.date_precision_source_code,
    src.date_precision_source_display,
    src.date_precision_code,
    src.date_precision_display,
    NULL::FLOAT AS result_value,
    NULL::UUID AS result_value_units_source_concept_id,
    NULL::VARCHAR(16777216) AS result_unit_source_code,
    NULL::VARCHAR(16777216) AS result_unit_source_display,
    NULL::VARCHAR(16777216) AS result_unit_code,
    NULL::VARCHAR(16777216) AS result_unit_display,
    NULL::DATE AS result_date,
    NULL::VARCHAR(16777216) AS result_text,
    NULL::BOOLEAN AS is_problem,
    src.is_review,
    NULL::DATE AS problem_end_date,
    src.allergy_intolerance_source_concept_id AS observation_source_concept_id,
    src.source_code,
    src.source_display,
    src.source_system,
    src.mapped_concept_id,
    src.mapped_concept_code,
    src.mapped_concept_display,
    src.target_system,
    src.age_at_event,
    src.age_at_event_baby,
    src.age_at_event_neonate,
    NULL::UUID AS episodicity_source_concept_id,
    NULL::BOOLEAN AS is_primary,
    src.date_recorded,
    NULL::BOOLEAN AS is_problem_deleted,
    src.is_confidential,
    src.lds_is_deleted,
    src.publisher_organisation_code,
    src.clinical_system,
    src.source_extraction_date,
    src.lds_transform_datetime,
    'allergy_intolerance'::VARCHAR(32) AS source_entity,
    src.id AS source_record_id,
    src.medication_name AS allergy_medication_name
FROM {{ ref('conformed_allergy_intolerance') }} AS src

UNION ALL

SELECT
    UUID_STRING('6ba7b811-9dad-11d1-80b4-00c04fd430c8', 'olids:referral_request:' || src.id::VARCHAR)::UUID AS id,
    src.lds_source_record_id,
    src.patient_id,
    src.person_id,
    src.publisher_organisation_id,
    NULL::UUID AS provider_organisation_id,
    src.author_organisation_id,
    src.encounter_id,
    src.practitioner_id,
    NULL::UUID AS parent_observation_id,
    src.clinical_effective_date,
    src.clinical_effective_date_precision_source_concept_id,
    src.date_precision_source_code,
    src.date_precision_source_display,
    src.date_precision_code,
    src.date_precision_display,
    NULL::FLOAT AS result_value,
    NULL::UUID AS result_value_units_source_concept_id,
    NULL::VARCHAR(16777216) AS result_unit_source_code,
    NULL::VARCHAR(16777216) AS result_unit_source_display,
    NULL::VARCHAR(16777216) AS result_unit_code,
    NULL::VARCHAR(16777216) AS result_unit_display,
    NULL::DATE AS result_date,
    NULL::VARCHAR(16777216) AS result_text,
    NULL::BOOLEAN AS is_problem,
    src.is_review,
    NULL::DATE AS problem_end_date,
    src.referral_request_source_concept_id AS observation_source_concept_id,
    src.source_code,
    src.source_display,
    src.source_system,
    src.mapped_concept_id,
    src.mapped_concept_code,
    src.mapped_concept_display,
    src.target_system,
    src.age_at_event,
    src.age_at_event_baby,
    src.age_at_event_neonate,
    NULL::UUID AS episodicity_source_concept_id,
    NULL::BOOLEAN AS is_primary,
    src.recorded_datetime AS date_recorded,
    NULL::BOOLEAN AS is_problem_deleted,
    NULL::BOOLEAN AS is_confidential,
    src.lds_is_deleted,
    src.publisher_organisation_code,
    src.clinical_system,
    src.source_extraction_date,
    src.lds_transform_datetime,
    'referral_request'::VARCHAR(32) AS source_entity,
    src.id AS source_record_id,
    NULL::VARCHAR(16777216) AS allergy_medication_name
FROM {{ ref('conformed_referral_request') }} AS src
