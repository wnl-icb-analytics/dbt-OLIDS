{{
    config(
        secure=true,
        alias='observation')
}}

/*
Base OBSERVATION View
Filters to WNL practices and excludes sensitive patients.
Pattern: Clinical table with patient_id + publisher_organisation_code
Surfaces indexed person and patient keys.
*/

SELECT
    src.lds_source_record_id,
    src.id,
    patient_idx.patient_id,
    person_idx.person_id,
    src.encounter_id,
    src.practitioner_id,
    src.parent_observation_id,
    src.clinical_effective_date,
    src.clinical_effective_date_precision_source_concept_id,
    src.result_value,
    src.result_units_source_concept_id,
    unit_concept_map.target_code AS result_unit_code,
    unit_concept_map.target_display AS result_unit_display,
    src.result_date,
    src.result_text,
    src.is_problem,
    src.is_review,
    src.problem_end_date,
    src.observation_source_concept_id,
    concept_map.target_concept_id AS mapped_concept_id,
    concept_map.target_code AS mapped_concept_code,
    concept_map.target_display AS mapped_concept_display,
    concept_map.source_code,
    concept_map.source_display,
    concept_map.source_system,
    concept_map.target_system,
    src.age_at_event,
    src.age_at_event_baby,
    src.age_at_event_neonate,
    src.episodicity_source_concept_id,
    src.is_primary,
    src.date_recorded,
    src.is_problem_deleted,
    src.is_confidential,
    src.publisher_organisation_id,
    src.provider_organisation_id,
    src.author_organisation_id,
    src.publisher_organisation_code,
    src.patient_shard_id,
    src.person_shard_id,
    src.lds_source_record_shard_id,
    src.lds_is_deleted,
    src.lds_id,
    src.lds_business_key,
    src.lds_source_dataset_id,
    src.lds_cdm_event_id,
    src.lds_versioner_event_id,
    src.lds_datetime_first_acquired,
    src.lds_datetime_update_acquired,
    src.lds_start_datetime,
    src.lds_lakehouse_date_processed,
    src.lds_lakehouse_datetime_updated
FROM {{ source('olids_common', 'OBSERVATION') }} AS src
INNER JOIN {{ ref('synapse_base_olids_patient') }} AS patients
    ON src.patient_id = patients.source_id
LEFT JOIN {{ ref('patient_id_index') }} AS patient_idx
    ON src.patient_id = patient_idx.source_patient_id
LEFT JOIN {{ ref('person_id_index') }} AS person_idx
    ON src.person_id = person_idx.source_person_id
INNER JOIN {{ ref('synapse_int_wnl_practices') }} AS wnl_practices
    ON src.publisher_organisation_code = wnl_practices.practice_code
LEFT JOIN {{ ref('synapse_int_enriched_concept_map') }} AS concept_map
    ON src.observation_source_concept_id = concept_map.source_concept_id
LEFT JOIN {{ ref('synapse_int_enriched_concept_map') }} AS unit_concept_map
    ON src.result_units_source_concept_id = unit_concept_map.source_concept_id
WHERE
    src.observation_source_concept_id IS NOT NULL
    AND src.lds_start_datetime IS NOT NULL
