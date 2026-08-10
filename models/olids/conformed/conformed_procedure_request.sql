{{
    config(
        materialized='table',
        transient=true,
        alias='procedure_request')
}}

/*
Conformed PROCEDURE_REQUEST view.
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
    src.practitioner_id,
    src.encounter_id,
    src.clinical_effective_date,
    src.clinical_effective_date_precision_source_concept_id,
    date_precision_map.source_code AS date_precision_source_code,
    date_precision_map.source_display AS date_precision_source_display,
    date_precision_map.target_code AS date_precision_code,
    date_precision_map.target_display AS date_precision_display,
    src.date_recorded,
    src.description,
    src.procedure_request_source_concept_id,
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
    src.is_confidential,
    src.status_source_concept_id,
    status_map.source_code AS status_source_code,
    status_map.source_display AS status_source_display,
    status_map.target_code AS status_code,
    status_map.target_display AS status_display,
    src.lds_is_deleted,
    src.publisher_organisation_code,
    patients.clinical_system,
    src.source_extraction_date,
    src.lds_transform_datetime
FROM {{ ref('landing_procedure_request') }} AS src
INNER JOIN {{ ref('conformed_patient') }} AS patients
    ON src.patient_id = patients.source_id
LEFT JOIN {{ ref('patient_id_index') }} AS patient_idx
    ON src.patient_id = patient_idx.source_patient_id
LEFT JOIN {{ ref('person_id_index') }} AS person_idx
    ON src.person_id = person_idx.source_person_id
INNER JOIN {{ ref('int_ncl_practices') }} AS ncl_practices
    ON src.publisher_organisation_code = ncl_practices.practice_code
LEFT JOIN {{ ref('int_enriched_concept_map') }} AS concept_map
    ON src.procedure_request_source_concept_id = concept_map.source_concept_id
LEFT JOIN {{ ref('int_enriched_concept_map') }} AS date_precision_map
    ON
        src.clinical_effective_date_precision_source_concept_id
        = date_precision_map.source_concept_id
LEFT JOIN {{ ref('int_enriched_concept_map') }} AS status_map
    ON src.status_source_concept_id = status_map.source_concept_id
