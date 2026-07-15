{{
    config(
        materialized='table',
        transient=true,
        alias='medication_order')
}}

/*
Conformed MEDICATION_ORDER view.
Filters to WNL practices, excludes patients outside the filtered spine and adds medication mappings.
*/

SELECT
    src.id,
    src.lds_source_record_id,
    patient_idx.patient_id,
    person_idx.person_id,
    src.publisher_organisation_id,
    src.provider_organisation_id,
    src.author_organisation_id,
    src.medication_statement_id,
    src.encounter_id,
    src.practitioner_id,
    src.observation_id,
    src.allergy_intolerance_id,
    src.diagnostic_order_id,
    src.referral_request_id,
    src.clinical_effective_date,
    src.clinical_effective_date_precision_source_concept_id,
    date_precision_map.source_code AS date_precision_source_code,
    date_precision_map.source_display AS date_precision_source_display,
    date_precision_map.target_code AS date_precision_code,
    date_precision_map.target_display AS date_precision_display,
    src.dose,
    src.quantity_value,
    src.quantity_value_description,
    src.quantity_unit,
    src.duration_days,
    src.estimated_cost,
    src.medication_name,
    src.medication_order_source_concept_id,
    ms.medication_statement_source_concept_id,
    ms.medication_name AS statement_medication_name,
    concept_map.source_code,
    concept_map.source_display,
    concept_map.source_system,
    concept_map.target_concept_id AS mapped_concept_id,
    concept_map.target_code AS mapped_concept_code,
    concept_map.target_display AS mapped_concept_display,
    concept_map.target_system,
    bnf.bnf_chapter,
    bnf.bnf_section,
    bnf.bnf_code,
    bnf.bnf_name,
    src.bnf_reference,
    src.age_at_event,
    src.age_at_event_baby,
    src.age_at_event_neonate,
    src.issue_method,
    src.date_recorded,
    src.is_confidential,
    src.issue_method_description,
    src.lds_is_deleted,
    src.publisher_organisation_code,
    src.source_extraction_date,
    src.lds_transform_datetime
FROM {{ ref('landing_medication_order') }} AS src
INNER JOIN {{ ref('conformed_patient') }} AS patients
    ON src.patient_id = patients.source_id
LEFT JOIN {{ ref('patient_id_index') }} AS patient_idx
    ON src.patient_id = patient_idx.source_patient_id
LEFT JOIN {{ ref('person_id_index') }} AS person_idx
    ON src.person_id = person_idx.source_person_id
INNER JOIN {{ ref('int_wnl_practices') }} AS wnl_practices
    ON src.publisher_organisation_code = wnl_practices.practice_code
LEFT JOIN {{ ref('landing_medication_statement') }} AS ms
    ON src.medication_statement_id = ms.id
LEFT JOIN {{ ref('int_enriched_concept_map') }} AS concept_map
    ON src.medication_order_source_concept_id = concept_map.source_concept_id
LEFT JOIN {{ ref('int_enriched_concept_map') }} AS date_precision_map
    ON
        src.clinical_effective_date_precision_source_concept_id
        = date_precision_map.source_concept_id
LEFT JOIN data_lab_olids_ncl.reference.bnf_latest AS bnf
    ON concept_map.target_code = bnf.snomed_code
WHERE src.medication_order_source_concept_id IS NOT NULL
