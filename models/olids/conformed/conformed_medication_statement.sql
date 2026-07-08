{{
    config(
        secure=true,
        alias='medication_statement')
}}

/*
Conformed MEDICATION_STATEMENT view.
Filters to WNL practices, excludes patients outside the filtered spine and adds medication mappings.
*/

SELECT
    src.id,
    src.lds_source_record_id,
    src.patient_id,
    {{ generate_person_id('src.person_id') }} AS person_id,
    src.publisher_organisation_id,
    src.provider_organisation_id,
    src.author_organisation_id,
    src.practitioner_id,
    src.encounter_id,
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
    src.cancellation_date,
    src.dose,
    src.quantity_value_description,
    src.quantity_value,
    src.quantity_unit,
    src.authorisation_type_source_concept_id,
    auth_concept_map.source_code AS authorisation_type_source_code,
    auth_concept_map.source_display AS authorisation_type_source_display,
    auth_concept_map.target_code AS authorisation_type_code,
    auth_concept_map.target_display AS authorisation_type_display,
    src.medication_name,
    src.medication_statement_source_concept_id,
    concept_map.source_code AS source_code,
    concept_map.source_display AS source_display,
    concept_map.source_system AS source_system,
    concept_map.target_concept_id AS mapped_concept_id,
    concept_map.target_code AS mapped_concept_code,
    concept_map.target_display AS mapped_concept_display,
    concept_map.target_system AS target_system,
    bnf.bnf_chapter AS bnf_chapter,
    bnf.bnf_section AS bnf_section,
    bnf.bnf_code AS bnf_code,
    bnf.bnf_name AS bnf_name,
    src.bnf_reference,
    src.age_at_event,
    src.age_at_event_baby,
    src.age_at_event_neonate,
    src.issue_method,
    src.date_recorded,
    src.is_active,
    src.is_confidential,
    src.expiry_date,
    src.lds_is_deleted,
    src.publisher_organisation_code,
    src.source_extraction_date,
    src.lds_transform_datetime
FROM {{ ref('landing_medication_statement') }} src
INNER JOIN {{ ref('conformed_patient') }} patients
    ON src.patient_id = patients.id
INNER JOIN {{ ref('int_wnl_practices') }} wnl_practices
    ON src.publisher_organisation_code = wnl_practices.practice_code
LEFT JOIN {{ ref('int_enriched_concept_map') }} concept_map
    ON src.medication_statement_source_concept_id = concept_map.source_concept_id
LEFT JOIN {{ ref('int_enriched_concept_map') }} auth_concept_map
    ON src.authorisation_type_source_concept_id = auth_concept_map.source_concept_id
LEFT JOIN {{ ref('int_enriched_concept_map') }} date_precision_map
    ON src.clinical_effective_date_precision_source_concept_id = date_precision_map.source_concept_id
LEFT JOIN DATA_LAB_OLIDS_NCL.REFERENCE.BNF_LATEST bnf
    ON concept_map.target_code = bnf.snomed_code
WHERE src.medication_statement_source_concept_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY src.id ORDER BY concept_map.target_display NULLS LAST, concept_map.target_concept_id NULLS LAST, auth_concept_map.target_display NULLS LAST, auth_concept_map.target_concept_id NULLS LAST, date_precision_map.target_display NULLS LAST, date_precision_map.target_concept_id NULLS LAST) = 1
