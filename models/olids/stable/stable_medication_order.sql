{{
    config(
        cluster_by=['mapped_concept_code', 'clinical_effective_date'],
        transient=false,
        alias='medication_order'
    )
}}

SELECT
    src.id,
    src.lds_source_record_id,
    src.patient_id,
    src.person_id,
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
    src.date_precision_source_code,
    src.date_precision_source_display,
    src.date_precision_code,
    src.date_precision_display,
    src.dose,
    src.quantity_value,
    src.quantity_value_description,
    src.quantity_unit,
    IFF(
        src.duration_days <= 0 OR (COALESCE(f.is_gp2gp_import, FALSE) AND src.duration_days = 28),
        NULL, src.duration_days
    ) AS duration_days,
    IFF(
        COALESCE(f.is_gp2gp_import, FALSE) AND src.estimated_cost = 0,
        NULL, src.estimated_cost
    ) AS estimated_cost,
    src.medication_name,
    src.medication_order_source_concept_id,
    src.statement_medication_name,
    src.source_code,
    src.source_display,
    src.source_system,
    src.mapped_concept_id,
    src.mapped_concept_code,
    src.mapped_concept_display,
    src.target_system,
    src.bnf_chapter,
    src.bnf_section,
    src.bnf_code,
    src.bnf_name,
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
    src.clinical_system,
    src.source_extraction_date,
    src.lds_transform_datetime,
    src.duration_days AS source_duration_days,
    src.estimated_cost AS source_estimated_cost,
    COALESCE(f.is_gp2gp_copy, FALSE) AS is_gp2gp_copy,
    f.gp2gp_original_id,
    COALESCE(f.is_gp2gp_import, FALSE) AS is_gp2gp_import
FROM {{ ref('conformed_medication_order') }} AS src
LEFT JOIN {{ ref('int_medication_order_gp2gp_flags') }} AS f
    ON src.id = f.id
