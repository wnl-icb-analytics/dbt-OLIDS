{{
    config(alias='MEDICATION_STATEMENT')
}}

/*
Single daily scan through source policies. Downstream models read this cache.
*/

SELECT
    id,
    lds_source_record_id,
    patient_id,
    person_id,
    publisher_organisation_id,
    provider_organisation_id,
    author_organisation_id,
    practitioner_id,
    encounter_id,
    observation_id,
    allergy_intolerance_id,
    diagnostic_order_id,
    referral_request_id,
    clinical_effective_date,
    clinical_effective_date_precision_source_concept_id,
    cancellation_date,
    dose,
    quantity_value_description,
    quantity_value,
    quantity_unit,
    authorisation_type_source_concept_id,
    medication_name,
    medication_statement_source_concept_id,
    bnf_reference,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    issue_method,
    date_recorded,
    is_active,
    is_confidential,
    expiry_date,
    lds_is_deleted,
    publisher_organisation_code,
    source_extraction_date,
    lds_transform_datetime
FROM {{ source('olids_pseudo', 'MEDICATION_STATEMENT') }}
