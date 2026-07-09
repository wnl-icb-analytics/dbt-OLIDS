{{
    config(alias='DIAGNOSTIC_ORDER')
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
    encounter_id,
    practitioner_id,
    parent_observation_id,
    clinical_effective_date,
    date_precision_raw,
    clinical_effective_date_precision_source_concept_id,
    result_value,
    result_measurement_units_source_concept_id,
    result_date,
    result_text,
    is_problem,
    is_review,
    problem_end_date,
    diagnostic_order_source_concept_id,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    episodicity_source_concept_id,
    is_primary,
    date_recorded,
    lds_is_deleted,
    publisher_organisation_code,
    source_extraction_date,
    lds_transform_datetime
FROM {{ source('olids_pseudo', 'DIAGNOSTIC_ORDER') }}
