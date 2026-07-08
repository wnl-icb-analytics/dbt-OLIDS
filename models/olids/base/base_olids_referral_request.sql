{{
    config(
        secure=true,
        alias='referral_request')
}}

/*
Base REFERRAL_REQUEST view.
Filters to WNL practices and excludes patients outside the filtered spine.
*/

SELECT
    src.id,
    src.lds_source_record_id,
    src.patient_id,
    {{ generate_person_id('src.person_id') }} AS person_id,
    src.publisher_organisation_id,
    src.author_organisation_id,
    src.organisation_id,
    src.encounter_id,
    src.practitioner_id,
    src.unique_booking_reference_number,
    src.clinical_effective_date,
    src.clinical_effective_date_precision_source_concept_id,
    date_precision_map.source_code AS date_precision_source_code,
    date_precision_map.source_display AS date_precision_source_display,
    date_precision_map.target_code AS date_precision_code,
    date_precision_map.target_display AS date_precision_display,
    src.requester_organisation_id,
    src.recipient_organisation_id,
    src.referral_request_priority_source_concept_id,
    priority_map.source_code AS referral_request_priority_source_code,
    priority_map.source_display AS referral_request_priority_source_display,
    priority_map.target_code AS referral_request_priority_code,
    priority_map.target_display AS referral_request_priority_display,
    src.referral_request_type_source_concept_id,
    type_map.source_code AS referral_request_type_source_code,
    type_map.source_display AS referral_request_type_source_display,
    type_map.target_code AS referral_request_type_code,
    type_map.target_display AS referral_request_type_display,
    src.referral_request_specialty_source_concept_id,
    specialty_map.source_code AS referral_request_specialty_source_code,
    specialty_map.source_display AS referral_request_specialty_source_display,
    specialty_map.target_code AS referral_request_specialty_code,
    specialty_map.target_display AS referral_request_specialty_display,
    src.mode,
    src.is_outgoing_referral,
    src.is_review,
    src.referral_request_source_concept_id,
    concept_map.source_code AS source_code,
    concept_map.source_display AS source_display,
    concept_map.source_system AS source_system,
    concept_map.target_concept_id AS mapped_concept_id,
    concept_map.target_code AS mapped_concept_code,
    concept_map.target_display AS mapped_concept_display,
    concept_map.target_system AS target_system,
    src.age_at_event,
    src.age_at_event_baby,
    src.age_at_event_neonate,
    src.recorded_date,
    src.value,
    src.lds_is_deleted,
    src.publisher_organisation_code,
    src.source_extraction_date,
    src.lds_transform_datetime
FROM {{ ref('landing_referral_request') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src.patient_id = patients.id
INNER JOIN {{ ref('int_wnl_practices') }} wnl_practices
    ON src.publisher_organisation_code = wnl_practices.practice_code
LEFT JOIN {{ ref('int_enriched_concept_map') }} concept_map
    ON src.referral_request_source_concept_id = concept_map.source_concept_id
LEFT JOIN {{ ref('int_enriched_concept_map') }} date_precision_map
    ON src.clinical_effective_date_precision_source_concept_id = date_precision_map.source_concept_id
LEFT JOIN {{ ref('int_enriched_concept_map') }} priority_map
    ON src.referral_request_priority_source_concept_id = priority_map.source_concept_id
LEFT JOIN {{ ref('int_enriched_concept_map') }} type_map
    ON src.referral_request_type_source_concept_id = type_map.source_concept_id
LEFT JOIN {{ ref('int_enriched_concept_map') }} specialty_map
    ON src.referral_request_specialty_source_concept_id = specialty_map.source_concept_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY src.id ORDER BY concept_map.target_display NULLS LAST, date_precision_map.target_display NULLS LAST, priority_map.target_display NULLS LAST, type_map.target_display NULLS LAST, specialty_map.target_display NULLS LAST) = 1
