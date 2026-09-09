{{ config(materialized='table', transient=true, alias='referral_request') }}

-- Grain: one retained observation meeting the agreed patient-referral terminology.
-- Original source content is preserved separately for the observation expansion.
SELECT
    CASE
        WHEN o.source_entity = 'referral_request' THEN o.source_record_id
        ELSE UUID_STRING('6ba7b811-9dad-11d1-80b4-00c04fd430c8',
            'olids:referral_request:observation:' || o.id::VARCHAR)::UUID
    END AS id,
    o.lds_source_record_id,
    o.patient_id,
    o.person_id,
    o.publisher_organisation_id,
    o.author_organisation_id,
    o.encounter_id,
    o.practitioner_id,
    r.unique_booking_reference_number,
    o.clinical_effective_date,
    o.clinical_effective_date_precision_source_concept_id,
    o.date_precision_source_code,
    o.date_precision_source_display,
    o.date_precision_code,
    o.date_precision_display,
    r.requester_organisation_id,
    r.recipient_organisation_id,
    r.referral_request_priority_source_concept_id,
    r.referral_request_priority_source_code,
    r.referral_request_priority_source_display,
    r.referral_request_priority_code,
    r.referral_request_priority_display,
    r.referral_request_type_source_concept_id,
    r.referral_request_type_source_code,
    r.referral_request_type_source_display,
    r.referral_request_type_code,
    r.referral_request_type_display,
    r.referral_request_specialty_source_concept_id,
    r.referral_request_specialty_source_code,
    r.referral_request_specialty_source_display,
    r.referral_request_specialty_code,
    r.referral_request_specialty_display,
    r.mode,
    r.is_outgoing_referral,
    o.is_review,
    o.observation_source_concept_id AS referral_request_source_concept_id,
    o.source_code,
    o.source_display,
    o.source_system,
    o.mapped_concept_id,
    o.mapped_concept_code,
    o.mapped_concept_display,
    o.target_system,
    o.age_at_event,
    o.age_at_event_baby,
    o.age_at_event_neonate,
    o.date_recorded AS recorded_datetime,
    r.value,
    o.lds_is_deleted,
    o.publisher_organisation_code,
    o.clinical_system,
    o.source_extraction_date,
    o.lds_transform_datetime,
    o.id AS observation_id,
    o.provider_organisation_id,
    COALESCE(source_referral.code, mapped_referral.code) AS referral_snomed_code,
    COALESCE(source_referral.code_name, mapped_referral.code_name) AS referral_snomed_name
FROM {{ ref('conformed_observation') }} AS o
LEFT JOIN {{ ref('conformed_referral_request_source') }} AS r
    ON o.source_entity = 'referral_request' AND o.source_record_id = r.id
    AND o.person_id = r.person_id
LEFT JOIN {{ ref('patient_referral_snomed_codes') }} AS source_referral
    ON o.source_code = source_referral.code
    AND o.source_system IN ('snomed_info_sct', 'http:__snomed.info_sct', 'http://snomed.info/sct')
LEFT JOIN {{ ref('patient_referral_snomed_codes') }} AS mapped_referral
    ON o.mapped_concept_code = mapped_referral.code
    AND o.target_system IN ('snomed_info_sct', 'http:__snomed.info_sct', 'http://snomed.info/sct')
WHERE NOT COALESCE(o.lds_is_deleted, FALSE)
    AND o.person_id IS NOT NULL
    AND (source_referral.code IS NOT NULL OR mapped_referral.code IS NOT NULL)
