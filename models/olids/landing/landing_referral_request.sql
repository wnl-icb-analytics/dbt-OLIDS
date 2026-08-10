{{
    config(alias='REFERRAL_REQUEST')
}}

/*
Single daily scan through source policies, scoped to NCL practices.
Downstream models read this cache.
*/

SELECT
    id,
    lds_source_record_id,
    patient_id,
    person_id,
    publisher_organisation_id,
    author_organisation_id,
    encounter_id,
    practitioner_id,
    unique_booking_reference_number,
    clinical_effective_date,
    clinical_effective_date_precision_source_concept_id,
    requester_organisation_id,
    recipient_organisation_id,
    referral_request_priority_source_concept_id,
    referral_request_type_source_concept_id,
    referral_request_specialty_source_concept_id,
    mode,
    is_outgoing_referral,
    is_review,
    referral_request_source_concept_id,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    recorded_datetime,
    value,
    lds_is_deleted,
    publisher_organisation_code,
    source_extraction_date,
    lds_source_dataset,
    lds_transform_datetime
FROM {{ source('olids_pseudo', 'REFERRAL_REQUEST') }}
WHERE publisher_organisation_code IN (
    SELECT ncl.practice_code FROM {{ ref('int_ncl_practices') }} AS ncl
)
