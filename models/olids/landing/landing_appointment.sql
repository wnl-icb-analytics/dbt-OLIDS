{{
    config(alias='APPOINTMENT')
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
    slot_id,
    practitioner_in_role_id,
    schedule_id,
    start_date,
    planned_duration_mins,
    actual_duration_mins,
    appointment_status_source_concept_id,
    patient_wait_mins,
    patient_delay_mins,
    datetime_booked,
    datetime_sent_in,
    datetime_left,
    cancelled_date,
    appointment_type,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    booking_method_source_concept_id,
    contact_mode_source_concept_id,
    is_blocked,
    national_slot_category_name,
    context_type,
    service_setting,
    national_slot_category_description,
    csds_care_contact_identifier,
    lds_is_deleted,
    publisher_organisation_code,
    source_extraction_date,
    lds_transform_datetime
FROM {{ source('olids_pseudo', 'APPOINTMENT') }}
