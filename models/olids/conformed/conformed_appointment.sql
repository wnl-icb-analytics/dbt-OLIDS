{{
    config(
        secure=true,
        alias='appointment')
}}

/*
Conformed APPOINTMENT view.
Filters to WNL practices and excludes patients outside the filtered spine.
*/

SELECT
    src.id,
    src.lds_source_record_id,
    src.patient_id,
    {{ generate_person_id('src.person_id') }} AS person_id,
    src.publisher_organisation_id,
    src.provider_organisation_id,
    src.author_organisation_id,
    src.slot_id,
    src.practitioner_in_role_id,
    src.schedule_id,
    src.start_date,
    src.planned_duration_mins,
    src.actual_duration_mins,
    src.appointment_status_source_concept_id,
    appointment_status_map.source_code AS appointment_status_source_code,
    appointment_status_map.source_display AS appointment_status_source_display,
    appointment_status_map.target_code AS appointment_status_code,
    appointment_status_map.target_display AS appointment_status_display,
    src.patient_wait_mins,
    src.patient_delay_mins,
    src.datetime_booked,
    src.datetime_sent_in,
    src.datetime_left,
    src.cancelled_date,
    src.appointment_type,
    src.age_at_event,
    src.age_at_event_baby,
    src.age_at_event_neonate,
    src.booking_method_source_concept_id,
    booking_method_map.source_code AS booking_method_source_code,
    booking_method_map.source_display AS booking_method_source_display,
    booking_method_map.target_code AS booking_method_code,
    booking_method_map.target_display AS booking_method_display,
    src.contact_mode_source_concept_id,
    contact_mode_map.source_code AS contact_mode_source_code,
    contact_mode_map.source_display AS contact_mode_source_display,
    contact_mode_map.target_code AS contact_mode_code,
    contact_mode_map.target_display AS contact_mode_display,
    src.is_blocked,
    src.national_slot_category_name,
    src.context_type,
    src.service_setting,
    src.national_slot_category_description,
    src.csds_care_contact_identifier,
    src.lds_is_deleted,
    src.publisher_organisation_code,
    src.source_extraction_date,
    src.lds_transform_datetime
FROM {{ ref('landing_appointment') }} src
INNER JOIN {{ ref('conformed_patient') }} patients
    ON src.patient_id = patients.id
INNER JOIN {{ ref('int_wnl_practices') }} wnl_practices
    ON src.publisher_organisation_code = wnl_practices.practice_code
LEFT JOIN {{ ref('int_enriched_concept_map') }} appointment_status_map
    ON src.appointment_status_source_concept_id = appointment_status_map.source_concept_id
LEFT JOIN {{ ref('int_enriched_concept_map') }} booking_method_map
    ON src.booking_method_source_concept_id = booking_method_map.source_concept_id
LEFT JOIN {{ ref('int_enriched_concept_map') }} contact_mode_map
    ON src.contact_mode_source_concept_id = contact_mode_map.source_concept_id
WHERE src.patient_id IS NOT NULL
    AND src.start_date IS NOT NULL
