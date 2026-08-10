{{
    config(
        materialized='table',
        transient=true,
        alias='appointment')
}}

/*
Conformed APPOINTMENT view.
Filters to NCL practices and excludes patients outside the filtered spine.
*/

SELECT
    src.id,
    src.lds_source_record_id,
    patient_idx.patient_id,
    person_idx.person_id,
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
    patients.clinical_system,
    src.source_extraction_date,
    src.lds_transform_datetime
FROM {{ ref('landing_appointment') }} AS src
INNER JOIN {{ ref('conformed_patient') }} AS patients
    ON src.patient_id = patients.source_id
LEFT JOIN {{ ref('patient_id_index') }} AS patient_idx
    ON src.patient_id = patient_idx.source_patient_id
LEFT JOIN {{ ref('person_id_index') }} AS person_idx
    ON src.person_id = person_idx.source_person_id
INNER JOIN {{ ref('int_ncl_practices') }} AS ncl_practices
    ON src.publisher_organisation_code = ncl_practices.practice_code
LEFT JOIN {{ ref('int_enriched_concept_map') }} AS appointment_status_map
    ON
        src.appointment_status_source_concept_id
        = appointment_status_map.source_concept_id
LEFT JOIN {{ ref('int_enriched_concept_map') }} AS booking_method_map
    ON
        src.booking_method_source_concept_id
        = booking_method_map.source_concept_id
LEFT JOIN {{ ref('int_enriched_concept_map') }} AS contact_mode_map
    ON src.contact_mode_source_concept_id = contact_mode_map.source_concept_id
WHERE
    src.patient_id IS NOT NULL
    AND src.start_date IS NOT NULL
-- the feed occasionally ships exact duplicate rows; keep one deterministically
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY src.id
    ORDER BY src.lds_transform_datetime DESC, src.lds_source_record_id ASC
) = 1
