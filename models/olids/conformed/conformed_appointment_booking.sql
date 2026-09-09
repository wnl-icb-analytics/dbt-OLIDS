{{ config(alias='appointment_booking') }}

-- Grain: the current recorded booking for each retained patient appointment.
SELECT
    id AS appointment_id,
    patient_id,
    person_id,
    datetime_booked,
    start_date,
    booking_method_source_code,
    booking_method_source_display,
    booking_method_code,
    booking_method_display,
    provider_organisation_id,
    publisher_organisation_id,
    publisher_organisation_code,
    source_extraction_date
FROM {{ ref('conformed_appointment') }}
WHERE datetime_booked IS NOT NULL
    AND person_id IS NOT NULL
    AND NOT COALESCE(lds_is_deleted, FALSE)
