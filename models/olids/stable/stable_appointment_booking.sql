{{ config(alias='appointment_booking', materialized='view') }}

-- The appointment snapshot already stores these rows. No second large copy is needed.
SELECT
    appointment_id,
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
FROM {{ ref('conformed_appointment_booking') }}
