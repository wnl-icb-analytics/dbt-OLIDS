{{ config(alias='healthcare_event') }}

-- Grain: one currently recorded milestone per source record and event type.
WITH events AS (
    SELECT
        'appointment_booking'::VARCHAR AS event_type,
        a.person_id,
        a.patient_id,
        a.datetime_booked AS event_at,
        a.datetime_booked::DATE AS event_date,
        'timestamp'::VARCHAR AS event_time_precision,
        'datetime_booked'::VARCHAR AS event_time_basis,
        'appointment'::VARCHAR AS source_record_type,
        a.appointment_id AS source_record_id,
        a.appointment_id,
        NULL::UUID AS clinical_record_id,
        NULL::VARCHAR AS event_code,
        NULL::VARCHAR AS event_code_name,
        NULL::VARCHAR AS event_coding_system,
        NULL::VARCHAR AS source_status_code,
        NULL::VARCHAR AS source_status_name,
        NULL::VARCHAR AS status_code,
        NULL::VARCHAR AS status_name,
        a.provider_organisation_id,
        a.publisher_organisation_id,
        a.publisher_organisation_code,
        a.source_extraction_date
    FROM {{ ref('conformed_appointment_booking') }} AS a

    UNION ALL

    SELECT
        'appointment_slot'::VARCHAR AS event_type,
        a.person_id,
        a.patient_id,
        a.start_date AS event_at,
        a.start_date::DATE AS event_date,
        'timestamp'::VARCHAR AS event_time_precision,
        'start_date'::VARCHAR AS event_time_basis,
        'appointment'::VARCHAR AS source_record_type,
        a.id AS source_record_id,
        a.id AS appointment_id,
        NULL::UUID AS clinical_record_id,
        NULL::VARCHAR AS event_code,
        NULL::VARCHAR AS event_code_name,
        NULL::VARCHAR AS event_coding_system,
        a.appointment_status_source_code AS source_status_code,
        a.appointment_status_source_display AS source_status_name,
        a.appointment_status_code AS status_code,
        a.appointment_status_display AS status_name,
        a.provider_organisation_id,
        a.publisher_organisation_id,
        a.publisher_organisation_code,
        a.source_extraction_date
    FROM {{ ref('conformed_appointment') }} AS a
    WHERE NOT COALESCE(a.lds_is_deleted, FALSE) AND a.person_id IS NOT NULL

    UNION ALL

    SELECT
        'patient_referral'::VARCHAR AS event_type,
        o.person_id,
        o.patient_id,
        NULL::TIMESTAMP_NTZ AS event_at,
        o.clinical_effective_date::DATE AS event_date,
        CASE
            WHEN o.clinical_effective_date IS NULL THEN 'unknown'
            WHEN o.date_precision_source_code = 'YMD' THEN 'day'
            WHEN o.date_precision_source_code = 'YM' THEN 'month'
            WHEN o.date_precision_source_code = 'Y' THEN 'year'
            ELSE 'unknown'
        END AS event_time_precision,
        'clinical_effective_date'::VARCHAR AS event_time_basis,
        'referral_request'::VARCHAR AS source_record_type,
        o.id AS source_record_id,
        NULL::UUID AS appointment_id,
        {{ olids_clinical_record_id("'observation'", 'o.observation_id') }} AS clinical_record_id,
        o.referral_snomed_code AS event_code,
        o.referral_snomed_name AS event_code_name,
        'snomed_info_sct'::VARCHAR AS event_coding_system,
        NULL::VARCHAR AS source_status_code,
        NULL::VARCHAR AS source_status_name,
        NULL::VARCHAR AS status_code,
        NULL::VARCHAR AS status_name,
        o.provider_organisation_id,
        o.publisher_organisation_id,
        o.publisher_organisation_code,
        o.source_extraction_date
    FROM {{ ref('conformed_referral_request') }} AS o
)
SELECT
    UUID_STRING('6ba7b811-9dad-11d1-80b4-00c04fd430c8',
        'olids:healthcare_event:' || e.event_type || ':' || e.source_record_id::VARCHAR)::UUID AS healthcare_event_id,
    e.event_type,
    e.person_id,
    e.patient_id,
    p.sk_patient_id,
    e.event_at,
    e.event_date,
    e.event_time_precision,
    e.event_time_basis,
    e.source_record_type,
    e.source_record_id,
    e.appointment_id,
    e.clinical_record_id,
    e.event_code,
    e.event_code_name,
    e.event_coding_system,
    e.source_status_code,
    e.source_status_name,
    e.status_code,
    e.status_name,
    e.provider_organisation_id,
    e.publisher_organisation_id,
    e.publisher_organisation_code,
    e.source_extraction_date,
    provider.organisation_code AS provider_organisation_code,
    provider.assigning_authority_code AS provider_code_authority,
    provider.name AS provider_organisation_name,
    publisher.name AS publisher_organisation_name
FROM events AS e
LEFT JOIN {{ ref('conformed_patient') }} AS p
    ON e.patient_id = p.id AND e.person_id = p.person_id
LEFT JOIN {{ ref('conformed_organisation') }} AS provider
    ON e.provider_organisation_id = provider.id AND NOT COALESCE(provider.lds_is_deleted, FALSE)
LEFT JOIN {{ ref('conformed_organisation') }} AS publisher
    ON e.publisher_organisation_id = publisher.id AND NOT COALESCE(publisher.lds_is_deleted, FALSE)
