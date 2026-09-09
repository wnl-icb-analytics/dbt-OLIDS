{{ config(alias='appointment_clinical_record') }}

-- Grain: one appointment, clinical record type and clinical record ID.
-- Match the recorded path and person, never proximity in time. A person can
-- have different patient IDs on records from different practice registrations.
WITH appointment_encounters AS (
    SELECT
        e.id AS encounter_id,
        a.id AS appointment_id,
        a.patient_id,
        a.person_id
    FROM {{ ref('conformed_encounter') }} AS e
    INNER JOIN {{ ref('conformed_appointment') }} AS a
        ON e.appointment_id = a.id
        AND e.person_id = a.person_id
    WHERE NOT COALESCE(e.lds_is_deleted, FALSE)
        AND NOT COALESCE(a.lds_is_deleted, FALSE)
),

clinical_records AS (
    SELECT
        'observation'::VARCHAR(32) AS clinical_record_type,
        id AS clinical_record_id,
        encounter_id,
        person_id
    FROM {{ ref('conformed_observation') }}
    WHERE encounter_id IS NOT NULL
        AND NOT COALESCE(lds_is_deleted, FALSE)

    UNION ALL

    SELECT
        'medication_order'::VARCHAR(32) AS clinical_record_type,
        id AS clinical_record_id,
        encounter_id,
        person_id
    FROM {{ ref('conformed_medication_order') }}
    WHERE encounter_id IS NOT NULL
        AND NOT COALESCE(lds_is_deleted, FALSE)

    UNION ALL

    SELECT
        'medication_statement'::VARCHAR(32) AS clinical_record_type,
        id AS clinical_record_id,
        encounter_id,
        person_id
    FROM {{ ref('conformed_medication_statement') }}
    WHERE encounter_id IS NOT NULL
        AND NOT COALESCE(lds_is_deleted, FALSE)
)

SELECT
    e.appointment_id,
    c.clinical_record_type,
    c.clinical_record_id,
    e.encounter_id,
    e.patient_id,
    e.person_id
FROM clinical_records AS c
INNER JOIN appointment_encounters AS e
    ON c.encounter_id = e.encounter_id
    AND c.person_id = e.person_id
