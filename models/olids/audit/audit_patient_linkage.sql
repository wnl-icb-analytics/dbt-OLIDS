WITH patient_person_ids AS (
    SELECT DISTINCT patient_id
    FROM {{ ref('landing_patient_person') }}
    WHERE patient_id IS NOT NULL
),

patient_metrics AS (
    SELECT
        COUNT(*) AS row_count,
        COUNT_IF(patient.lds_is_deleted = FALSE) AS live_patients,
        COUNT_IF(patient.lds_is_deleted = TRUE) AS deleted_rows,
        COUNT_IF(
            patient.lds_is_deleted = FALSE
            AND patient_person.patient_id IS NULL
        ) AS unlinked_live_patients,
        COUNT_IF(
            patient.lds_is_deleted = FALSE
            AND patient_person.patient_id IS NULL
            AND TRY_TO_NUMBER(patient.sk_patient_id) IS NOT NULL
        ) AS unlinked_live_with_valid_sk,
        COUNT_IF(
            patient.lds_is_deleted = FALSE
            AND patient_person.patient_id IS NULL
            AND patient.sk_patient_id IS NULL
        ) AS unlinked_live_null_sk,
        COUNT_IF(
            patient.lds_is_deleted = FALSE
            AND patient.person_id IS NULL
        ) AS live_null_person_id
    FROM {{ ref('landing_patient') }} AS patient
    LEFT JOIN patient_person_ids AS patient_person
        ON patient.id = patient_person.patient_id
),

practice_metrics AS (
    SELECT
        patient.publisher_organisation_code AS practice_code,
        COUNT_IF(patient.lds_is_deleted = FALSE) AS live_patients,
        COUNT_IF(
            patient.lds_is_deleted = FALSE
            AND patient_person.patient_id IS NULL
        ) AS unlinked_live_patients
    FROM {{ ref('landing_patient') }} AS patient
    LEFT JOIN patient_person_ids AS patient_person
        ON patient.id = patient_person.patient_id
    GROUP BY patient.publisher_organisation_code
),

person_metrics AS (
    SELECT COUNT(*) AS person_rows
    FROM {{ ref('landing_person') }}
),

patient_person_metrics AS (
    SELECT COUNT(*) AS patient_person_rows
    FROM {{ ref('landing_patient_person') }}
),

metrics AS (
    SELECT
        'PATIENT' AS table_name,
        'row_count' AS metric_name,
        NULL AS practice_code,
        row_count AS value_number,
        NULL AS value_text,
        NULL AS value_date
    FROM patient_metrics
    UNION ALL
    SELECT
        'PATIENT',
        'live_patients',
        NULL,
        live_patients,
        NULL,
        NULL
    FROM patient_metrics
    UNION ALL
    SELECT
        'PATIENT',
        'deleted_rows',
        NULL,
        deleted_rows,
        NULL,
        NULL
    FROM patient_metrics
    UNION ALL
    SELECT
        'PATIENT',
        'unlinked_live_patients',
        NULL,
        unlinked_live_patients,
        NULL,
        NULL
    FROM patient_metrics
    UNION ALL
    SELECT
        'PATIENT',
        'unlinked_live_with_valid_sk',
        NULL,
        unlinked_live_with_valid_sk,
        NULL,
        NULL
    FROM patient_metrics
    UNION ALL
    SELECT
        'PATIENT',
        'unlinked_live_null_sk',
        NULL,
        unlinked_live_null_sk,
        NULL,
        NULL
    FROM patient_metrics
    UNION ALL
    SELECT
        'PATIENT',
        'live_null_person_id',
        NULL,
        live_null_person_id,
        NULL,
        NULL
    FROM patient_metrics
    UNION ALL
    SELECT
        'PERSON',
        'person_rows',
        NULL,
        person_rows,
        NULL,
        NULL
    FROM person_metrics
    UNION ALL
    SELECT
        'PATIENT_PERSON',
        'patient_person_rows',
        NULL,
        patient_person_rows,
        NULL,
        NULL
    FROM patient_person_metrics
    UNION ALL
    SELECT
        'PATIENT',
        'live_patients',
        practice_code,
        live_patients,
        NULL,
        NULL
    FROM practice_metrics
    UNION ALL
    SELECT
        'PATIENT',
        'unlinked_live_patients',
        practice_code,
        unlinked_live_patients,
        NULL,
        NULL
    FROM practice_metrics
)

SELECT
    table_name::VARCHAR AS table_name,
    metric_name::VARCHAR AS metric_name,
    practice_code::VARCHAR AS practice_code,
    value_number::NUMBER(38, 4) AS value_number,
    value_text::VARCHAR AS value_text,
    value_date::DATE AS value_date
FROM metrics
