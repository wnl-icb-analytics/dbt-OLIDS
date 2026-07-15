WITH global_metrics AS (
    SELECT
        COUNT(*) AS row_count,
        COUNT_IF(lds_is_deleted = TRUE) AS deleted_rows,
        COUNT(*) - COUNT(DISTINCT id) AS duplicate_id_rows
    FROM {{ ref('landing_appointment') }}
),

orphan_metrics AS (
    SELECT
        COUNT_IF(src.person_id IS NOT NULL AND person.id IS NULL)
            AS person_orphan_rows,
        COUNT_IF(src.person_id IS NOT NULL) AS rows_with_person_id,
        COUNT_IF(src.patient_id IS NOT NULL AND patient.id IS NULL)
            AS patient_orphan_rows
    FROM {{ ref('landing_appointment') }} AS src
    LEFT JOIN (SELECT DISTINCT id FROM {{ ref('landing_person') }}) AS person
        ON src.person_id = person.id
    LEFT JOIN (SELECT DISTINCT id FROM {{ ref('landing_patient') }}) AS patient
        ON src.patient_id = patient.id
),

metrics AS (
    SELECT
        'APPOINTMENT' AS table_name,
        'row_count' AS metric_name,
        NULL AS practice_code,
        row_count AS value_number,
        NULL AS value_text,
        NULL AS value_date
    FROM global_metrics
    UNION ALL
    SELECT
        'APPOINTMENT',
        'deleted_rows',
        NULL,
        deleted_rows,
        NULL,
        NULL
    FROM global_metrics
    UNION ALL
    SELECT
        'APPOINTMENT',
        'duplicate_id_rows',
        NULL,
        duplicate_id_rows,
        NULL,
        NULL
    FROM global_metrics
    UNION ALL
    SELECT
        'APPOINTMENT',
        'person_orphan_rows',
        NULL,
        person_orphan_rows,
        NULL,
        NULL
    FROM orphan_metrics
    UNION ALL
    SELECT
        'APPOINTMENT',
        'person_orphan_pct',
        NULL,
        ROUND(100 * person_orphan_rows / NULLIF(rows_with_person_id, 0), 4),
        NULL,
        NULL
    FROM orphan_metrics
    UNION ALL
    SELECT
        'APPOINTMENT',
        'patient_orphan_rows',
        NULL,
        patient_orphan_rows,
        NULL,
        NULL
    FROM orphan_metrics
)

SELECT
    table_name::VARCHAR AS table_name,
    metric_name::VARCHAR AS metric_name,
    practice_code::VARCHAR AS practice_code,
    value_number::NUMBER(38, 4) AS value_number,
    value_text::VARCHAR AS value_text,
    value_date::DATE AS value_date
FROM metrics
