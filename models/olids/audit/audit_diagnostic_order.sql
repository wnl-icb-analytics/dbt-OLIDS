{% set concept_field = 'diagnostic_order_source_concept_id' %}

WITH global_metrics AS (
    SELECT
        COUNT(*) AS row_count,
        COUNT_IF(src.lds_is_deleted = TRUE) AS deleted_rows,
        COUNT(*) - COUNT(DISTINCT src.id) AS duplicate_id_rows,
        COUNT_IF(
            src.{{ concept_field }} IS NOT NULL
            AND (ecm.source_concept_id IS NULL OR ecm.mapped_concept_code IS NULL)
        ) AS {{ concept_field }}_unmapped_rows,
        COUNT_IF(src.{{ concept_field }} IS NOT NULL)
            AS {{ concept_field }}_populated_rows
    FROM {{ ref('landing_diagnostic_order') }} AS src
    LEFT JOIN {{ ref('conformed_concept_map') }} AS ecm
        ON src.{{ concept_field }} = ecm.source_concept_id
),

orphan_metrics AS (
    SELECT
        COUNT_IF(src.person_id IS NOT NULL AND person.id IS NULL)
            AS person_orphan_rows,
        COUNT_IF(src.person_id IS NOT NULL) AS rows_with_person_id,
        COUNT_IF(src.patient_id IS NOT NULL AND patient.id IS NULL)
            AS patient_orphan_rows
    FROM {{ ref('landing_diagnostic_order') }} AS src
    LEFT JOIN (SELECT DISTINCT id FROM {{ ref('landing_person') }}) AS person
        ON src.person_id = person.id
    LEFT JOIN (SELECT DISTINCT id FROM {{ ref('landing_patient') }}) AS patient
        ON src.patient_id = patient.id
),

unmapped_concepts AS (
    SELECT
        src.{{ concept_field }}::VARCHAR AS source_concept_id,
        COUNT(*) AS affected_rows
    FROM {{ ref('landing_diagnostic_order') }} AS src
    LEFT JOIN {{ ref('conformed_concept_map') }} AS ecm
        ON src.{{ concept_field }} = ecm.source_concept_id
    WHERE
        src.{{ concept_field }} IS NOT NULL
        AND (ecm.source_concept_id IS NULL OR ecm.mapped_concept_code IS NULL)
    GROUP BY src.{{ concept_field }}
    QUALIFY ROW_NUMBER() OVER (
        ORDER BY COUNT(*) DESC, src.{{ concept_field }}
    ) <= 20
),

metrics AS (
    SELECT
        'DIAGNOSTIC_ORDER' AS table_name,
        'row_count' AS metric_name,
        NULL AS practice_code,
        row_count AS value_number,
        NULL AS value_text,
        NULL AS value_date
    FROM global_metrics
    UNION ALL
    SELECT
        'DIAGNOSTIC_ORDER',
        'deleted_rows',
        NULL,
        deleted_rows,
        NULL,
        NULL
    FROM global_metrics
    UNION ALL
    SELECT
        'DIAGNOSTIC_ORDER',
        'duplicate_id_rows',
        NULL,
        duplicate_id_rows,
        NULL,
        NULL
    FROM global_metrics
    UNION ALL
    SELECT
        'DIAGNOSTIC_ORDER',
        'person_orphan_rows',
        NULL,
        person_orphan_rows,
        NULL,
        NULL
    FROM orphan_metrics
    UNION ALL
    SELECT
        'DIAGNOSTIC_ORDER',
        'person_orphan_pct',
        NULL,
        ROUND(100 * person_orphan_rows / NULLIF(rows_with_person_id, 0), 4),
        NULL,
        NULL
    FROM orphan_metrics
    UNION ALL
    SELECT
        'DIAGNOSTIC_ORDER',
        'patient_orphan_rows',
        NULL,
        patient_orphan_rows,
        NULL,
        NULL
    FROM orphan_metrics
    UNION ALL
    SELECT
        'DIAGNOSTIC_ORDER',
        '{{ concept_field }}_unmapped_rows',
        NULL,
        {{ concept_field }}_unmapped_rows,
        NULL,
        NULL
    FROM global_metrics
    UNION ALL
    SELECT
        'DIAGNOSTIC_ORDER',
        '{{ concept_field }}_unmapped_pct',
        NULL,
        ROUND(
            100 * {{ concept_field }}_unmapped_rows
            / NULLIF({{ concept_field }}_populated_rows, 0),
            4
        ),
        NULL,
        NULL
    FROM global_metrics
    UNION ALL
    SELECT
        'DIAGNOSTIC_ORDER',
        '{{ concept_field }}_unmapped_concept',
        NULL,
        affected_rows,
        source_concept_id,
        NULL
    FROM unmapped_concepts
)

SELECT
    table_name::VARCHAR AS table_name,
    metric_name::VARCHAR AS metric_name,
    practice_code::VARCHAR AS practice_code,
    value_number::NUMBER(38, 4) AS value_number,
    value_text::VARCHAR AS value_text,
    value_date::DATE AS value_date
FROM metrics
