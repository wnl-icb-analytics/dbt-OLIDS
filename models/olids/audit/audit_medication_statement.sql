{% set concept_fields = [
    'medication_statement_source_concept_id',
    'authorisation_type_source_concept_id',
    'clinical_effective_date_precision_source_concept_id'
] %}

WITH global_metrics AS (
    SELECT
        COUNT(*) AS row_count,
        COUNT_IF(src.lds_is_deleted = TRUE) AS deleted_rows,
        COUNT(*) - COUNT(DISTINCT src.id) AS duplicate_id_rows,
        {% for field in concept_fields %}
            COUNT_IF(
                src.{{ field }} IS NOT NULL
                AND (
                    ecm_{{ loop.index }}.source_concept_id IS NULL
                    OR ecm_{{ loop.index }}.mapped_concept_code IS NULL
                )
            ) AS {{ field }}_unmapped_rows,
            COUNT_IF(src.{{ field }} IS NOT NULL)
                AS {{ field }}_populated_rows

            {% if not loop.last %}
                ,
            {% endif %}
        {% endfor %}
    FROM {{ ref('landing_medication_statement') }} AS src
    {% for field in concept_fields %}
        LEFT JOIN {{ ref('conformed_concept_map') }} AS ecm_{{ loop.index }}
            ON src.{{ field }} = ecm_{{ loop.index }}.source_concept_id
    {% endfor %}
),

orphan_metrics AS (
    SELECT
        COUNT_IF(src.person_id IS NOT NULL AND person.id IS NULL)
            AS person_orphan_rows,
        COUNT_IF(src.person_id IS NOT NULL) AS rows_with_person_id,
        COUNT_IF(src.patient_id IS NOT NULL AND patient.id IS NULL)
            AS patient_orphan_rows
    FROM {{ ref('landing_medication_statement') }} AS src
    LEFT JOIN (SELECT DISTINCT id FROM {{ ref('landing_person') }}) AS person
        ON src.person_id = person.id
    LEFT JOIN (SELECT DISTINCT id FROM {{ ref('landing_patient') }}) AS patient
        ON src.patient_id = patient.id
),

unmapped_concepts AS (
    SELECT
        unpivoted.source_concept_id::VARCHAR AS source_concept_id,
        LOWER(unpivoted.concept_field) AS concept_field,
        COUNT(*) AS affected_rows
    FROM {{ ref('landing_medication_statement') }}
    UNPIVOT (
        source_concept_id FOR concept_field IN (
            medication_statement_source_concept_id,
            authorisation_type_source_concept_id,
            clinical_effective_date_precision_source_concept_id
        )
    ) AS unpivoted
    LEFT JOIN {{ ref('conformed_concept_map') }} AS ecm
        ON unpivoted.source_concept_id = ecm.source_concept_id
    WHERE ecm.source_concept_id IS NULL OR ecm.mapped_concept_code IS NULL
    GROUP BY unpivoted.concept_field, unpivoted.source_concept_id
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY unpivoted.concept_field
        ORDER BY COUNT(*) DESC, unpivoted.source_concept_id
    ) <= 20
),

metrics AS (
    SELECT
        'MEDICATION_STATEMENT' AS table_name,
        'row_count' AS metric_name,
        NULL AS practice_code,
        row_count AS value_number,
        NULL AS value_text,
        NULL AS value_date
    FROM global_metrics
    UNION ALL
    SELECT
        'MEDICATION_STATEMENT',
        'deleted_rows',
        NULL,
        deleted_rows,
        NULL,
        NULL
    FROM global_metrics
    UNION ALL
    SELECT
        'MEDICATION_STATEMENT',
        'duplicate_id_rows',
        NULL,
        duplicate_id_rows,
        NULL,
        NULL
    FROM global_metrics
    UNION ALL
    SELECT
        'MEDICATION_STATEMENT',
        'person_orphan_rows',
        NULL,
        person_orphan_rows,
        NULL,
        NULL
    FROM orphan_metrics
    UNION ALL
    SELECT
        'MEDICATION_STATEMENT',
        'person_orphan_pct',
        NULL,
        ROUND(100 * person_orphan_rows / NULLIF(rows_with_person_id, 0), 4),
        NULL,
        NULL
    FROM orphan_metrics
    UNION ALL
    SELECT
        'MEDICATION_STATEMENT',
        'patient_orphan_rows',
        NULL,
        patient_orphan_rows,
        NULL,
        NULL
    FROM orphan_metrics
    {% for field in concept_fields %}
        UNION ALL
        SELECT
            'MEDICATION_STATEMENT',
            '{{ field }}_unmapped_rows',
            NULL,
            {{ field }}_unmapped_rows,
            NULL,
            NULL
        FROM global_metrics
        UNION ALL
        SELECT
            'MEDICATION_STATEMENT',
            '{{ field }}_unmapped_pct',
            NULL,
            ROUND(
                100
                * {{ field }}_unmapped_rows
                / NULLIF({{ field }}_populated_rows, 0),
                4
            ),
            NULL,
            NULL
        FROM global_metrics
    {% endfor %}
    UNION ALL
    SELECT
        'MEDICATION_STATEMENT',
        concept_field || '_unmapped_concept',
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
