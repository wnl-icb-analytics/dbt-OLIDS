{% set concept_fields = [
    'episode_status_source_concept_id',
    'episode_type_source_concept_id'
] %}

WITH source_grain_metrics AS (
    SELECT
        COUNT(*) AS row_count,
        COUNT_IF(src.lds_is_deleted = TRUE) AS deleted_rows,
        COUNT(src.id) - COUNT(DISTINCT src.id) AS repeated_id_rows,
        COUNT(src.lds_source_record_id)
            - COUNT(DISTINCT src.lds_source_record_id)
            AS duplicate_source_record_id_rows,
        COUNT_IF(src.id IS NULL) AS null_id_rows,
        COUNT_IF(src.lds_source_record_id IS NULL)
            AS null_source_record_id_rows,
        COUNT_IF(
            src.episode_of_care_end_date < src.episode_of_care_start_date
        ) AS end_before_start_rows
    FROM {{ ref('landing_episode_of_care_v2') }} AS src
),

mapping_metrics AS (
    SELECT
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
    FROM {{ ref('landing_episode_of_care_v2') }} AS src
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
    FROM {{ ref('landing_episode_of_care_v2') }} AS src
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
    FROM {{ ref('landing_episode_of_care_v2') }}
    UNPIVOT (
        source_concept_id FOR concept_field IN (
            episode_status_source_concept_id,
            episode_type_source_concept_id
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

practice_staleness AS (
    SELECT
        publisher_organisation_code AS practice_code,
        MAX(episode_of_care_start_date)::DATE AS max_activity_date
    FROM {{ ref('landing_episode_of_care_v2') }}
    WHERE
        NOT COALESCE(lds_is_deleted, FALSE)
        AND episode_of_care_start_date <= source_extraction_date
    GROUP BY publisher_organisation_code
),

metrics AS (
    SELECT
        'EPISODE_OF_CARE_V2' AS table_name,
        'row_count' AS metric_name,
        NULL AS practice_code,
        row_count AS value_number,
        NULL AS value_text,
        NULL AS value_date
    FROM source_grain_metrics
    UNION ALL
    SELECT 'EPISODE_OF_CARE_V2', 'deleted_rows', NULL, deleted_rows, NULL, NULL
    FROM source_grain_metrics
    UNION ALL
    SELECT 'EPISODE_OF_CARE_V2', 'repeated_id_rows', NULL, repeated_id_rows, NULL, NULL
    FROM source_grain_metrics
    UNION ALL
    SELECT
        'EPISODE_OF_CARE_V2',
        'duplicate_source_record_id_rows',
        NULL,
        duplicate_source_record_id_rows,
        NULL,
        NULL
    FROM source_grain_metrics
    UNION ALL
    SELECT 'EPISODE_OF_CARE_V2', 'null_id_rows', NULL, null_id_rows, NULL, NULL
    FROM source_grain_metrics
    UNION ALL
    SELECT
        'EPISODE_OF_CARE_V2',
        'null_source_record_id_rows',
        NULL,
        null_source_record_id_rows,
        NULL,
        NULL
    FROM source_grain_metrics
    UNION ALL
    SELECT
        'EPISODE_OF_CARE_V2',
        'end_before_start_rows',
        NULL,
        end_before_start_rows,
        NULL,
        NULL
    FROM source_grain_metrics
    UNION ALL
    SELECT 'EPISODE_OF_CARE_V2', 'person_orphan_rows', NULL, person_orphan_rows, NULL, NULL
    FROM orphan_metrics
    UNION ALL
    SELECT
        'EPISODE_OF_CARE_V2',
        'person_orphan_pct',
        NULL,
        ROUND(100 * person_orphan_rows / NULLIF(rows_with_person_id, 0), 4),
        NULL,
        NULL
    FROM orphan_metrics
    UNION ALL
    SELECT 'EPISODE_OF_CARE_V2', 'patient_orphan_rows', NULL, patient_orphan_rows, NULL, NULL
    FROM orphan_metrics
    {% for field in concept_fields %}
        UNION ALL
        SELECT
            'EPISODE_OF_CARE_V2',
            '{{ field }}_unmapped_rows',
            NULL,
            {{ field }}_unmapped_rows,
            NULL,
            NULL
        FROM mapping_metrics
        UNION ALL
        SELECT
            'EPISODE_OF_CARE_V2',
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
        FROM mapping_metrics
    {% endfor %}
    UNION ALL
    SELECT
        'EPISODE_OF_CARE_V2',
        concept_field || '_unmapped_concept',
        NULL,
        affected_rows,
        source_concept_id,
        NULL
    FROM unmapped_concepts
    UNION ALL
    SELECT
        'EPISODE_OF_CARE_V2',
        'max_activity_date',
        practice_code,
        NULL,
        NULL,
        max_activity_date
    FROM practice_staleness
)

SELECT
    table_name::VARCHAR AS table_name,
    metric_name::VARCHAR AS metric_name,
    practice_code::VARCHAR AS practice_code,
    value_number::NUMBER(38, 4) AS value_number,
    value_text::VARCHAR AS value_text,
    value_date::DATE AS value_date
FROM metrics
