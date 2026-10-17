{% set entities = ['medication_order', 'medication_statement', 'observation'] %}

WITH
{% for entity in entities %}
    {{ entity }}_metrics AS (
        SELECT
            COUNT(*) AS live_rows,
            COALESCE(COUNT_IF(f.is_gp2gp_copy), 0) AS gp2gp_copy_rows,
            COALESCE(COUNT_IF(f.is_gp2gp_import), 0) AS gp2gp_import_rows
            {% if entity == 'medication_order' %}
                , COALESCE(COUNT_IF(
                    f.is_gp2gp_import AND src.duration_days > 0 AND src.duration_days <> 28
                ), 0) AS import_non_default_duration_rows
                , COALESCE(COUNT_IF(
                    f.is_gp2gp_import AND src.estimated_cost <> 0
                ), 0) AS import_non_default_cost_rows
            {% endif %}
        FROM {{ ref('conformed_' ~ entity) }} AS src
        LEFT JOIN {{ ref('int_' ~ entity ~ '_gp2gp_flags') }} AS f
            ON src.id = f.id
        WHERE NOT COALESCE(src.lds_is_deleted, FALSE)
    ),
{% endfor %}

metrics AS (
    {% for entity in entities %}
        {% if not loop.first %}UNION ALL{% endif %}
        SELECT '{{ entity | upper }}' AS table_name, 'live_rows' AS metric_name, live_rows AS value_number
        FROM {{ entity }}_metrics
        UNION ALL
        SELECT '{{ entity | upper }}', 'gp2gp_copy_rows', gp2gp_copy_rows
        FROM {{ entity }}_metrics
        UNION ALL
        SELECT '{{ entity | upper }}', 'gp2gp_copy_pct',
            ROUND(100.0 * gp2gp_copy_rows / NULLIF(live_rows, 0), 4)
        FROM {{ entity }}_metrics
        UNION ALL
        SELECT '{{ entity | upper }}', 'gp2gp_import_rows', gp2gp_import_rows
        FROM {{ entity }}_metrics
        UNION ALL
        SELECT '{{ entity | upper }}', 'gp2gp_import_pct',
            ROUND(100.0 * gp2gp_import_rows / NULLIF(live_rows, 0), 4)
        FROM {{ entity }}_metrics
    {% endfor %}
    UNION ALL
    SELECT 'MEDICATION_ORDER', 'import_non_default_duration_rows', import_non_default_duration_rows
    FROM medication_order_metrics
    UNION ALL
    SELECT 'MEDICATION_ORDER', 'import_non_default_cost_rows', import_non_default_cost_rows
    FROM medication_order_metrics
)

SELECT
    table_name::VARCHAR AS table_name,
    metric_name::VARCHAR AS metric_name,
    NULL::VARCHAR AS practice_code,
    value_number::NUMBER(38, 4) AS value_number,
    NULL::VARCHAR AS value_text,
    NULL::DATE AS value_date
FROM metrics
