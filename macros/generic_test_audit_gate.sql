{% test audit_gate(model, audit_model, metric, max_value, table_name=none) %}

WITH metric_rows AS (
    SELECT value_number
    FROM {{ ref(audit_model) }}
    WHERE
        metric_name = '{{ metric }}'
        AND practice_code IS NULL
        {% if table_name is not none %}
        AND table_name = '{{ table_name }}'
        {% endif %}
)

SELECT value_number
FROM metric_rows
WHERE value_number > {{ max_value }}

UNION ALL

-- a missing metric row must fail the gate, not pass it vacuously
SELECT NULL AS value_number
FROM (SELECT 1)
WHERE NOT EXISTS (SELECT 1 FROM metric_rows)

{% endtest %}
