-- noqa: disable=all
-- sqlfluff fix loops on this jinja test header; linting disabled for the file
{% test audit_metric_within_band(model, metric, max_value=none, min_value=none, table_name=none) %}

WITH metric_rows AS (
    SELECT value_number
    FROM {{ model }}
    WHERE
        metric_name = '{{ metric }}'
        AND practice_code IS NULL
        {% if table_name is not none %}
        AND table_name = '{{ table_name }}'
        {% endif %}
)

SELECT value_number
FROM metric_rows
WHERE
    FALSE
    {% if max_value is not none %}
    OR value_number > {{ max_value }}
    {% endif %}
{% if min_value is not none %}
    OR value_number < {{ min_value }}
    {% endif %}

UNION ALL

-- a missing metric row must fail, not pass vacuously
SELECT NULL AS value_number
FROM (SELECT 1)
WHERE NOT EXISTS (SELECT 1 FROM metric_rows)

{% endtest %}
