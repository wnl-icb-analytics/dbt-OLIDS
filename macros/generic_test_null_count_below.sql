{% test null_count_below(model, column_name, max_nulls) %}

SELECT 1 AS failure
FROM (
    SELECT COUNT_IF({{ column_name }} IS NULL) AS null_count
    FROM {{ model }}
)
WHERE null_count > {{ max_nulls }}

{% endtest %}
