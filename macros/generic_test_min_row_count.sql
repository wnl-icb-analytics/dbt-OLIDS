{% test min_row_count(model, min_rows) %}

SELECT 1 AS failure
FROM (SELECT COUNT(*) AS n FROM {{ model }})
WHERE n < {{ min_rows }}

{% endtest %}
