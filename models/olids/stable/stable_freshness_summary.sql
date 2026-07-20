{{
    config(
        materialized='table',
        alias='FRESHNESS_SUMMARY')
}}

/*
One row per table: the scan view of feed health. Rolls up FRESHNESS and adds
the worst-lagging publisher; tables without a publisher column (location,
organisation, person, patient_person) appear with processing watermarks only.
*/

{% set table_level_only = [
    {'name': 'location', 'ext': true},
    {'name': 'organisation', 'ext': true},
    {'name': 'person', 'ext': true},
    {'name': 'patient_person', 'ext': false},
] %}

WITH rollup AS (
    SELECT
        table_name,
        MAX(max_source_extraction_date) AS max_source_extraction_date,
        MAX(max_lds_transform_datetime) AS max_lds_transform_datetime,
        MAX(max_activity_date) AS max_activity_date,
        MAX(consensus_activity_date) AS consensus_activity_date,
        COUNT_IF(max_activity_date IS NOT NULL) AS practices_reporting,
        COUNT_IF(activity_lag_days > 7) AS practices_lagging_consensus_7d,
        COUNT_IF(activity_lag_days > 14) AS practices_lagging_consensus_14d
    FROM {{ ref('stable_freshness') }}
    GROUP BY table_name
),

worst_publisher AS (
    SELECT
        table_name,
        publisher_code AS worst_publisher_code,
        activity_lag_days AS worst_publisher_lag_days
    FROM {{ ref('stable_freshness') }}
    WHERE activity_lag_days IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY table_name
        ORDER BY activity_lag_days DESC, publisher_code
    ) = 1
)

SELECT
    r.table_name,
    r.max_source_extraction_date,
    r.max_lds_transform_datetime,
    r.consensus_activity_date,
    r.max_activity_date,
    r.practices_reporting,
    r.practices_lagging_consensus_7d,
    r.practices_lagging_consensus_14d,
    w.worst_publisher_code,
    w.worst_publisher_lag_days
FROM rollup AS r
LEFT JOIN worst_publisher AS w ON r.table_name = w.table_name

{% for t in table_level_only %}
UNION ALL
SELECT
    '{{ t.name | upper }}',
    {% if t.ext %}MAX(source_extraction_date){% else %}NULL{% endif %},
    MAX(lds_transform_datetime),
    NULL, NULL, NULL, NULL, NULL, NULL, NULL
FROM {{ ref('stable_' ~ t.name) }}
{% endfor %}
