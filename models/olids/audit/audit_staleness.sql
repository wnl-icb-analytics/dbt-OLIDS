WITH domain_dates AS (
    SELECT
        table_name,
        practice_code,
        value_date
    FROM {{ ref('audit_observation') }}
    WHERE metric_name = 'max_activity_date' AND value_date IS NOT NULL

    UNION ALL

    SELECT
        table_name,
        practice_code,
        value_date
    FROM {{ ref('audit_medication_order') }}
    WHERE metric_name = 'max_activity_date' AND value_date IS NOT NULL

    UNION ALL

    SELECT
        table_name,
        practice_code,
        value_date
    FROM {{ ref('audit_episode_of_care') }}
    WHERE metric_name = 'max_activity_date' AND value_date IS NOT NULL

    UNION ALL

    SELECT
        table_name,
        practice_code,
        value_date
    FROM {{ ref('audit_episode_of_care_v2') }}
    WHERE metric_name = 'max_activity_date' AND value_date IS NOT NULL
),

consensus AS (
    SELECT
        table_name,
        DATEADD(
            'day',
            PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY DATEDIFF('day', TO_DATE('1970-01-01'), value_date)
            ),
            TO_DATE('1970-01-01')
        )::DATE AS consensus_activity_date
    FROM domain_dates
    GROUP BY table_name
),

metrics AS (
    SELECT
        consensus.table_name,
        'consensus_activity_date' AS metric_name,
        NULL AS practice_code,
        NULL AS value_number,
        NULL AS value_text,
        consensus.consensus_activity_date AS value_date
    FROM consensus

    UNION ALL

    SELECT
        domain_dates.table_name,
        'practices_reporting',
        NULL,
        COUNT(*),
        NULL,
        NULL
    FROM domain_dates
    GROUP BY domain_dates.table_name

    UNION ALL

    SELECT
        domain_dates.table_name,
        'practices_lagging_consensus_7d',
        NULL,
        COUNT_IF(
            domain_dates.value_date
            < DATEADD('day', -7, consensus.consensus_activity_date)
        ),
        NULL,
        NULL
    FROM domain_dates
    INNER JOIN consensus
        ON domain_dates.table_name = consensus.table_name
    GROUP BY domain_dates.table_name

    UNION ALL

    SELECT
        domain_dates.table_name,
        'practices_lagging_consensus_14d',
        NULL,
        COUNT_IF(
            domain_dates.value_date
            < DATEADD('day', -14, consensus.consensus_activity_date)
        ),
        NULL,
        NULL
    FROM domain_dates
    INNER JOIN consensus
        ON domain_dates.table_name = consensus.table_name
    GROUP BY domain_dates.table_name
)

SELECT
    table_name::VARCHAR AS table_name,
    metric_name::VARCHAR AS metric_name,
    practice_code::VARCHAR AS practice_code,
    value_number::NUMBER(38, 4) AS value_number,
    value_text::VARCHAR AS value_text,
    value_date::DATE AS value_date
FROM metrics
