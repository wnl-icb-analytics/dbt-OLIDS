{{
    config(
        materialized='table',
        alias='FRESHNESS')
}}

/*
Published freshness watermarks, computed from the stable tables themselves so
they always describe exactly what consumers read: a gate-blocked build leaves
them at the previous values rather than advertising unpublished data.

Grain: one row per table_name and publisher_code; publisher_code is null for
table-level rows. Processing watermarks say whether the pipeline is moving;
activity watermarks say whether the clinical content is current. Activity
excludes deleted rows and rows dated after their own extraction, and the
table-level consensus is the median of publisher maxima, so future-dated
source rows cannot distort it. No CURRENT_DATE: lag is data-to-data.

Tables without processing date columns (concept, concept_map, postcode_hash,
patient_uprn, national_data_opt_out) carry no freshness row.
*/

{% set tables = [
    {'name': 'observation', 'activity': 'clinical_effective_date'},
    {'name': 'medication_order', 'activity': 'clinical_effective_date'},
    {'name': 'medication_statement', 'activity': 'clinical_effective_date'},
    {'name': 'encounter', 'activity': 'clinical_effective_date'},
    {'name': 'appointment', 'activity': 'start_date'},
    {'name': 'episode_of_care', 'activity': 'episode_of_care_start_date'},
    {'name': 'referral_request', 'activity': 'clinical_effective_date'},
    {'name': 'procedure_request', 'activity': 'clinical_effective_date'},
    {'name': 'diagnostic_order', 'activity': 'clinical_effective_date'},
    {'name': 'allergy_intolerance', 'activity': 'clinical_effective_date'},
    {'name': 'patient', 'activity': none},
    {'name': 'patient_address', 'activity': none},
    {'name': 'patient_contact', 'activity': none},
    {'name': 'appointment_practitioner', 'activity': none},
    {'name': 'practitioner', 'activity': none},
    {'name': 'practitioner_in_role', 'activity': none},
    {'name': 'schedule', 'activity': none},
    {'name': 'schedule_practitioner', 'activity': none},
] %}

{% set table_level_only = [
    {'name': 'location', 'ext': true},
    {'name': 'organisation', 'ext': true},
    {'name': 'person', 'ext': true},
    {'name': 'patient_person', 'ext': false},
] %}

WITH per_publisher AS (
    {% for t in tables %}
    SELECT
        '{{ t.name | upper }}' AS table_name,
        publisher_organisation_code AS publisher_code,
        MAX(source_extraction_date) AS max_source_extraction_date,
        MAX(lds_transform_datetime) AS max_lds_transform_datetime,
        {% if t.activity %}
        MAX(CASE
            WHEN
                NOT COALESCE(lds_is_deleted, FALSE)
                AND {{ t.activity }} <= source_extraction_date
                THEN {{ t.activity }}
        END)::DATE AS max_activity_date
        {% else %}
        NULL::DATE AS max_activity_date
        {% endif %}
    FROM {{ ref('stable_' ~ t.name) }}
    WHERE publisher_organisation_code IS NOT NULL
    GROUP BY publisher_organisation_code
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
),

consensus AS (
    SELECT
        table_name,
        DATEADD(
            'day',
            MEDIAN(DATEDIFF('day', TO_DATE('1970-01-01'), max_activity_date)),
            TO_DATE('1970-01-01')
        )::DATE AS consensus_activity_date,
        COUNT(*) AS practices_reporting
    FROM per_publisher
    WHERE max_activity_date IS NOT NULL
    GROUP BY table_name
),

table_level AS (
    SELECT
        p.table_name,
        NULL::VARCHAR AS publisher_code,
        MAX(p.max_source_extraction_date) AS max_source_extraction_date,
        MAX(p.max_lds_transform_datetime) AS max_lds_transform_datetime,
        MAX(p.max_activity_date) AS max_activity_date,
        MAX(c.consensus_activity_date) AS consensus_activity_date,
        MAX(c.practices_reporting) AS practices_reporting,
        COUNT_IF(
            DATEDIFF('day', p.max_activity_date, c.consensus_activity_date) > 7
        ) AS practices_lagging_consensus_7d,
        COUNT_IF(
            DATEDIFF('day', p.max_activity_date, c.consensus_activity_date) > 14
        ) AS practices_lagging_consensus_14d
    FROM per_publisher AS p
    LEFT JOIN consensus AS c ON p.table_name = c.table_name
    GROUP BY p.table_name

    {% for t in table_level_only %}
    UNION ALL
    SELECT
        '{{ t.name | upper }}',
        NULL,
        {% if t.ext %}MAX(source_extraction_date){% else %}NULL{% endif %},
        MAX(lds_transform_datetime),
        NULL, NULL, NULL, NULL, NULL
    FROM {{ ref('stable_' ~ t.name) }}
    {% endfor %}
)

SELECT
    table_name,
    publisher_code,
    max_source_extraction_date,
    max_lds_transform_datetime,
    max_activity_date,
    consensus_activity_date,
    -- data-to-data difference; positive means the publisher trails the consensus
    DATEDIFF('day', max_activity_date, consensus_activity_date) AS activity_lag_days,
    practices_reporting,
    practices_lagging_consensus_7d,
    practices_lagging_consensus_14d
FROM table_level

UNION ALL

SELECT
    p.table_name,
    p.publisher_code,
    p.max_source_extraction_date,
    p.max_lds_transform_datetime,
    p.max_activity_date,
    c.consensus_activity_date,
    DATEDIFF('day', p.max_activity_date, c.consensus_activity_date) AS activity_lag_days,
    NULL AS practices_reporting,
    NULL AS practices_lagging_consensus_7d,
    NULL AS practices_lagging_consensus_14d
FROM per_publisher AS p
LEFT JOIN consensus AS c ON p.table_name = c.table_name
