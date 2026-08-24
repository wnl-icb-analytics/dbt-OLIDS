{{
    config(
        materialized='table',
        alias='FRESHNESS')
}}

/*
Per-publisher freshness watermarks, computed from the stable tables themselves
so they always describe exactly what consumers read: a gate-blocked build
leaves them at the previous values rather than advertising unpublished data.

Grain: one row per table_name and publisher_code, every column populated.
Processing watermarks say whether the pipeline is moving; activity watermarks
say whether the clinical content is current. Activity excludes deleted rows
and rows dated after their own extraction, and the consensus is the median of
publisher maxima, so future-dated source rows cannot distort it. Lag is
data-to-data: no CURRENT_DATE. Table-level rollups live in FRESHNESS_SUMMARY.
*/

{% set tables = [
    {'name': 'observation', 'activity': 'clinical_effective_date'},
    {'name': 'medication_order', 'activity': 'clinical_effective_date'},
    {'name': 'medication_statement', 'activity': 'clinical_effective_date'},
    {'name': 'encounter', 'activity': 'clinical_effective_date'},
    {'name': 'appointment', 'activity': 'start_date'},
    {'name': 'episode_of_care', 'activity': 'episode_of_care_start_date'},
    {'name': 'episode_of_care_v2', 'activity': 'episode_of_care_start_date'},
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
        )::DATE AS consensus_activity_date
    FROM per_publisher
    WHERE max_activity_date IS NOT NULL
    GROUP BY table_name
)

SELECT
    p.table_name,
    p.publisher_code,
    p.max_source_extraction_date,
    p.max_lds_transform_datetime,
    p.max_activity_date,
    c.consensus_activity_date,
    -- data-to-data difference; positive means the publisher trails the consensus
    DATEDIFF('day', p.max_activity_date, c.consensus_activity_date)
        AS activity_lag_days
FROM per_publisher AS p
LEFT JOIN consensus AS c ON p.table_name = c.table_name
