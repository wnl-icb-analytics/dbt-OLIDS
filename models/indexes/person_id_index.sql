{{
    config(
        alias='person_id_index',
        materialized='incremental',
        unique_key='source_person_id',
        on_schema_change='fail',
        full_refresh=false,
        transient=false,
        tags=['index']
    )
}}

WITH source_ids AS (
    SELECT DISTINCT
        person_id::VARCHAR AS source_person_id,
        'OLIDS' AS source_feed,
        1 AS feed_order
    FROM {{ ref('landing_patient') }}
    WHERE person_id IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT
        id::VARCHAR AS source_person_id,
        'OLIDS' AS source_feed,
        1 AS feed_order
    FROM {{ ref('landing_person') }}
    WHERE id IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT
        person_id::VARCHAR AS source_person_id,
        'SYNAPSE' AS source_feed,
        2 AS feed_order
    FROM {{ source('olids_masked', 'PATIENT') }}
    WHERE person_id IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT
        id::VARCHAR AS source_person_id,
        'SYNAPSE' AS source_feed,
        2 AS feed_order
    FROM {{ source('olids_masked', 'PERSON') }}
    WHERE id IS NOT NULL
),

deduplicated AS (
    SELECT
        source_person_id,
        source_feed
    FROM source_ids
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY source_person_id
        ORDER BY feed_order
    ) = 1
),

new_ids AS (
    SELECT
        source_person_id,
        source_feed
    FROM deduplicated
    {% if is_incremental() %}
        WHERE source_person_id NOT IN (
            SELECT source_person_id
            FROM {{ this }}
        )
    {% endif %}
),

assigned AS (
    SELECT
        source_person_id,
        {% if is_incremental() %}
            (
                COALESCE((SELECT MAX(person_seq) FROM {{ this }}), 0)
                + ROW_NUMBER() OVER (ORDER BY source_person_id)
            )::NUMBER(38, 0
            ) AS person_seq,
        {% else %}
        ROW_NUMBER() OVER (ORDER BY source_person_id)::NUMBER(38, 0) AS person_seq,
        {% endif %}
        source_feed,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS first_seen_at
    FROM new_ids
)

SELECT
    source_person_id,
    person_seq,
    source_feed,
    first_seen_at,
    100000000 + person_seq AS person_id
FROM assigned
