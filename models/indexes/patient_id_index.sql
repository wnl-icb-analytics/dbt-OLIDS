{{
    config(
        alias='patient_id_index',
        materialized='incremental',
        unique_key='source_patient_id',
        on_schema_change='fail',
        full_refresh=false,
        transient=false,
        tags=['index']
    )
}}

WITH source_ids AS (
    SELECT DISTINCT
        id::VARCHAR AS source_patient_id,
        'OLIDS' AS source_feed,
        1 AS feed_order
    FROM {{ ref('landing_patient') }}
    WHERE id IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT
        id::VARCHAR AS source_patient_id,
        'SYNAPSE' AS source_feed,
        2 AS feed_order
    FROM {{ source('olids_masked', 'PATIENT') }}
    WHERE id IS NOT NULL
),
deduplicated AS (
    SELECT
        source_patient_id,
        source_feed
    FROM source_ids
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY source_patient_id
        ORDER BY feed_order
    ) = 1
),
new_ids AS (
    SELECT
        source_patient_id,
        source_feed
    FROM deduplicated
    {% if is_incremental() %}
    WHERE source_patient_id NOT IN (
        SELECT source_patient_id
        FROM {{ this }}
    )
    {% endif %}
),
assigned AS (
    SELECT
        source_patient_id,
        {% if is_incremental() %}
        COALESCE((SELECT MAX(patient_seq) FROM {{ this }}), 0)
            + ROW_NUMBER() OVER (ORDER BY source_patient_id) AS patient_seq,
        {% else %}
        ROW_NUMBER() OVER (ORDER BY source_patient_id) AS patient_seq,
        {% endif %}
        source_feed,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS first_seen_at
    FROM new_ids
)

SELECT
    source_patient_id,
    patient_seq,
    'PT' || {{ encode_crockford32('patient_seq') }} AS patient_id,
    source_feed,
    first_seen_at
FROM assigned
