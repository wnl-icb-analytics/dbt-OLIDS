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
        'OLIDS'::VARCHAR AS source_feed,
        1::NUMBER(38, 0) AS feed_order
    FROM {{ ref('landing_patient') }}
    WHERE person_id IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT
        id::VARCHAR AS source_person_id,
        'OLIDS'::VARCHAR AS source_feed,
        1::NUMBER(38, 0) AS feed_order
    FROM {{ ref('landing_person') }}
    WHERE id IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT
        person_id::VARCHAR AS source_person_id,
        'SYNAPSE'::VARCHAR AS source_feed,
        2::NUMBER(38, 0) AS feed_order
    FROM {{ source('olids_masked', 'PATIENT') }}
    WHERE person_id IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT
        id::VARCHAR AS source_person_id,
        'SYNAPSE'::VARCHAR AS source_feed,
        2::NUMBER(38, 0) AS feed_order
    FROM {{ source('olids_masked', 'PERSON') }}
    WHERE id IS NOT NULL
),

patient_rows AS (
    SELECT
        person_id::VARCHAR AS source_person_id,
        TRY_TO_NUMBER(sk_patient_id)::NUMBER(38, 0) AS sk_patient_id,
        birth_year::NUMBER(38, 0) AS birth_year,
        birth_month::NUMBER(38, 0) AS birth_month
    FROM {{ ref('landing_patient') }}
    -- deleted registrations excluded: stale attributes must not poison resolution
    WHERE person_id IS NOT NULL
        AND COALESCE(lds_is_deleted, FALSE) = FALSE

    UNION ALL

    SELECT
        person_id::VARCHAR AS source_person_id,
        sk_patient_id::NUMBER(38, 0) AS sk_patient_id,
        birth_year::NUMBER(38, 0) AS birth_year,
        birth_month::NUMBER(38, 0) AS birth_month
    FROM {{ source('olids_masked', 'PATIENT') }}
    WHERE person_id IS NOT NULL
        AND COALESCE(lds_is_deleted, FALSE) = FALSE
),

deduplicated_sources AS (
    SELECT
        source_person_id::VARCHAR AS source_person_id,
        source_feed::VARCHAR AS source_feed
    FROM source_ids
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY source_person_id
        ORDER BY feed_order, source_feed
    ) = 1
),

patient_attributes AS (
    SELECT
        source_person_id::VARCHAR AS source_person_id,
        COUNT(DISTINCT sk_patient_id)::NUMBER(38, 0) AS sk_count,
        COUNT(DISTINCT
            CASE
                WHEN birth_year IS NOT NULL AND birth_month IS NOT NULL
                    THEN birth_year::VARCHAR || '-' || birth_month::VARCHAR
            END
        )::NUMBER(38, 0) AS dob_count,
        MIN(sk_patient_id)::NUMBER(38, 0) AS resolved_sk_patient_id,
        MIN(birth_year)::NUMBER(38, 0) AS resolved_birth_year,
        MIN(birth_month)::NUMBER(38, 0) AS resolved_birth_month
    FROM patient_rows
    GROUP BY source_person_id
),

source_attributes AS (
    SELECT
        src.source_person_id::VARCHAR AS source_person_id,
        src.source_feed::VARCHAR AS source_feed,
        COALESCE(attr.sk_count, 0)::NUMBER(38, 0) AS sk_count,
        COALESCE(attr.dob_count, 0)::NUMBER(38, 0) AS dob_count,
        CASE
            WHEN COALESCE(attr.sk_count, 0) = 1
                THEN attr.resolved_sk_patient_id
        END::NUMBER(38, 0) AS sk_patient_id,
        CASE
            WHEN COALESCE(attr.dob_count, 0) = 1
                THEN attr.resolved_birth_year
        END::NUMBER(38, 0) AS birth_year,
        CASE
            WHEN COALESCE(attr.dob_count, 0) = 1
                THEN attr.resolved_birth_month
        END::NUMBER(38, 0) AS birth_month,
        (
            COALESCE(attr.sk_count, 0) = 1
            AND COALESCE(attr.dob_count, 0) = 1
        )::BOOLEAN AS is_resolvable
    FROM deduplicated_sources AS src
    LEFT JOIN patient_attributes AS attr
        ON src.source_person_id = attr.source_person_id
),

resolvable_keys AS (
    SELECT
        source_person_id::VARCHAR AS source_person_id,
        sk_patient_id::NUMBER(38, 0) AS sk_patient_id,
        birth_year::NUMBER(38, 0) AS birth_year,
        birth_month::NUMBER(38, 0) AS birth_month
    FROM source_attributes
    WHERE is_resolvable
),

key_stats AS (
    SELECT
        sk_patient_id::NUMBER(38, 0) AS sk_patient_id,
        birth_year::NUMBER(38, 0) AS birth_year,
        birth_month::NUMBER(38, 0) AS birth_month,
        COUNT(*)::NUMBER(38, 0) AS cluster_size
    FROM resolvable_keys
    GROUP BY sk_patient_id, birth_year, birth_month
),

sk_stats AS (
    SELECT
        sk_patient_id::NUMBER(38, 0) AS sk_patient_id,
        COUNT(DISTINCT source_person_id)::NUMBER(38, 0) AS sk_source_count
    FROM resolvable_keys
    GROUP BY sk_patient_id
),

classified_sources AS (
    SELECT
        src.source_person_id::VARCHAR AS source_person_id,
        src.source_feed::VARCHAR AS source_feed,
        src.sk_patient_id::NUMBER(38, 0) AS sk_patient_id,
        src.birth_year::NUMBER(38, 0) AS birth_year,
        src.birth_month::NUMBER(38, 0) AS birth_month,
        src.sk_count::NUMBER(38, 0) AS sk_count,
        src.dob_count::NUMBER(38, 0) AS dob_count,
        src.is_resolvable::BOOLEAN AS is_resolvable,
        COALESCE(key_stats.cluster_size, 0)::NUMBER(38, 0) AS cluster_size,
        COALESCE(sk_stats.sk_source_count, 0)::NUMBER(38, 0) AS sk_source_count,
        (
            src.is_resolvable
            AND COALESCE(key_stats.cluster_size, 0) > 1
        )::BOOLEAN AS cluster_eligible,
        CASE
            WHEN src.sk_count > 1 THEN 'multi_sk'
            WHEN src.dob_count > 1 THEN 'ambiguous_dob'
            WHEN src.is_resolvable
                AND COALESCE(key_stats.cluster_size, 0) = 1
                AND COALESCE(sk_stats.sk_source_count, 0) > 1
                THEN 'sk_dob_mismatch'
        END::VARCHAR AS review_reason
    FROM source_attributes AS src
    LEFT JOIN key_stats
        ON src.sk_patient_id = key_stats.sk_patient_id
        AND src.birth_year = key_stats.birth_year
        AND src.birth_month = key_stats.birth_month
    LEFT JOIN sk_stats
        ON src.sk_patient_id = sk_stats.sk_patient_id
),

new_sources AS (
    SELECT
        classified_sources.source_person_id::VARCHAR AS source_person_id,
        classified_sources.source_feed::VARCHAR AS source_feed,
        classified_sources.sk_patient_id::NUMBER(38, 0) AS sk_patient_id,
        classified_sources.birth_year::NUMBER(38, 0) AS birth_year,
        classified_sources.birth_month::NUMBER(38, 0) AS birth_month,
        classified_sources.sk_count::NUMBER(38, 0) AS sk_count,
        classified_sources.dob_count::NUMBER(38, 0) AS dob_count,
        classified_sources.is_resolvable::BOOLEAN AS is_resolvable,
        classified_sources.cluster_eligible::BOOLEAN AS cluster_eligible,
        classified_sources.review_reason::VARCHAR AS review_reason
    FROM classified_sources
    {% if is_incremental() %}
        WHERE NOT EXISTS (
            SELECT 1
            FROM {{ this }} AS existing
            WHERE existing.source_person_id = classified_sources.source_person_id
        )
    {% endif %}
),

{% if is_incremental() %}

existing_candidates AS (
    SELECT
        sk_patient_id::NUMBER(38, 0) AS sk_patient_id,
        birth_year::NUMBER(38, 0) AS birth_year,
        birth_month::NUMBER(38, 0) AS birth_month,
        COUNT(DISTINCT person_id)::NUMBER(38, 0) AS candidate_count,
        MIN(person_id)::NUMBER(38, 0) AS candidate_person_id,
        MIN(person_seq)::NUMBER(38, 0) AS candidate_person_seq
    FROM {{ this }}
    WHERE sk_patient_id IS NOT NULL
        AND birth_year IS NOT NULL
        AND birth_month IS NOT NULL
    GROUP BY sk_patient_id, birth_year, birth_month
),

existing_sk_stats AS (
    SELECT
        sk_patient_id::NUMBER(38, 0) AS sk_patient_id,
        COUNT(DISTINCT person_id)::NUMBER(38, 0) AS existing_sk_person_count
    FROM {{ this }}
    WHERE sk_patient_id IS NOT NULL
    GROUP BY sk_patient_id
),

new_with_candidates AS (
    SELECT
        src.source_person_id::VARCHAR AS source_person_id,
        src.source_feed::VARCHAR AS source_feed,
        src.sk_patient_id::NUMBER(38, 0) AS sk_patient_id,
        src.birth_year::NUMBER(38, 0) AS birth_year,
        src.birth_month::NUMBER(38, 0) AS birth_month,
        src.sk_count::NUMBER(38, 0) AS sk_count,
        src.dob_count::NUMBER(38, 0) AS dob_count,
        src.is_resolvable::BOOLEAN AS is_resolvable,
        COALESCE(candidates.candidate_count, 0)::NUMBER(38, 0) AS candidate_count,
        candidates.candidate_person_id::NUMBER(38, 0) AS candidate_person_id,
        candidates.candidate_person_seq::NUMBER(38, 0) AS candidate_person_seq,
        COALESCE(existing_sk_stats.existing_sk_person_count, 0)::NUMBER(38, 0) AS existing_sk_person_count,
        src.review_reason::VARCHAR AS source_review_reason
    FROM new_sources AS src
    LEFT JOIN existing_candidates AS candidates
        ON src.sk_patient_id = candidates.sk_patient_id
        AND src.birth_year = candidates.birth_year
        AND src.birth_month = candidates.birth_month
    LEFT JOIN existing_sk_stats
        ON src.sk_patient_id = existing_sk_stats.sk_patient_id
),

alias_rows AS (
    SELECT
        source_person_id::VARCHAR AS source_person_id,
        candidate_person_id::NUMBER(38, 0) AS person_id,
        candidate_person_seq::NUMBER(38, 0) AS person_seq,
        sk_patient_id::NUMBER(38, 0) AS sk_patient_id,
        birth_year::NUMBER(38, 0) AS birth_year,
        birth_month::NUMBER(38, 0) AS birth_month,
        source_feed::VARCHAR AS source_feed,
        'sk_dob_alias'::VARCHAR AS match_method,
        NULL::VARCHAR AS review_reason,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS first_seen_at
    FROM new_with_candidates
    WHERE is_resolvable
        AND candidate_count = 1
),

to_mint AS (
    SELECT
        source_person_id::VARCHAR AS source_person_id,
        source_feed::VARCHAR AS source_feed,
        sk_patient_id::NUMBER(38, 0) AS sk_patient_id,
        birth_year::NUMBER(38, 0) AS birth_year,
        birth_month::NUMBER(38, 0) AS birth_month,
        sk_count::NUMBER(38, 0) AS sk_count,
        dob_count::NUMBER(38, 0) AS dob_count,
        is_resolvable::BOOLEAN AS is_resolvable,
        candidate_count::NUMBER(38, 0) AS candidate_count,
        existing_sk_person_count::NUMBER(38, 0) AS existing_sk_person_count,
        source_review_reason::VARCHAR AS source_review_reason
    FROM new_with_candidates
    WHERE NOT (
        is_resolvable
        AND candidate_count = 1
    )
),

new_key_stats AS (
    SELECT
        sk_patient_id::NUMBER(38, 0) AS sk_patient_id,
        birth_year::NUMBER(38, 0) AS birth_year,
        birth_month::NUMBER(38, 0) AS birth_month,
        COUNT(*)::NUMBER(38, 0) AS new_cluster_size
    FROM to_mint
    WHERE is_resolvable
    GROUP BY sk_patient_id, birth_year, birth_month
),

new_sk_stats AS (
    SELECT
        sk_patient_id::NUMBER(38, 0) AS sk_patient_id,
        COUNT(DISTINCT source_person_id)::NUMBER(38, 0) AS new_sk_source_count
    FROM to_mint
    WHERE is_resolvable
    GROUP BY sk_patient_id
),

mint_classified AS (
    SELECT
        mint.source_person_id::VARCHAR AS source_person_id,
        mint.source_feed::VARCHAR AS source_feed,
        mint.sk_patient_id::NUMBER(38, 0) AS sk_patient_id,
        mint.birth_year::NUMBER(38, 0) AS birth_year,
        mint.birth_month::NUMBER(38, 0) AS birth_month,
        mint.is_resolvable::BOOLEAN AS is_resolvable,
        COALESCE(new_key_stats.new_cluster_size, 0)::NUMBER(38, 0) AS new_cluster_size,
        CASE
            WHEN mint.candidate_count > 1 THEN 'ambiguous_candidates'
            WHEN mint.sk_count > 1 THEN 'multi_sk'
            WHEN mint.dob_count > 1 THEN 'ambiguous_dob'
            WHEN mint.is_resolvable
                AND COALESCE(new_key_stats.new_cluster_size, 0) = 1
                AND (
                    COALESCE(new_sk_stats.new_sk_source_count, 0) > 1
                    OR mint.existing_sk_person_count > 0
                )
                THEN 'sk_dob_mismatch'
        END::VARCHAR AS review_reason,
        (
            mint.is_resolvable
            AND COALESCE(new_key_stats.new_cluster_size, 0) > 1
        )::BOOLEAN AS new_cluster_eligible
    FROM to_mint AS mint
    LEFT JOIN new_key_stats
        ON mint.sk_patient_id = new_key_stats.sk_patient_id
        AND mint.birth_year = new_key_stats.birth_year
        AND mint.birth_month = new_key_stats.birth_month
    LEFT JOIN new_sk_stats
        ON mint.sk_patient_id = new_sk_stats.sk_patient_id
),

assignment_units AS (
    SELECT
        CASE
            WHEN new_cluster_eligible
                THEN (
                    'SKDOB:' || sk_patient_id::VARCHAR
                    || ':' || birth_year::VARCHAR
                    || ':' || birth_month::VARCHAR
                )
            ELSE 'SRC:' || source_person_id
        END::VARCHAR AS assignment_key,
        MIN(source_person_id)::VARCHAR AS unit_order_key
    FROM mint_classified
    GROUP BY assignment_key
),

numbered_units AS (
    SELECT
        assignment_key::VARCHAR AS assignment_key,
        (
            COALESCE((SELECT MAX(person_seq) FROM {{ this }}), 0)
            + DENSE_RANK() OVER (ORDER BY unit_order_key, assignment_key)
        )::NUMBER(38, 0) AS person_seq
    FROM assignment_units
),

minted_rows AS (
    SELECT
        mint.source_person_id::VARCHAR AS source_person_id,
        (100000000 + numbered_units.person_seq)::NUMBER(38, 0) AS person_id,
        numbered_units.person_seq::NUMBER(38, 0) AS person_seq,
        mint.sk_patient_id::NUMBER(38, 0) AS sk_patient_id,
        mint.birth_year::NUMBER(38, 0) AS birth_year,
        mint.birth_month::NUMBER(38, 0) AS birth_month,
        mint.source_feed::VARCHAR AS source_feed,
        CASE
            WHEN mint.new_cluster_eligible THEN 'sk_dob_cluster'
            ELSE 'minted'
        END::VARCHAR AS match_method,
        mint.review_reason::VARCHAR AS review_reason,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS first_seen_at
    FROM mint_classified AS mint
    INNER JOIN numbered_units
        ON CASE
            WHEN mint.new_cluster_eligible
                THEN (
                    'SKDOB:' || mint.sk_patient_id::VARCHAR
                    || ':' || mint.birth_year::VARCHAR
                    || ':' || mint.birth_month::VARCHAR
                )
            ELSE 'SRC:' || mint.source_person_id
        END = numbered_units.assignment_key
)

SELECT
    source_person_id::VARCHAR AS source_person_id,
    person_id::NUMBER(38, 0) AS person_id,
    person_seq::NUMBER(38, 0) AS person_seq,
    sk_patient_id::NUMBER(38, 0) AS sk_patient_id,
    birth_year::NUMBER(38, 0) AS birth_year,
    birth_month::NUMBER(38, 0) AS birth_month,
    source_feed::VARCHAR AS source_feed,
    match_method::VARCHAR AS match_method,
    review_reason::VARCHAR AS review_reason,
    first_seen_at::TIMESTAMP_NTZ AS first_seen_at
FROM alias_rows

UNION ALL

SELECT
    source_person_id::VARCHAR AS source_person_id,
    person_id::NUMBER(38, 0) AS person_id,
    person_seq::NUMBER(38, 0) AS person_seq,
    sk_patient_id::NUMBER(38, 0) AS sk_patient_id,
    birth_year::NUMBER(38, 0) AS birth_year,
    birth_month::NUMBER(38, 0) AS birth_month,
    source_feed::VARCHAR AS source_feed,
    match_method::VARCHAR AS match_method,
    review_reason::VARCHAR AS review_reason,
    first_seen_at::TIMESTAMP_NTZ AS first_seen_at
FROM minted_rows

{% else %}

assignment_units AS (
    SELECT
        CASE
            WHEN cluster_eligible
                THEN (
                    'SKDOB:' || sk_patient_id::VARCHAR
                    || ':' || birth_year::VARCHAR
                    || ':' || birth_month::VARCHAR
                )
            ELSE 'SRC:' || source_person_id
        END::VARCHAR AS assignment_key,
        MIN(source_person_id)::VARCHAR AS unit_order_key
    FROM new_sources
    GROUP BY assignment_key
),

numbered_units AS (
    SELECT
        assignment_key::VARCHAR AS assignment_key,
        DENSE_RANK() OVER (
            ORDER BY unit_order_key, assignment_key
        )::NUMBER(38, 0) AS person_seq
    FROM assignment_units
),

assigned_rows AS (
    SELECT
        src.source_person_id::VARCHAR AS source_person_id,
        (100000000 + numbered_units.person_seq)::NUMBER(38, 0) AS person_id,
        numbered_units.person_seq::NUMBER(38, 0) AS person_seq,
        src.sk_patient_id::NUMBER(38, 0) AS sk_patient_id,
        src.birth_year::NUMBER(38, 0) AS birth_year,
        src.birth_month::NUMBER(38, 0) AS birth_month,
        src.source_feed::VARCHAR AS source_feed,
        CASE
            WHEN src.cluster_eligible THEN 'sk_dob_cluster'
            ELSE 'minted'
        END::VARCHAR AS match_method,
        src.review_reason::VARCHAR AS review_reason,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS first_seen_at
    FROM new_sources AS src
    INNER JOIN numbered_units
        ON CASE
            WHEN src.cluster_eligible
                THEN (
                    'SKDOB:' || src.sk_patient_id::VARCHAR
                    || ':' || src.birth_year::VARCHAR
                    || ':' || src.birth_month::VARCHAR
                )
            ELSE 'SRC:' || src.source_person_id
        END = numbered_units.assignment_key
)

SELECT
    source_person_id::VARCHAR AS source_person_id,
    person_id::NUMBER(38, 0) AS person_id,
    person_seq::NUMBER(38, 0) AS person_seq,
    sk_patient_id::NUMBER(38, 0) AS sk_patient_id,
    birth_year::NUMBER(38, 0) AS birth_year,
    birth_month::NUMBER(38, 0) AS birth_month,
    source_feed::VARCHAR AS source_feed,
    match_method::VARCHAR AS match_method,
    review_reason::VARCHAR AS review_reason,
    first_seen_at::TIMESTAMP_NTZ AS first_seen_at
FROM assigned_rows

{% endif %}
