{{
    config(
        alias='person_id_crosswalk',
        materialized='table',
        schema='pseudonymisation',
        tags=['index']
    )
}}

-- Supports consumer cutover for #257/#263 by mapping retired 14-digit
-- person ids to the new indexed ids. Many old ids can map to one new id
-- when identities collapse. The old formula is copied verbatim from the
-- retired generate_person_id macro and applied to the source UUID.
WITH legacy_members AS (
    SELECT
        (
            ABS(MOD(MD5_NUMBER_LOWER64(source_person_id), 9 * POWER(10, 13)::NUMBER))
            + POWER(10, 13)::NUMBER
        )::NUMBER(38, 0) AS old_person_id,
        person_id::NUMBER(38, 0) AS new_person_id
    FROM {{ ref('person_id_index') }}
    WHERE source_feed = 'SYNAPSE'
        AND source_person_id IS NOT NULL
        AND person_id IS NOT NULL
),

member_counts AS (
    SELECT
        new_person_id::NUMBER(38, 0) AS new_person_id,
        COUNT(*)::NUMBER(38, 0) AS member_count
    FROM legacy_members
    GROUP BY new_person_id
)

SELECT
    legacy_members.old_person_id::NUMBER(38, 0) AS old_person_id,
    legacy_members.new_person_id::NUMBER(38, 0) AS new_person_id,
    (member_counts.member_count > 1)::BOOLEAN AS is_collapsed
FROM legacy_members
INNER JOIN member_counts
    ON legacy_members.new_person_id = member_counts.new_person_id
