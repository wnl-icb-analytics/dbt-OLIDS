/*
    Validation: Numeric person_id Feasibility

    Tests whether hashing person_id (UUID) to a 14-digit numeric key
    is collision-free for our current population.

    Approach: ABS(MOD(MD5_NUMBER_LOWER64(person_id), 9 * 10^13)) + 10^13
    - Produces a deterministic fixed-length 14-digit number from each UUID
    - MD5_NUMBER_LOWER64 returns a signed 64-bit integer from MD5 hash
    - MOD constrains to 0..8_999_999_999_999, offset ensures range 10^13..10^14-1
    - Always exactly 14 digits

    Expected result: zero collisions for ~5M distinct person_ids.
    Run this BEFORE applying changes to the dbt models.
*/

-- ============================================
-- STEP 1: Count distinct person_ids in source
-- ============================================
WITH source_persons AS (
    SELECT DISTINCT
        person_id AS person_uuid,
        ABS(MOD(MD5_NUMBER_LOWER64(person_id), 9 * POWER(10, 13)::NUMBER)) + POWER(10, 13)::NUMBER AS person_id_numeric
    FROM "Data_Store_OLIDS_Alpha".OLIDS_COMMON.PATIENT_PERSON
    WHERE person_id IS NOT NULL
),

-- ============================================
-- STEP 2: Check for collisions
-- ============================================
collision_check AS (
    SELECT
        person_id_numeric,
        COUNT(*) AS uuid_count,
        LISTAGG(person_uuid, ', ') WITHIN GROUP (ORDER BY person_uuid) AS colliding_uuids
    FROM source_persons
    GROUP BY person_id_numeric
    HAVING COUNT(*) > 1
),

-- ============================================
-- STEP 3: Summary statistics
-- ============================================
summary AS (
    SELECT
        COUNT(*) AS total_distinct_uuids,
        COUNT(DISTINCT person_id_numeric) AS total_distinct_numeric_ids,
        MIN(person_id_numeric) AS min_numeric_id,
        MAX(person_id_numeric) AS max_numeric_id,
        AVG(LEN(person_id_numeric::VARCHAR)) AS avg_digit_length,
        MIN(LEN(person_id_numeric::VARCHAR)) AS min_digit_length,
        MAX(LEN(person_id_numeric::VARCHAR)) AS max_digit_length
    FROM source_persons
)

-- ============================================
-- RESULTS
-- ============================================
SELECT
    'numeric_person_id_validation' AS test_name,
    'person_id' AS test_subject,
    CASE
        WHEN s.total_distinct_uuids = s.total_distinct_numeric_ids THEN 'PASS'
        ELSE 'FAIL'
    END AS status,
    s.total_distinct_uuids - s.total_distinct_numeric_ids AS collisions_found,
    OBJECT_CONSTRUCT(
        'total_distinct_uuids', s.total_distinct_uuids,
        'total_distinct_numeric_ids', s.total_distinct_numeric_ids,
        'collision_count', s.total_distinct_uuids - s.total_distinct_numeric_ids,
        'min_numeric_id', s.min_numeric_id,
        'max_numeric_id', s.max_numeric_id,
        'avg_digit_length', ROUND(s.avg_digit_length, 1),
        'min_digit_length', s.min_digit_length,
        'max_digit_length', s.max_digit_length,
        'hash_function', 'ABS(MOD(MD5_NUMBER_LOWER64(person_id), 9*10^13)) + 10^13',
        'colliding_examples', (SELECT ARRAY_AGG(OBJECT_CONSTRUCT('numeric_id', person_id_numeric, 'uuid_count', uuid_count, 'uuids', colliding_uuids)) FROM collision_check LIMIT 10)
    )::VARCHAR AS details
FROM summary s;
