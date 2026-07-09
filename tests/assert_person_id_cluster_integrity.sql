WITH keyed_members AS (
    SELECT
        person_id,
        sk_patient_id,
        birth_year,
        birth_month
    FROM {{ ref('person_id_index') }}
    WHERE
        sk_patient_id IS NOT NULL
        AND birth_year IS NOT NULL
        AND birth_month IS NOT NULL
)

SELECT
    person_id,
    COUNT(
        DISTINCT
        sk_patient_id::VARCHAR
        || '-'
        || birth_year::VARCHAR
        || '-'
        || birth_month::VARCHAR
    ) AS identity_key_count
FROM keyed_members
GROUP BY person_id
HAVING identity_key_count > 1
