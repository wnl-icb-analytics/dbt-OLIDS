-- Published sk_patient_id must resolve to one live person; ownership is settled in conformed_patient.
SELECT
    sk_patient_id,
    COUNT(DISTINCT person_id) AS person_count
FROM {{ ref('stable_patient') }}
WHERE
    sk_patient_id IS NOT NULL
    AND COALESCE(lds_is_deleted, FALSE) = FALSE
GROUP BY sk_patient_id
HAVING person_count > 1
