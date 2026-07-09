WITH person_sources AS (
    SELECT person_id::VARCHAR AS source_person_id
    FROM {{ ref('landing_patient') }}
    WHERE person_id IS NOT NULL

    UNION DISTINCT

    SELECT id::VARCHAR AS source_person_id
    FROM {{ ref('landing_person') }}
    WHERE id IS NOT NULL

    UNION DISTINCT

    SELECT person_id::VARCHAR AS source_person_id
    FROM {{ source('olids_masked', 'PATIENT') }}
    WHERE person_id IS NOT NULL

    UNION DISTINCT

    SELECT id::VARCHAR AS source_person_id
    FROM {{ source('olids_masked', 'PERSON') }}
    WHERE id IS NOT NULL
),

patient_sources AS (
    SELECT id::VARCHAR AS source_patient_id
    FROM {{ ref('landing_patient') }}
    WHERE id IS NOT NULL

    UNION DISTINCT

    SELECT id::VARCHAR AS source_patient_id
    FROM {{ source('olids_masked', 'PATIENT') }}
    WHERE id IS NOT NULL
),

missing_persons AS (
    SELECT source_person_id
    FROM person_sources
    WHERE source_person_id NOT IN (
        SELECT source_person_id
        FROM {{ ref('person_id_index') }}
    )
),

missing_patients AS (
    SELECT source_patient_id
    FROM patient_sources
    WHERE source_patient_id NOT IN (
        SELECT source_patient_id
        FROM {{ ref('patient_id_index') }}
    )
)

SELECT
    'person' AS index_name,
    source_person_id AS source_id
FROM missing_persons

UNION ALL

SELECT
    'patient' AS index_name,
    source_patient_id AS source_id
FROM missing_patients
