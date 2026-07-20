{{
    config(
        secure=true,
        alias='patient_person')
}}

/*
Conformed patient-person bridge.

Built from the patient spine, not the source PATIENT_PERSON feed: the feed is
missing ~950k patient links that PATIENT.person_id carries (where both exist
they always agree). Every conformed patient with a person UUID gets a bridge
row; the source feed row supplies bridge metadata when present.

Derived rows (no source feed row) reuse the patient source UUID as id and have
NULL lds_source_record_id.
*/

WITH bridge AS (
    SELECT
        id,
        lds_source_record_id,
        patient_id,
        person_id,
        lds_business_id_person,
        lds_source_record_id_person,
        gp_practice_code,
        lds_is_deleted,
        lds_transform_datetime
    FROM {{ ref('landing_patient_person') }}
    -- the feed can ship duplicate rows per patient; keep the latest
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY patient_id
        ORDER BY lds_transform_datetime DESC NULLS LAST, id
    ) = 1
)

SELECT
    COALESCE(bridge.id, patients.source_id) AS id,
    bridge.lds_source_record_id,
    patients.id AS patient_id,
    COALESCE(patients.person_id, bridge_person_idx.person_id) AS person_id,
    COALESCE(patients.person_uuid, bridge.person_id) AS person_uuid,
    bridge.lds_business_id_person,
    bridge.lds_source_record_id_person,
    COALESCE(bridge.gp_practice_code, patients.publisher_organisation_code)
        AS gp_practice_code,
    COALESCE(bridge.lds_is_deleted, patients.lds_is_deleted) AS lds_is_deleted,
    COALESCE(bridge.lds_transform_datetime, patients.lds_transform_datetime)
        AS lds_transform_datetime,
    patients.clinical_system
FROM {{ ref('conformed_patient') }} AS patients
LEFT JOIN bridge
    ON patients.source_id = bridge.patient_id
-- resolves feed-only person UUIDs for patients without their own person_id
LEFT JOIN {{ ref('person_id_index') }} AS bridge_person_idx
    ON bridge.person_id = bridge_person_idx.source_person_id
WHERE COALESCE(patients.person_uuid, bridge.person_id) IS NOT NULL
