{{
    config(
        secure=true,
        alias='patient_person')
}}

/*
Conformed patient-person bridge.
Keeps only links for the filtered patient spine.
*/

SELECT
    src.id,
    src.lds_source_record_id,
    patient_idx.patient_id,
    person_idx.person_id,
    src.person_id AS person_uuid,
    src.lds_business_id_person,
    src.lds_source_record_id_person,
    src.gp_practice_code,
    src.lds_is_deleted,
    src.lds_transform_datetime
FROM {{ ref('landing_patient_person') }} AS src
INNER JOIN {{ ref('conformed_patient') }} AS patients
    ON src.patient_id = patients.source_id
LEFT JOIN {{ ref('patient_id_index') }} AS patient_idx
    ON src.patient_id = patient_idx.source_patient_id
LEFT JOIN {{ ref('person_id_index') }} AS person_idx
    ON src.person_id = person_idx.source_person_id
