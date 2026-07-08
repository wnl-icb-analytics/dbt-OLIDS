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
    src.patient_id,
    {{ generate_person_id('src.person_id') }} AS person_id,
    src.person_id AS person_uuid,
    src.lds_business_id_person,
    src.lds_source_record_id_person,
    src.gp_practice_code,
    src.lds_is_deleted,
    src.lds_transform_datetime
FROM {{ ref('landing_patient_person') }} src
INNER JOIN {{ ref('conformed_patient') }} patients
    ON src.patient_id = patients.id
