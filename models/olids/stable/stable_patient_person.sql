{{
    config(
        cluster_by=['patient_id', 'person_id'],
        transient=false,
        alias='patient_person'
    )
}}

SELECT
    id,
    lds_source_record_id,
    patient_id,
    person_id,
    person_uuid,
    lds_business_id_person,
    lds_source_record_id_person,
    gp_practice_code,
    lds_is_deleted,
    lds_transform_datetime
FROM {{ ref('conformed_patient_person') }}
