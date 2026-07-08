{{
    config(
        materialized='table',
        alias='person')
}}

/*
Conformed PERSON view.
Keeps people linked to the filtered patient spine and passes pseudonymised fields through.
*/

WITH gender_fallback AS (
    SELECT
        pp.person_uuid,
        c.display AS gender
    FROM {{ ref('conformed_patient_person') }} AS pp
    INNER JOIN {{ ref('conformed_patient') }} AS pat
        ON pp.patient_id = pat.id
    LEFT JOIN {{ ref('conformed_concept') }} AS c
        ON pat.gender_source_concept_id = c.concept_id
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pp.person_uuid
        ORDER BY pat.lds_transform_datetime DESC NULLS LAST
    ) = 1
)

SELECT
    person_idx.person_id AS id,
    src.id AS person_uuid,
    src.lds_source_record_id,
    src.req_nhs_number,
    src.matched_nhs_no,
    src.date_of_birth,
    src.date_of_birth_year,
    src.date_of_birth_month,
    src.date_of_birth_day,
    src.date_of_birth_time,
    src.date_of_death,
    src.date_of_death_year,
    src.date_of_death_month,
    src.date_of_death_day,
    src.date_of_death_time,
    src.death_notification_status,
    src.postcode,
    src.preferred_contact_method,
    src.nominated_pharmacy,
    src.dispensing_doctor,
    src.medical_appliance_supplier,
    src.gp_practice_code,
    src.gp_registration_date,
    src.nhais_posting_id,
    src.as_at_date,
    src.local_patient_id,
    src.internal_id,
    src.mps_id,
    src.patient_flagged_sensitive,
    src.error_success_code,
    src.lds_is_deleted,
    src.source_extraction_date,
    src.lds_transform_datetime,
    COALESCE(src.gender, gf.gender) AS gender
FROM {{ ref('landing_person') }} AS src
LEFT JOIN gender_fallback AS gf
    ON src.id = gf.person_uuid
LEFT JOIN {{ ref('person_id_index') }} AS person_idx
    ON src.id = person_idx.source_person_id
WHERE EXISTS (
    SELECT 1
    FROM {{ ref('conformed_patient_person') }} AS pp
    WHERE pp.person_uuid = src.id
)
