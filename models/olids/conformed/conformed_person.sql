{{
    config(
        materialized='table',
        alias='person')
}}

/*
Conformed PERSON view.

Driven from the patient-person bridge, not the source PERSON feed: the feed is
missing ~830k persons that appear on PATIENT rows. Every bridged person gets a
row; the feed record supplies pseudonymised attributes when present, otherwise
they are NULL (gender and clinical_system fall back to the patient record).
*/

WITH person_spine AS (
    SELECT DISTINCT
        person_id,
        person_uuid
    FROM {{ ref('conformed_patient_person') }}
    WHERE person_id IS NOT NULL AND person_uuid IS NOT NULL
),

gender_fallback AS (
    SELECT
        pp.person_uuid,
        c.display AS gender,
        pat.clinical_system
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
    spine.person_id AS id,
    spine.person_uuid,
    src.lds_source_record_id,
    src.req_nhs_number,
    src.matched_nhs_no,
    src.date_of_birth,
    src.date_of_birth_year,
    src.date_of_birth_month,
    src.date_of_birth_precision,
    src.date_of_death,
    src.date_of_death_year,
    src.date_of_death_month,
    src.date_of_death_precision,
    src.death_notification_status,
    src.postcode,
    src.preferred_contact_method,
    src.nominated_pharmacy,
    src.dispensing_doctor,
    src.medical_appliance_supplier,
    src.gp_practice_code,
    src.gp_registration_date,
    src.nhais_posting_id,
    src.is_patient_flagged_sensitive,
    src.is_latest_for_person,
    src.error_success_code,
    -- from the patient record backing this person row
    gf.clinical_system,
    src.source_extraction_date,
    src.lds_transform_datetime,
    COALESCE(src.lds_is_deleted, FALSE) AS lds_is_deleted,
    COALESCE(src.gender, gf.gender) AS gender
FROM person_spine AS spine
LEFT JOIN {{ ref('landing_person') }} AS src
    ON spine.person_uuid = src.id
LEFT JOIN gender_fallback AS gf
    ON spine.person_uuid = gf.person_uuid
-- identity resolution can map several source person records to one person_id;
-- the person table carries one canonical row per person (latest record wins)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY spine.person_id
    ORDER BY src.lds_transform_datetime DESC NULLS LAST, spine.person_uuid ASC
) = 1
