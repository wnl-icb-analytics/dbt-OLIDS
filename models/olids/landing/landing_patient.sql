{{
    config(alias='PATIENT')
}}

/*
Single daily scan through source policies, scoped to NCL practices.
Downstream models read this cache.
*/

SELECT
    id,
    lds_source_record_id,
    person_id,
    publisher_organisation_id,
    provider_organisation_id,
    author_organisation_id,
    registered_practice_organisation_id,
    local_patient_id,
    sk_patient_id,
    title,
    gender_source_concept_id,
    birth_date,
    birth_year,
    birth_month,
    birth_week_iso,
    birth_day,
    death_date,
    death_year,
    death_month,
    death_week_iso,
    is_confidential,
    is_test_patient,
    is_spine_sensitive,
    lds_source_dataset,
    lds_is_deleted,
    publisher_organisation_code,
    source_extraction_date,
    lds_transform_datetime
FROM {{ source('olids_pseudo', 'PATIENT') }}
WHERE publisher_organisation_code IN (
    SELECT ncl.practice_code FROM {{ ref('int_ncl_practices') }} AS ncl
)
