{{
    config(
        materialized='table',
        alias='patient')
}}

/*
Conformed filtered patient view.
Excludes sensitive, confidential and test patients, then restricts to NCL practices.

sk_patient_id ownership: the feed can hand one sk to several persons. The PDS
trace on PERSON (submitted vs matched NHS number) settles who owns it. Rows
that lose keep their person_id and stay in the population; only sk_patient_id
is nulled. See sk_linkage_status. Rows are still filtered on the submitted sk.
*/

WITH pds AS (
    SELECT
        id,
        req_nhs_number,
        matched_nhs_no
    FROM {{ ref('landing_person') }}
    -- the feed ships one row per person uuid; guard against a future change
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY lds_transform_datetime DESC NULLS LAST
    ) = 1
),

patients AS (
    SELECT
        patient_idx.patient_id AS id,
        src.id AS source_id,
        src.lds_source_record_id,
        person_idx.person_id,
        src.person_id AS person_uuid,
        src.publisher_organisation_id,
        src.provider_organisation_id,
        src.author_organisation_id,
        src.registered_practice_organisation_id,
        src.local_patient_id,
        src.title,
        src.gender_source_concept_id,
        gender_map.source_code AS gender_source_code,
        gender_map.source_display AS gender_source_display,
        gender_map.target_code AS gender_code,
        gender_map.target_display AS gender_display,
        src.birth_date,
        src.birth_year,
        src.birth_month,
        src.birth_week_iso,
        src.birth_day,
        src.death_date,
        src.death_year,
        src.death_month,
        src.death_week_iso,
        src.is_confidential,
        src.is_test_patient,
        src.is_spine_sensitive,
        src.lds_source_dataset,
        src.lds_is_deleted,
        src.publisher_organisation_code,
        src.source_extraction_date,
        src.lds_transform_datetime,
        CASE
            WHEN src.lds_source_dataset ILIKE '%emis%' THEN 'EMIS'
            WHEN
                src.lds_source_dataset ILIKE '%tpp%'
                OR src.lds_source_dataset ILIKE '%systmone%'
                THEN 'SystmOne'
            ELSE src.lds_source_dataset
        END AS clinical_system,
        TRY_TO_NUMBER(src.sk_patient_id) AS submitted_sk_patient_id,
        CASE
            WHEN pds.id IS NULL THEN 'no_person_row'
            WHEN pds.matched_nhs_no IS NULL THEN 'untraced'
            WHEN pds.req_nhs_number = pds.matched_nhs_no THEN 'confirmed'
            ELSE 'contradicted'
        END AS pds_trace_status
    FROM {{ ref('landing_patient') }} AS src
    INNER JOIN {{ ref('int_ncl_practices') }} AS ncl_practices
        ON src.publisher_organisation_code = ncl_practices.practice_code
    LEFT JOIN {{ ref('patient_id_index') }} AS patient_idx
        ON src.id = patient_idx.source_patient_id
    LEFT JOIN {{ ref('person_id_index') }} AS person_idx
        ON src.person_id = person_idx.source_person_id
    LEFT JOIN {{ ref('int_enriched_concept_map') }} AS gender_map
        ON src.gender_source_concept_id = gender_map.source_concept_id
    LEFT JOIN pds
        ON src.person_id = pds.id
    WHERE
        TRY_TO_NUMBER(src.sk_patient_id) IS NOT NULL
        -- strict = FALSE: rows with NULL flags are excluded (unknown sensitivity treated as sensitive)
        AND src.is_spine_sensitive = FALSE
        AND src.is_confidential = FALSE
        AND src.is_test_patient = FALSE
    -- the feed occasionally ships exact duplicate patient rows; keep one deterministically
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY src.id
        ORDER BY src.lds_transform_datetime DESC, src.lds_source_record_id ASC
    ) = 1
),

-- ownership is judged across live persons only; deleted registrations must not contest a key
sk_owners AS (
    SELECT
        submitted_sk_patient_id,
        COUNT(DISTINCT person_id) AS person_count,
        COUNT(DISTINCT CASE
            WHEN pds_trace_status = 'confirmed' THEN person_id
        END) AS confirmed_person_count
    FROM patients
    WHERE COALESCE(lds_is_deleted, FALSE) = FALSE
    GROUP BY submitted_sk_patient_id
),

classified AS (
    SELECT
        patients.*,
        CASE
            WHEN COALESCE(sk_owners.person_count, 1) <= 1 THEN 'sole'
            WHEN
                patients.pds_trace_status = 'confirmed'
                AND sk_owners.confirmed_person_count = 1
                THEN 'pds_owner'
            WHEN
                patients.pds_trace_status = 'confirmed'
                AND sk_owners.confirmed_person_count > 1
                THEN 'ambiguous_owner'
            WHEN sk_owners.confirmed_person_count >= 1 THEN 'contested'
            ELSE 'no_owner'
        END AS sk_linkage_status
    FROM patients
    LEFT JOIN sk_owners
        ON patients.submitted_sk_patient_id = sk_owners.submitted_sk_patient_id
)

SELECT
    id,
    source_id,
    lds_source_record_id,
    person_id,
    person_uuid,
    publisher_organisation_id,
    provider_organisation_id,
    author_organisation_id,
    registered_practice_organisation_id,
    local_patient_id,
    title,
    gender_source_concept_id,
    gender_source_code,
    gender_source_display,
    gender_code,
    gender_display,
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
    lds_transform_datetime,
    clinical_system,
    pds_trace_status,
    sk_linkage_status,
    IFF(
        sk_linkage_status IN ('sole', 'pds_owner'),
        submitted_sk_patient_id,
        NULL
    ) AS sk_patient_id
FROM classified
