{{
    config(alias='PERSON')
}}

/*
Single daily scan through source policies. Downstream models read this cache.
*/

SELECT
    id,
    lds_source_record_id,
    lds_source_dataset,
    req_nhs_number,
    matched_nhs_no,
    gender,
    date_of_birth,
    date_of_birth_precision,
    date_of_birth_year,
    date_of_birth_month,
    date_of_death,
    date_of_death_precision,
    date_of_death_year,
    date_of_death_month,
    death_notification_status,
    postcode,
    preferred_contact_method,
    nominated_pharmacy,
    dispensing_doctor,
    medical_appliance_supplier,
    gp_practice_code,
    gp_registration_date,
    nhais_posting_id,
    is_patient_flagged_sensitive,
    error_success_code,
    is_latest_for_person,
    lds_is_deleted,
    source_extraction_date,
    lds_transform_datetime
FROM {{ source('olids_pseudo', 'PERSON') }}
