{{
    config(alias='NATIONAL_DATA_OPT_OUT')
}}

/*
Single daily scan through source policies. Downstream models read this cache.
*/

SELECT
    lds_business_id,
    lds_record_id,
    sk_patient_id,
    preference_type,
    preference_status,
    lds_is_deleted,
    effective_from,
    effective_to,
    is_latest
FROM {{ source('olids_pseudo', 'NATIONAL_DATA_OPT_OUT') }}
