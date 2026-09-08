{{
    config(alias='PATIENT_OPT_OUT')
}}

/*
Single daily scan through source policies. Downstream models read this cache.
Successor to NATIONAL_DATA_OPT_OUT: same shape, generic category/state names.
*/

SELECT
    lds_business_id,
    lds_record_id,
    sk_patient_id,
    patient_instruction_category,
    patient_instruction_state,
    lds_is_deleted,
    effective_from,
    effective_to,
    is_latest
FROM {{ source('olids_pseudo', 'PATIENT_OPT_OUT') }}
