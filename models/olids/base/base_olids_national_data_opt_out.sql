{{
    config(
        secure=true,
        alias='national_data_opt_out')
}}

/*
NATIONAL_DATA_OPT_OUT base view.
Uses the landing cache for the WNL pseudonymised feed.
*/

SELECT
    src.lds_business_id,
    src.lds_record_id,
    src.preference_type,
    src.preference_status,
    src.lds_is_deleted,
    src.effective_from,
    src.effective_to,
    src.is_latest,
    -- cast matches base_olids_patient.sk_patient_id so opt-out joins stay type-consistent
    TRY_TO_NUMBER(src.sk_patient_id) AS sk_patient_id
FROM {{ ref('landing_national_data_opt_out') }} AS src
