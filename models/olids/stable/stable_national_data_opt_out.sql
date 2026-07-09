{{
    config(
        cluster_by=['sk_patient_id'],
        transient=false,
        alias='national_data_opt_out'
    )
}}

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
FROM {{ ref('conformed_national_data_opt_out') }}
