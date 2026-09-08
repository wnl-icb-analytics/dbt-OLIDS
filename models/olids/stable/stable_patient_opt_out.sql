{{
    config(
        cluster_by=['sk_patient_id'],
        transient=false,
        alias='patient_opt_out'
    )
}}

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
FROM {{ ref('conformed_patient_opt_out') }}
