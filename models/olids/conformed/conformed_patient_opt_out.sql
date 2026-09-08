{{
    config(
        secure=true,
        alias='patient_opt_out')
}}

/*
Conformed PATIENT_OPT_OUT view.
Uses the landing cache for the WNL pseudonymised feed.
Mirrors conformed_national_data_opt_out, which this table will replace.
*/

SELECT
    src.lds_business_id,
    src.lds_record_id,
    src.patient_instruction_category,
    src.patient_instruction_state,
    src.lds_is_deleted,
    src.effective_from,
    src.effective_to,
    src.is_latest,
    -- cast matches conformed_patient.sk_patient_id so opt-out joins stay type-consistent
    TRY_TO_NUMBER(src.sk_patient_id) AS sk_patient_id
FROM {{ ref('landing_patient_opt_out') }} AS src
-- opt-out rows without a patient key cannot be joined; upstream carries a handful
WHERE TRY_TO_NUMBER(src.sk_patient_id) IS NOT NULL
-- upstream carries a couple of duplicated record ids; keep the latest
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY src.lds_record_id
    ORDER BY src.effective_from DESC NULLS LAST
) = 1
