-- Guards the sk_patient_id TEXT to NUMBER casts (PATIENT and NATIONAL_DATA_OPT_OUT).
SELECT sk_patient_id
FROM {{ ref('landing_patient') }}
WHERE
    sk_patient_id IS NOT NULL
    AND TRY_TO_NUMBER(sk_patient_id) IS NULL

UNION ALL

SELECT sk_patient_id
FROM {{ ref('landing_national_data_opt_out') }}
WHERE
    sk_patient_id IS NOT NULL
    AND TRY_TO_NUMBER(sk_patient_id) IS NULL
