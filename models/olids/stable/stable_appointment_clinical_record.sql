{{ config(alias='appointment_clinical_record', cluster_by=['appointment_id']) }}

SELECT
    appointment_id,
    clinical_record_type,
    clinical_record_id,
    encounter_id,
    patient_id,
    person_id
FROM {{ ref('conformed_appointment_clinical_record') }}
