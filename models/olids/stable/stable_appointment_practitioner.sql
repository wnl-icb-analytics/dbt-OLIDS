{{
    config(
        cluster_by=['appointment_id'],
        transient=false,
        alias='appointment_practitioner'
    )
}}

SELECT
    id,
    lds_source_record_id,
    patient_id,
    person_id,
    publisher_organisation_id,
    provider_organisation_id,
    author_organisation_id,
    lds_source_record_id_practitioner,
    appointment_id,
    practitioner_id,
    lds_is_deleted,
    publisher_organisation_code,
    source_extraction_date,
    lds_transform_datetime
FROM {{ ref('base_olids_appointment_practitioner') }}
