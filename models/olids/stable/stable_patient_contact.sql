{{
    config(
        cluster_by=['patient_id'],
        transient=false,
        alias='patient_contact'
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
    contact_type_source_concept_id,
    start_date,
    end_date,
    lds_is_deleted,
    publisher_organisation_code,
    source_extraction_date,
    lds_transform_datetime
FROM {{ ref('base_olids_patient_contact') }}
