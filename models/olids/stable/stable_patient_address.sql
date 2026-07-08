{{
    config(
        cluster_by=['patient_id'],
        transient=false,
        alias='patient_address'
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
    is_home_address,
    address_type_source_concept_id,
    postcode,
    start_date,
    end_date,
    lds_is_deleted,
    publisher_organisation_code,
    source_extraction_date,
    lds_transform_datetime
FROM {{ ref('base_olids_patient_address') }}
