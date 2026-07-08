{{
    config(
        cluster_by=['patient_address_id'],
        transient=false,
        alias='patient_uprn'
    )
}}

SELECT
    patient_address_id,
    status,
    matched,
    uprn,
    postcode_quality,
    qualifier,
    classification,
    algorithm,
    match_pattern,
    error_message,
    publisher_organisation_code
FROM {{ ref('base_olids_patient_uprn') }}
