{{
    config(alias='PATIENT_UPRN')
}}

/*
Single daily scan through source policies. Downstream models read this cache.
*/

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
FROM {{ source('olids_pseudo', 'PATIENT_UPRN') }}
