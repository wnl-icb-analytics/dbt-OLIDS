{{
    config(alias='PATIENT_UPRN')
}}

/*
Single daily scan through source policies, scoped to NCL practices.
Downstream models read this cache.
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
    publisher_organisation_code
FROM {{ source('olids_pseudo', 'PATIENT_UPRN') }}
WHERE publisher_organisation_code IN (
    SELECT ncl.practice_code FROM {{ ref('int_ncl_practices') }} AS ncl
)
