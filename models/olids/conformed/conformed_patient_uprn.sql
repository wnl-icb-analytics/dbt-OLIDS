{{
    config(
        secure=true,
        alias='patient_uprn')
}}

/*
Conformed PATIENT_UPRN view.
Uses the landing cache for the WNL pseudonymised feed.
*/

SELECT
    src.patient_address_id,
    src.status,
    src.matched,
    src.uprn,
    src.postcode_quality,
    src.qualifier,
    src.classification,
    src.algorithm,
    src.match_pattern,
    src.error_message,
    src.publisher_organisation_code
FROM {{ ref('landing_patient_uprn') }} AS src
