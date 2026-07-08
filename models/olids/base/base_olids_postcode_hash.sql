{{
    config(
        secure=true,
        alias='postcode_hash')
}}

/*
POSTCODE_HASH base view.
Uses the landing cache for the WNL pseudonymised feed.
*/

SELECT
    src.postcode_hash,
    src.outcode,
    src.local_authority_organisation,
    src.primary_care_organisation,
    src.yr2011_lsoa,
    src.yr2021_lsoa,
    src.yr2011_msoa,
    src.yr2021_msoa,
    src.ward,
    src.version,
    src.last_updated
FROM {{ ref('landing_postcode_hash') }} AS src
