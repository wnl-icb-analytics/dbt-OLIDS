{{
    config(alias='POSTCODE_HASH')
}}

/*
Single daily scan through source policies. Downstream models read this cache.
*/

SELECT
    postcode,
    postcode_hash,
    outcode,
    local_authority_organisation,
    primary_care_organisation,
    yr2011_lsoa,
    yr2021_lsoa,
    yr2011_msoa,
    yr2021_msoa,
    ward,
    version,
    last_updated
FROM {{ source('olids_pseudo', 'POSTCODE_HASH') }}
