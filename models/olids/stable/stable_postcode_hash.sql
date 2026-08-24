{{
    config(
        cluster_by=['postcode_hash'],
        transient=false,
        alias='postcode_hash'
    )
}}

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
FROM {{ ref('conformed_postcode_hash') }}
