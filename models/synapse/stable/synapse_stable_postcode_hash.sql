{{
    config(
        cluster_by=['id', 'postcode_hash'],
        alias='postcode_hash',
        transient=false,
        tags=['stable']
    )
}}

SELECT
    id,
    postcode_hash,
    primary_care_organisation,
    local_authority_organisation,
    yr2011_lsoa,
    yr2011_msoa,
    yr2021_lsoa,
    yr2021_msoa,
    effective_from,
    effective_to,
    is_latest,
    lds_is_deleted,
    lds_start_date_time,
    lakehouse_date_processed,
    high_watermark_date_time
FROM {{ ref('synapse_base_olids_postcode_hash') }}
