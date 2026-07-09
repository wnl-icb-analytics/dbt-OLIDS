{{
    config(
        cluster_by=['concept_id'],
        alias='concept',
        transient=false,
        tags=['stable']
    )
}}

SELECT
    concept_id,
    system,
    code,
    display,
    is_mapped,
    use_count,
    lds_is_deleted,
    lds_start_datetime
FROM {{ ref('synapse_base_olids_concept') }}
