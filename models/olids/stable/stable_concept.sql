{{
    config(
        cluster_by=['concept_id'],
        transient=false,
        alias='concept'
    )
}}

SELECT
    concept_id,
    code,
    display,
    system,
    present_in_terminology_server,
    is_mapped,
    use_count
FROM {{ ref('base_olids_concept') }}
