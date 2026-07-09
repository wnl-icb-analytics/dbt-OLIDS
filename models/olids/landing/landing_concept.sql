{{
    config(alias='CONCEPT')
}}

/*
Single daily scan through source policies. Downstream models read this cache.
*/

SELECT
    concept_id,
    code,
    display,
    system,
    present_in_terminology_server,
    is_mapped,
    use_count
FROM {{ source('olids_pseudo', 'CONCEPT') }}
