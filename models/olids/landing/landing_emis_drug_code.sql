{{
    config(alias='EMIS_DRUG_CODE')
}}

/*
Single daily scan through source policies. Downstream models read this cache.
*/

SELECT
    concept_id,
    code_id,
    term,
    dmd_product_code_id,
    bnf_chapter_ref
FROM {{ source('olids_pseudo', 'EMIS_DRUG_CODE') }}
