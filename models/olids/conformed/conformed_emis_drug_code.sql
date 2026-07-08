{{
    config(
        secure=true,
        alias='emis_drug_code')
}}

/*
Conformed EMIS_DRUG_CODE view.
Uses the landing cache for the WNL pseudonymised feed.
*/

SELECT
    src.concept_id,
    src.code_id,
    src.term,
    src.dmd_product_code_id,
    src.bnf_chapter_ref
FROM {{ ref('landing_emis_drug_code') }} AS src
