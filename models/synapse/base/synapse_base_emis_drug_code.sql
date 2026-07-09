{{
    config(
        secure=true,
        alias='emis_drug_code')
}}

/*
EMIS Drug Code Reference
EMIS-provided lookup mapping EMIS drug codes to dm+d product codes.
*/

SELECT
    emis_drug_code_id,
    olids_emis_drug_code_concept_id,
    term,
    dmd_product_code_id,
    bnf_chapter_ref,
    lds_start_date_time
FROM {{ source('emis_reference', 'PRIMARY_CARE_EMIS_DRUG_CODE') }}
