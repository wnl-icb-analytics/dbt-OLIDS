{{
    config(alias='EMIS_CLINICAL_CODE')
}}

/*
Single daily scan through source policies. Downstream models read this cache.
*/

SELECT
    concept_id,
    code_id,
    term,
    read_term_id,
    snomed_ct_concept_id,
    snomed_ct_description_id,
    national_code,
    national_code_category,
    national_description,
    emis_code_category_description
FROM {{ source('olids_pseudo', 'EMIS_CLINICAL_CODE') }}
