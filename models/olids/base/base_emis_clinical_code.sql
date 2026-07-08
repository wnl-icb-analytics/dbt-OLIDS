{{
    config(
        secure=true,
        alias='emis_clinical_code')
}}

/*
EMIS_CLINICAL_CODE base view.
Uses the landing cache for the WNL pseudonymised feed.
*/

SELECT
    src.concept_id,
    src.code_id,
    src.term,
    src.read_term_id,
    src.snomed_ct_concept_id,
    src.snomed_ct_description_id,
    src.national_code,
    src.national_code_category,
    src.national_description,
    src.emis_code_category_description
FROM {{ ref('landing_emis_clinical_code') }} AS src
WHERE src.snomed_ct_concept_id IS NOT NULL
