{{
    config(
        secure=true,
        alias='emis_clinical_code')
}}

/*
EMIS Clinical Code Reference
EMIS-provided lookup mapping EMIS codes to SNOMED concepts.
Used to enrich the concept map where mappings are missing or point to root concept.
*/

SELECT
    emis_code_id,
    olids_emis_code_concept_id,
    term,
    read_term_id,
    snomed_ct_concept_id,
    olids_snomed_concept_id,
    snomed_ct_description_id,
    national_code,
    national_code_category,
    national_description,
    emis_code_category,
    emis_parent_code_id,
    lds_start_date_time
FROM {{ source('emis_reference', 'PRIMARY_CARE_EMIS_CLINICAL_CODE') }}
WHERE snomed_ct_concept_id IS NOT NULL
