{{
    config(
        secure=true,
        alias='patient_address')
}}

/*
Conformed PATIENT_ADDRESS view.
Filters to WNL practices and excludes patients outside the filtered spine.
*/

SELECT
    src.id,
    src.lds_source_record_id,
    patient_idx.patient_id,
    person_idx.person_id,
    src.publisher_organisation_id,
    src.provider_organisation_id,
    src.author_organisation_id,
    src.is_home_address,
    src.address_type_source_concept_id,
    src.postcode,
    src.start_date,
    src.end_date,
    src.lds_is_deleted,
    src.publisher_organisation_code,
    patients.clinical_system,
    src.source_extraction_date,
    src.lds_transform_datetime
FROM {{ ref('landing_patient_address') }} AS src
INNER JOIN {{ ref('conformed_patient') }} AS patients
    ON src.patient_id = patients.source_id
LEFT JOIN {{ ref('patient_id_index') }} AS patient_idx
    ON src.patient_id = patient_idx.source_patient_id
LEFT JOIN {{ ref('person_id_index') }} AS person_idx
    ON src.person_id = person_idx.source_person_id
INNER JOIN {{ ref('int_wnl_practices') }} AS wnl_practices
    ON src.publisher_organisation_code = wnl_practices.practice_code
