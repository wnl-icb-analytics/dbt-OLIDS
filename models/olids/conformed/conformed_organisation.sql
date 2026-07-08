{{
    config(
        secure=true,
        alias='organisation')
}}

/*
Conformed ORGANISATION view.
Restricts to WNL organisations using publisher organisation code.
*/

SELECT
    src.id,
    src.lds_source_record_id,
    src.organisation_code,
    src.assigning_authority_code,
    src.name,
    src.type_description,
    src.primary_location_type_source_concept_id,
    src.postcode,
    src.parent_organisation_id,
    src.open_date,
    src.close_date,
    src.is_obsolete,
    src.lds_is_deleted,
    src.publisher_organisation_code,
    src.source_extraction_date,
    src.lds_transform_datetime
FROM {{ ref('landing_organisation') }} AS src
INNER JOIN {{ ref('int_wnl_practices') }} AS wnl_practices
    ON src.publisher_organisation_code = wnl_practices.practice_code
