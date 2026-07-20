{{
    config(
        secure=true,
        alias='organisation')
}}

/*
Conformed ORGANISATION reference view.
Unfiltered: the feed no longer attributes organisation records to a
publisher, so a WNL restriction is not possible. The fuller reference
set improves organisation id resolution downstream.
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
    src.source_extraction_date,
    src.lds_transform_datetime
FROM {{ ref('landing_organisation') }} AS src
