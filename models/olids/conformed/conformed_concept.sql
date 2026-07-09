{{
    config(
        secure=true,
        alias='concept')
}}

/*
Conformed CONCEPT view.
Uses the landing cache for the WNL pseudonymised feed.
*/

SELECT
    src.concept_id,
    src.code,
    src.display,
    src.system,
    src.present_in_terminology_server,
    src.is_mapped,
    src.use_count
FROM {{ ref('landing_concept') }} AS src
