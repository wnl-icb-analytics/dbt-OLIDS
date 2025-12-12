/*
CONCEPT_MAP Base View
OLIDS terminology concept mappings from CONCEPT_MAP source.
Passthrough view with standard column naming applied.
Includes source and target code/display/system columns for simplified joins.
*/

SELECT
    id,
    lds_id,
    lds_business_key,
    lds_dataset_id,
    concept_map_id,
    concept_map_resource_id,
    concept_map_url,
    concept_map_version,
    source_code_id,
    source_system,
    source_code,
    source_display,
    target_code_id,
    target_system,
    target_code,
    target_display,
    is_primary,
    is_active,
    equivalence,
    lds_start_date_time
FROM {{ source('olids_terminology', 'CONCEPT_MAP') }}
