{{
    config(alias='base_olids_concept_map')
}}

/*
CONCEPT_MAP Base View
OLIDS terminology concept mappings from CONCEPT_MAP source.
Passthrough view. The primary key column is now `mapped_item_id` (UUID);
`source_code_id` / `target_code_id` were renamed to `source_concept_id` /
`target_concept_id`. `lds_id` / `lds_business_key` / `lds_dataset_id` were
dropped in the latest OLIDS release.
*/

SELECT
    mapped_item_id,
    concept_map_id,
    concept_map_resource_id,
    concept_map_url,
    concept_map_version,
    source_concept_id,
    source_system,
    source_code,
    source_display,
    target_concept_id,
    target_system,
    target_code,
    target_display,
    is_primary,
    is_active,
    equivalence,
    lakehouse_date_processed,
    lakehouse_datetime_updated,
    lds_is_deleted,
    lds_start_datetime
FROM {{ source('olids_terminology', 'CONCEPT_MAP') }}
