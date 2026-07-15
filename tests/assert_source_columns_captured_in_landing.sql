{{ config(severity='warn') }}

-- Every source column should be captured by its landing cache.
-- A hit means the feed added (or renamed) a column landing does not
-- yet select: new data is flowing that we silently ignore. Warn only;
-- adopt the column or record the exclusion here.

{% set src_db = source('olids_pseudo', 'PATIENT').database %}

WITH source_columns AS (
    SELECT
        table_name,
        column_name
    FROM "{{ src_db }}".information_schema.columns
    WHERE table_schema = 'OLIDS_PSEUDO'
),

landing_columns AS (
    SELECT
        table_name,
        column_name
    FROM "{{ target.database }}".information_schema.columns
    WHERE table_schema = 'LANDING'
)

SELECT
    s.table_name,
    s.column_name
FROM source_columns AS s
LEFT JOIN landing_columns AS l
    ON s.table_name = l.table_name AND s.column_name = l.column_name
WHERE
    l.column_name IS NULL
    AND s.table_name IN (SELECT table_name FROM landing_columns)
