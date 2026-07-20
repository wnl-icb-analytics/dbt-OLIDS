-- Every landing column must still exist on its source object.
-- A hit means the feed dropped or renamed a column the landing cache
-- carries: the next landing build will fail. Runs before landing in
-- dbt build, so drift is named before warehouse time is spent.
-- Landing aliases match source table names one to one.

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
    l.table_name,
    l.column_name
FROM landing_columns AS l
LEFT JOIN source_columns AS s
    ON l.table_name = s.table_name AND l.column_name = s.column_name
WHERE
    s.column_name IS NULL
    AND l.table_name IN (SELECT table_name FROM source_columns)
