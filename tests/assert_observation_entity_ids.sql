-- Synthetic IDs exercise the shared-ID case without source records.
WITH source_records AS (
    SELECT column1 AS source_entity, column2::UUID AS source_record_id
    FROM VALUES
        ('observation', '00000000-0000-0000-0000-000000000001'),
        ('allergy_intolerance', '00000000-0000-0000-0000-000000000001'),
        ('referral_request', '00000000-0000-0000-0000-000000000001'),
        ('referral_request', '00000000-0000-0000-0000-000000000002')
),

assigned AS (
    SELECT
        source_entity,
        source_record_id,
        CASE
            WHEN source_entity = 'observation' THEN source_record_id
            ELSE UUID_STRING(
                '6ba7b811-9dad-11d1-80b4-00c04fd430c8',
                'olids:' || source_entity || ':' || source_record_id::VARCHAR
            )::UUID
        END AS id
    FROM source_records
)

SELECT 'source entity IDs are not distinct' AS failure
FROM assigned
HAVING COUNT(DISTINCT id) != 4

UNION ALL

SELECT 'native observation ID changed'
FROM assigned
WHERE source_entity = 'observation' AND id != source_record_id
