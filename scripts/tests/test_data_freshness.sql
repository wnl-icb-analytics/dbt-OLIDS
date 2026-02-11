/*
    Test: Data Freshness
    
    Checks that data is being received recently from GP practices.
    Looks at MAX(date_recorded) per record_owner_organisation_code for each table.
    Ignores future dates.
    
    Configuration:
    - FRESHNESS_DAYS: Maximum days since last record (default: 5)
    - PASS_THRESHOLD_PCT: Percentage of orgs that must be fresh (default: 90)
    
    Returns standardised test results with PASS/FAIL status.
    
    Tables with date_recorded:
    - OBSERVATION
    - DIAGNOSTIC_ORDER
    - ENCOUNTER
    - MEDICATION_ORDER
    - MEDICATION_STATEMENT
    - PROCEDURE_REQUEST
    - ALLERGY_INTOLERANCE
    - REFERRAL_REQUEST
*/

-- ============================================
-- CONFIGURATION - Edit thresholds here
-- ============================================
SET freshness_days = 5;           -- Data should be within this many days
SET pass_threshold_pct = 90.0;    -- This % of orgs should be fresh

-- ============================================
-- TEST EXECUTION
-- ============================================
WITH org_freshness AS (
    -- OBSERVATION freshness by org
    SELECT
        'OBSERVATION' AS table_name,
        record_owner_organisation_code AS org_code,
        MAX(CASE WHEN date_recorded <= CURRENT_DATE THEN date_recorded END) AS max_date_recorded,
        DATEDIFF('day', MAX(CASE WHEN date_recorded <= CURRENT_DATE THEN date_recorded END), CURRENT_DATE) AS days_since_last
    FROM "Data_Store_OLIDS_Alpha".OLIDS_COMMON.OBSERVATION
    WHERE record_owner_organisation_code IS NOT NULL
    GROUP BY record_owner_organisation_code
    
    UNION ALL
    
    -- ENCOUNTER freshness by org
    SELECT
        'ENCOUNTER' AS table_name,
        record_owner_organisation_code AS org_code,
        MAX(CASE WHEN date_recorded <= CURRENT_DATE THEN date_recorded END) AS max_date_recorded,
        DATEDIFF('day', MAX(CASE WHEN date_recorded <= CURRENT_DATE THEN date_recorded END), CURRENT_DATE) AS days_since_last
    FROM "Data_Store_OLIDS_Alpha".OLIDS_COMMON.ENCOUNTER
    WHERE record_owner_organisation_code IS NOT NULL
    GROUP BY record_owner_organisation_code
    
    UNION ALL
    
    -- MEDICATION_ORDER freshness by org
    SELECT
        'MEDICATION_ORDER' AS table_name,
        record_owner_organisation_code AS org_code,
        MAX(CASE WHEN date_recorded <= CURRENT_DATE THEN date_recorded END) AS max_date_recorded,
        DATEDIFF('day', MAX(CASE WHEN date_recorded <= CURRENT_DATE THEN date_recorded END), CURRENT_DATE) AS days_since_last
    FROM "Data_Store_OLIDS_Alpha".OLIDS_COMMON.MEDICATION_ORDER
    WHERE record_owner_organisation_code IS NOT NULL
    GROUP BY record_owner_organisation_code
    
    UNION ALL
    
    -- MEDICATION_STATEMENT freshness by org
    SELECT
        'MEDICATION_STATEMENT' AS table_name,
        record_owner_organisation_code AS org_code,
        MAX(CASE WHEN date_recorded <= CURRENT_DATE THEN date_recorded END) AS max_date_recorded,
        DATEDIFF('day', MAX(CASE WHEN date_recorded <= CURRENT_DATE THEN date_recorded END), CURRENT_DATE) AS days_since_last
    FROM "Data_Store_OLIDS_Alpha".OLIDS_COMMON.MEDICATION_STATEMENT
    WHERE record_owner_organisation_code IS NOT NULL
    GROUP BY record_owner_organisation_code
    
    UNION ALL
    
    -- DIAGNOSTIC_ORDER freshness by org
    SELECT
        'DIAGNOSTIC_ORDER' AS table_name,
        record_owner_organisation_code AS org_code,
        MAX(CASE WHEN date_recorded <= CURRENT_DATE THEN date_recorded END) AS max_date_recorded,
        DATEDIFF('day', MAX(CASE WHEN date_recorded <= CURRENT_DATE THEN date_recorded END), CURRENT_DATE) AS days_since_last
    FROM "Data_Store_OLIDS_Alpha".OLIDS_COMMON.DIAGNOSTIC_ORDER
    WHERE record_owner_organisation_code IS NOT NULL
    GROUP BY record_owner_organisation_code
    
    UNION ALL
    
    -- ALLERGY_INTOLERANCE freshness by org
    SELECT
        'ALLERGY_INTOLERANCE' AS table_name,
        record_owner_organisation_code AS org_code,
        MAX(CASE WHEN date_recorded <= CURRENT_DATE THEN date_recorded END) AS max_date_recorded,
        DATEDIFF('day', MAX(CASE WHEN date_recorded <= CURRENT_DATE THEN date_recorded END), CURRENT_DATE) AS days_since_last
    FROM "Data_Store_OLIDS_Alpha".OLIDS_COMMON.ALLERGY_INTOLERANCE
    WHERE record_owner_organisation_code IS NOT NULL
    GROUP BY record_owner_organisation_code
    
    UNION ALL
    
    -- PROCEDURE_REQUEST freshness by org
    SELECT
        'PROCEDURE_REQUEST' AS table_name,
        record_owner_organisation_code AS org_code,
        MAX(CASE WHEN date_recorded <= CURRENT_DATE THEN date_recorded END) AS max_date_recorded,
        DATEDIFF('day', MAX(CASE WHEN date_recorded <= CURRENT_DATE THEN date_recorded END), CURRENT_DATE) AS days_since_last
    FROM "Data_Store_OLIDS_Alpha".OLIDS_COMMON.PROCEDURE_REQUEST
    WHERE record_owner_organisation_code IS NOT NULL
    GROUP BY record_owner_organisation_code
    
    UNION ALL
    
    -- REFERRAL_REQUEST freshness by org
    SELECT
        'REFERRAL_REQUEST' AS table_name,
        record_owner_organisation_code AS org_code,
        MAX(CASE WHEN date_recorded <= CURRENT_DATE THEN date_recorded END) AS max_date_recorded,
        DATEDIFF('day', MAX(CASE WHEN date_recorded <= CURRENT_DATE THEN date_recorded END), CURRENT_DATE) AS days_since_last
    FROM "Data_Store_OLIDS_Alpha".OLIDS_COMMON.REFERRAL_REQUEST
    WHERE record_owner_organisation_code IS NOT NULL
    GROUP BY record_owner_organisation_code
),

table_summary AS (
    SELECT
        table_name,
        COUNT(DISTINCT org_code) AS total_orgs,
        COUNT(DISTINCT CASE WHEN days_since_last <= $freshness_days THEN org_code END) AS fresh_orgs,
        COUNT(DISTINCT CASE WHEN days_since_last > $freshness_days THEN org_code END) AS stale_orgs,
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN days_since_last <= $freshness_days THEN org_code END) / NULLIF(COUNT(DISTINCT org_code), 0), 2) AS fresh_pct,
        MIN(days_since_last) AS min_days_since,
        MAX(days_since_last) AS max_days_since,
        ROUND(AVG(days_since_last), 1) AS avg_days_since
    FROM org_freshness
    WHERE max_date_recorded IS NOT NULL
    GROUP BY table_name
)

SELECT
    'data_freshness' AS test_name,
    table_name,
    'date_recorded (by org)' AS test_subject,
    CASE 
        WHEN fresh_pct >= $pass_threshold_pct THEN 'PASS'
        ELSE 'FAIL'
    END AS status,
    fresh_pct AS metric_value,
    $pass_threshold_pct AS threshold,
    OBJECT_CONSTRUCT(
        'total_orgs', total_orgs,
        'fresh_orgs', fresh_orgs,
        'stale_orgs', stale_orgs,
        'fresh_percentage', fresh_pct,
        'freshness_days_threshold', $freshness_days,
        'min_days_since_record', min_days_since,
        'max_days_since_record', max_days_since,
        'avg_days_since_record', avg_days_since
    )::VARCHAR AS details
FROM table_summary
ORDER BY status DESC, fresh_pct ASC, table_name;
