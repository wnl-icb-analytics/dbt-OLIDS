-- Extract monthly practice counts from PMCT (GPPracticePatientsRegisteredMonthlySourceAppendRevise)
-- Using ENGINEER role to access PMCT data
-- Target Date: 2025-11-01
-- Sub ICB Location Code: 93C
-- All ages included (no age filter)
USE ROLE "ENGINEER";
USE DATABASE "DATA_LAKE";
USE WAREHOUSE "WH_NCL_OLIDS_M";
WITH pmct_by_age AS (
    SELECT 
        O."Organisation_Code" AS PRACTICE_CODE,
        O."Organisation_Name" AS PRACTICE_NAME,
        pcmt.AGE,
        SUM(pcmt.NUMBER_OF_PATIENTS) AS PMCT_COUNT_BY_AGE
    FROM DATA_LAKE.PMCT."GPPracticePatientsRegisteredMonthlySourceAppendRevise" pcmt
    INNER JOIN "Dictionary"."dbo"."Organisation" O
        ON pcmt.ORG_CODE = O."Organisation_Code"
    WHERE pcmt.extract_date = '2025-11-01' 
        AND pcmt.SUB_ICB_LOCATION_CODE = '93C'
        AND pcmt.AGE != 'ALL'  -- Exclude 'ALL' category but include '95+'
    GROUP BY O."Organisation_Code", O."Organisation_Name", pcmt.AGE
),
pmct_life_stage AS (
    SELECT 
        PRACTICE_CODE,
        PRACTICE_NAME,
        CASE 
            WHEN AGE = '95+' THEN '85+ (Very Elderly)'  -- Map '95+' to 85+ category
            WHEN TRY_CAST(AGE AS INTEGER) BETWEEN 0 AND 4 THEN '0-4 (Young Children)'
            WHEN TRY_CAST(AGE AS INTEGER) BETWEEN 5 AND 11 THEN '5-11 (Children)'
            WHEN TRY_CAST(AGE AS INTEGER) BETWEEN 12 AND 17 THEN '12-17 (Teenagers)'
            WHEN TRY_CAST(AGE AS INTEGER) BETWEEN 18 AND 24 THEN '18-24 (Young Adults)'
            WHEN TRY_CAST(AGE AS INTEGER) BETWEEN 25 AND 44 THEN '25-44 (Adults)'
            WHEN TRY_CAST(AGE AS INTEGER) BETWEEN 45 AND 64 THEN '45-64 (Middle-Aged Adults)'
            WHEN TRY_CAST(AGE AS INTEGER) BETWEEN 65 AND 74 THEN '65-74 (Elderly)'
            WHEN TRY_CAST(AGE AS INTEGER) BETWEEN 75 AND 84 THEN '75-84 (Very Elderly)'
            WHEN TRY_CAST(AGE AS INTEGER) >= 85 THEN '85+ (Very Elderly)'
            ELSE 'Unknown'
        END AS LIFE_STAGE,
        SUM(PMCT_COUNT_BY_AGE) AS PMCT_COUNT_BY_LIFE_STAGE
    FROM pmct_by_age
    GROUP BY PRACTICE_CODE, PRACTICE_NAME, LIFE_STAGE
),
pmct_totals AS (
    SELECT 
        PRACTICE_CODE,
        PRACTICE_NAME,
        SUM(PMCT_COUNT_BY_LIFE_STAGE) AS PMCT_TOTAL_COUNT
    FROM pmct_life_stage
    GROUP BY PRACTICE_CODE, PRACTICE_NAME
)
SELECT 
    pt.PRACTICE_CODE,
    pt.PRACTICE_NAME,
    pt.PMCT_TOTAL_COUNT,
    pls.LIFE_STAGE,
    pls.PMCT_COUNT_BY_LIFE_STAGE
FROM pmct_totals pt
LEFT JOIN pmct_life_stage pls ON pt.PRACTICE_CODE = pls.PRACTICE_CODE
ORDER BY pt.PRACTICE_CODE, pls.LIFE_STAGE;

