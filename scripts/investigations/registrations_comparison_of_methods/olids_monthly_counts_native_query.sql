-- Extract OLIDS registration counts (overall and by life stage)
-- Target Date: 2025-11-01 (matching PMCT extract date)
-- NCL Configuration
USE ROLE "ISL-USERGROUP-SECONDEES-NCL";
USE DATABASE "Data_Store_OLIDS_Alpha";
USE WAREHOUSE "WH_NCL_OLIDS_M";
WITH patient_death_date AS (
    SELECT
        ID AS PATIENT_ID,
        DEATH_YEAR,
        DEATH_MONTH,
        DEATH_YEAR IS NOT NULL AS IS_DECEASED,
        CASE
            WHEN DEATH_YEAR IS NOT NULL
            AND DEATH_MONTH IS NOT NULL THEN DATEADD(
                DAY,
                FLOOR(
                    DAY(
                        LAST_DAY(
                            TO_DATE(
                                TO_VARCHAR(DEATH_YEAR) || '-' || TO_VARCHAR(DEATH_MONTH) || '-01'
                            )
                        )
                    ) / 2
                ),
                TO_DATE(
                    TO_VARCHAR(DEATH_YEAR) || '-' || TO_VARCHAR(DEATH_MONTH) || '-01'
                )
            )
        END AS DEATH_DATE_APPROX
    FROM
        "Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT
),
patient_with_age AS (
    SELECT
        P.ID AS PATIENT_ID,
        P.SK_PATIENT_ID,
        P.BIRTH_YEAR,
        P.BIRTH_MONTH,
        CASE
            WHEN P.BIRTH_YEAR IS NOT NULL AND P.BIRTH_MONTH IS NOT NULL THEN
                DATEADD(
                    DAY,
                    FLOOR(
                        DAY(
                            LAST_DAY(
                                TO_DATE(
                                    TO_VARCHAR(P.BIRTH_YEAR) || '-' || TO_VARCHAR(P.BIRTH_MONTH) || '-01'
                                )
                            )
                        ) / 2
                    ),
                    TO_DATE(
                        TO_VARCHAR(P.BIRTH_YEAR) || '-' || TO_VARCHAR(P.BIRTH_MONTH) || '-01'
                    )
                )
        END AS BIRTH_DATE_APPROX,
        CASE
            WHEN P.BIRTH_YEAR IS NOT NULL THEN
                DATEDIFF(YEAR, 
                    DATEADD(
                        DAY,
                        FLOOR(
                            DAY(
                                LAST_DAY(
                                    TO_DATE(
                                        TO_VARCHAR(P.BIRTH_YEAR) || '-' || TO_VARCHAR(P.BIRTH_MONTH) || '-01'
                                    )
                                )
                            ) / 2
                        ),
                        TO_DATE(
                            TO_VARCHAR(P.BIRTH_YEAR) || '-' || TO_VARCHAR(P.BIRTH_MONTH) || '-01'
                        )
                    ),
                    DATE '2025-11-01'
                )
        END AS AGE_ON_TARGET_DATE,
        CASE
            WHEN P.BIRTH_YEAR IS NOT NULL THEN
                CASE
                    WHEN DATEDIFF(YEAR, 
                        DATEADD(
                            DAY,
                            FLOOR(
                                DAY(
                                    LAST_DAY(
                                        TO_DATE(
                                            TO_VARCHAR(P.BIRTH_YEAR) || '-' || TO_VARCHAR(P.BIRTH_MONTH) || '-01'
                                        )
                                    )
                                ) / 2
                            ),
                            TO_DATE(
                                TO_VARCHAR(P.BIRTH_YEAR) || '-' || TO_VARCHAR(P.BIRTH_MONTH) || '-01'
                            )
                        ),
                        DATE '2025-11-01'
                    ) BETWEEN 0 AND 4 THEN '0-4 (Young Children)'
                    WHEN DATEDIFF(YEAR, 
                        DATEADD(
                            DAY,
                            FLOOR(
                                DAY(
                                    LAST_DAY(
                                        TO_DATE(
                                            TO_VARCHAR(P.BIRTH_YEAR) || '-' || TO_VARCHAR(P.BIRTH_MONTH) || '-01'
                                        )
                                    )
                                ) / 2
                            ),
                            TO_DATE(
                                TO_VARCHAR(P.BIRTH_YEAR) || '-' || TO_VARCHAR(P.BIRTH_MONTH) || '-01'
                            )
                        ),
                        DATE '2025-11-01'
                    ) BETWEEN 5 AND 11 THEN '5-11 (Children)'
                    WHEN DATEDIFF(YEAR, 
                        DATEADD(
                            DAY,
                            FLOOR(
                                DAY(
                                    LAST_DAY(
                                        TO_DATE(
                                            TO_VARCHAR(P.BIRTH_YEAR) || '-' || TO_VARCHAR(P.BIRTH_MONTH) || '-01'
                                        )
                                    )
                                ) / 2
                            ),
                            TO_DATE(
                                TO_VARCHAR(P.BIRTH_YEAR) || '-' || TO_VARCHAR(P.BIRTH_MONTH) || '-01'
                            )
                        ),
                        DATE '2025-11-01'
                    ) BETWEEN 12 AND 17 THEN '12-17 (Teenagers)'
                    WHEN DATEDIFF(YEAR, 
                        DATEADD(
                            DAY,
                            FLOOR(
                                DAY(
                                    LAST_DAY(
                                        TO_DATE(
                                            TO_VARCHAR(P.BIRTH_YEAR) || '-' || TO_VARCHAR(P.BIRTH_MONTH) || '-01'
                                        )
                                    )
                                ) / 2
                            ),
                            TO_DATE(
                                TO_VARCHAR(P.BIRTH_YEAR) || '-' || TO_VARCHAR(P.BIRTH_MONTH) || '-01'
                            )
                        ),
                        DATE '2025-11-01'
                    ) BETWEEN 18 AND 24 THEN '18-24 (Young Adults)'
                    WHEN DATEDIFF(YEAR, 
                        DATEADD(
                            DAY,
                            FLOOR(
                                DAY(
                                    LAST_DAY(
                                        TO_DATE(
                                            TO_VARCHAR(P.BIRTH_YEAR) || '-' || TO_VARCHAR(P.BIRTH_MONTH) || '-01'
                                        )
                                    )
                                ) / 2
                            ),
                            TO_DATE(
                                TO_VARCHAR(P.BIRTH_YEAR) || '-' || TO_VARCHAR(P.BIRTH_MONTH) || '-01'
                            )
                        ),
                        DATE '2025-11-01'
                    ) BETWEEN 25 AND 44 THEN '25-44 (Adults)'
                    WHEN DATEDIFF(YEAR, 
                        DATEADD(
                            DAY,
                            FLOOR(
                                DAY(
                                    LAST_DAY(
                                        TO_DATE(
                                            TO_VARCHAR(P.BIRTH_YEAR) || '-' || TO_VARCHAR(P.BIRTH_MONTH) || '-01'
                                        )
                                    )
                                ) / 2
                            ),
                            TO_DATE(
                                TO_VARCHAR(P.BIRTH_YEAR) || '-' || TO_VARCHAR(P.BIRTH_MONTH) || '-01'
                            )
                        ),
                        DATE '2025-11-01'
                    ) BETWEEN 45 AND 64 THEN '45-64 (Middle-Aged Adults)'
                    WHEN DATEDIFF(YEAR, 
                        DATEADD(
                            DAY,
                            FLOOR(
                                DAY(
                                    LAST_DAY(
                                        TO_DATE(
                                            TO_VARCHAR(P.BIRTH_YEAR) || '-' || TO_VARCHAR(P.BIRTH_MONTH) || '-01'
                                        )
                                    )
                                ) / 2
                            ),
                            TO_DATE(
                                TO_VARCHAR(P.BIRTH_YEAR) || '-' || TO_VARCHAR(P.BIRTH_MONTH) || '-01'
                            )
                        ),
                        DATE '2025-11-01'
                    ) BETWEEN 65 AND 74 THEN '65-74 (Elderly)'
                    WHEN DATEDIFF(YEAR, 
                        DATEADD(
                            DAY,
                            FLOOR(
                                DAY(
                                    LAST_DAY(
                                        TO_DATE(
                                            TO_VARCHAR(P.BIRTH_YEAR) || '-' || TO_VARCHAR(P.BIRTH_MONTH) || '-01'
                                        )
                                    )
                                ) / 2
                            ),
                            TO_DATE(
                                TO_VARCHAR(P.BIRTH_YEAR) || '-' || TO_VARCHAR(P.BIRTH_MONTH) || '-01'
                            )
                        ),
                        DATE '2025-11-01'
                    ) BETWEEN 75 AND 84 THEN '75-84 (Very Elderly)'
                    WHEN DATEDIFF(YEAR, 
                        DATEADD(
                            DAY,
                            FLOOR(
                                DAY(
                                    LAST_DAY(
                                        TO_DATE(
                                            TO_VARCHAR(P.BIRTH_YEAR) || '-' || TO_VARCHAR(P.BIRTH_MONTH) || '-01'
                                        )
                                    )
                                ) / 2
                            ),
                            TO_DATE(
                                TO_VARCHAR(P.BIRTH_YEAR) || '-' || TO_VARCHAR(P.BIRTH_MONTH) || '-01'
                            )
                        ),
                        DATE '2025-11-01'
                    ) >= 85 THEN '85+ (Very Elderly)'
                    ELSE 'Unknown'
                END
            ELSE 'Unknown'
        END AS LIFE_STAGE
    FROM
        "Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT P
),
patient_episodes_active AS (
    SELECT
        EOC.PATIENT_ID,
        EOC.ORGANISATION_ID_PUBLISHER,
        PWA.SK_PATIENT_ID,
        PWA.AGE_ON_TARGET_DATE,
        PWA.LIFE_STAGE,
        EOC.EPISODE_OF_CARE_START_DATE,
        EOC.EPISODE_OF_CARE_END_DATE,
        EOC.EPISODE_TYPE_SOURCE_CONCEPT_ID,
        COALESCE(SC.DISPLAY, 'Unknown Type') AS EPISODE_TYPE_DISPLAY
    FROM
        "Data_Store_OLIDS_Alpha".OLIDS_COMMON.EPISODE_OF_CARE EOC
        INNER JOIN patient_with_age PWA ON EOC.PATIENT_ID = PWA.PATIENT_ID
        LEFT JOIN patient_death_date PDD ON EOC.PATIENT_ID = PDD.PATIENT_ID
        LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT SC ON EOC.EPISODE_TYPE_SOURCE_CONCEPT_ID = SC.ID
    WHERE
        EOC.EPISODE_OF_CARE_START_DATE IS NOT NULL
        AND EOC.PATIENT_ID IS NOT NULL
        AND EOC.ORGANISATION_ID_PUBLISHER IS NOT NULL
        AND PWA.SK_PATIENT_ID IS NOT NULL
        -- Episode active on target date
        AND EOC.EPISODE_OF_CARE_START_DATE <= DATE '2025-11-01'
        AND (
            EOC.EPISODE_OF_CARE_END_DATE IS NULL
            OR EOC.EPISODE_OF_CARE_END_DATE > DATE '2025-11-01'
        )
        -- Patient alive on target date
        AND (
            PDD.IS_DECEASED = FALSE
            OR PDD.DEATH_DATE_APPROX IS NULL
            OR PDD.DEATH_DATE_APPROX > DATE '2025-11-01'
        )
        -- No age filter - include all ages
        -- Filter to REGULAR episode types only (matching EMIS comparison pattern)
        AND COALESCE(SC.DISPLAY, 'Unknown Type') = 'Regular'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY PWA.SK_PATIENT_ID,
        EOC.ORGANISATION_ID_PUBLISHER
        ORDER BY
            EOC.EPISODE_OF_CARE_START_DATE DESC,
            EOC.ID
    ) = 1
),
olids_overall_counts AS (
    SELECT
        O.ORGANISATION_CODE AS PRACTICE_CODE,
        O.NAME AS PRACTICE_NAME,
        COUNT(DISTINCT PEA.SK_PATIENT_ID) AS OLIDS_TOTAL_COUNT
    FROM
        patient_episodes_active PEA
        INNER JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.ORGANISATION O ON PEA.ORGANISATION_ID_PUBLISHER = O.ID
    WHERE
        O.ORGANISATION_CODE IS NOT NULL
    GROUP BY
        O.ORGANISATION_CODE,
        O.NAME
),
olids_by_life_stage AS (
    SELECT
        O.ORGANISATION_CODE AS PRACTICE_CODE,
        O.NAME AS PRACTICE_NAME,
        PEA.LIFE_STAGE,
        COUNT(DISTINCT PEA.SK_PATIENT_ID) AS OLIDS_COUNT_BY_LIFE_STAGE
    FROM
        patient_episodes_active PEA
        INNER JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.ORGANISATION O ON PEA.ORGANISATION_ID_PUBLISHER = O.ID
    WHERE
        O.ORGANISATION_CODE IS NOT NULL
        AND PEA.LIFE_STAGE IS NOT NULL
        AND PEA.LIFE_STAGE != 'Unknown'
    GROUP BY
        O.ORGANISATION_CODE,
        O.NAME,
        PEA.LIFE_STAGE
)
SELECT 
    COALESCE(OOC.PRACTICE_CODE, OLS.PRACTICE_CODE) AS PRACTICE_CODE,
    COALESCE(OOC.PRACTICE_NAME, OLS.PRACTICE_NAME) AS PRACTICE_NAME,
    OOC.OLIDS_TOTAL_COUNT,
    OLS.LIFE_STAGE,
    OLS.OLIDS_COUNT_BY_LIFE_STAGE
FROM olids_overall_counts OOC
FULL OUTER JOIN olids_by_life_stage OLS 
    ON OOC.PRACTICE_CODE = OLS.PRACTICE_CODE
ORDER BY PRACTICE_CODE, LIFE_STAGE;

