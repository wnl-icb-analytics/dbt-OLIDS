-- Compare registration counts between OLIDS source data and EMIS static extract
-- With breakdown by episode type as percentages
-- Using reference table: DATA_LAB_OLIDS_UAT.REFERENCE.EMIS_LIST_SIZE_2025_11_04
-- Target Date: 2025-11-04
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
patient_episodes_with_type AS (
    SELECT
        EOC.PATIENT_ID,
        EOC.ORGANISATION_ID_PUBLISHER,
        P.SK_PATIENT_ID,
        EOC.EPISODE_OF_CARE_START_DATE,
        EOC.EPISODE_OF_CARE_END_DATE,
        EOC.EPISODE_TYPE_SOURCE_CONCEPT_ID,
        COALESCE(SC.DISPLAY, 'Unknown Type') AS EPISODE_TYPE_DISPLAY
    FROM
        "Data_Store_OLIDS_Alpha".OLIDS_COMMON.EPISODE_OF_CARE EOC
        INNER JOIN "Data_Store_OLIDS_Alpha".OLIDS_MASKED.PATIENT P ON EOC.PATIENT_ID = P.ID
        LEFT JOIN patient_death_date PDD ON EOC.PATIENT_ID = PDD.PATIENT_ID
        LEFT JOIN "Data_Store_OLIDS_Alpha".OLIDS_TERMINOLOGY.CONCEPT SC ON EOC.EPISODE_TYPE_SOURCE_CONCEPT_ID = SC.ID
    WHERE
        EOC.EPISODE_OF_CARE_START_DATE IS NOT NULL
        AND EOC.PATIENT_ID IS NOT NULL
        AND EOC.ORGANISATION_ID_PUBLISHER IS NOT NULL
        AND P.SK_PATIENT_ID IS NOT NULL
        -- Episode active on target date
        AND EOC.EPISODE_OF_CARE_START_DATE <= DATE '2025-11-04'
        AND (
            EOC.EPISODE_OF_CARE_END_DATE IS NULL
            OR EOC.EPISODE_OF_CARE_END_DATE > DATE '2025-11-04'
        )
        -- Patient alive on target date
        AND (
            PDD.IS_DECEASED = FALSE
            OR PDD.DEATH_DATE_APPROX IS NULL
            OR PDD.DEATH_DATE_APPROX > DATE '2025-11-04'
        )
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY P.SK_PATIENT_ID,
        EOC.ORGANISATION_ID_PUBLISHER
        ORDER BY
            EOC.EPISODE_OF_CARE_START_DATE DESC,
            EOC.ID
    ) = 1
),
olids_registrations AS (
    SELECT
        O.ORGANISATION_CODE AS PRACTICE_CODE,
        O.NAME AS PRACTICE_NAME,
        COUNT(DISTINCT PE.SK_PATIENT_ID) AS OLIDS_REGISTERED_PATIENTS
    FROM
        patient_episodes_with_type PE
        INNER JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.ORGANISATION O ON PE.ORGANISATION_ID_PUBLISHER = O.ID
    WHERE
        O.ORGANISATION_CODE IS NOT NULL
    GROUP BY
        O.ORGANISATION_CODE,
        O.NAME
),
olids_by_type AS (
    SELECT
        O.ORGANISATION_CODE AS PRACTICE_CODE,
        PE.EPISODE_TYPE_DISPLAY,
        COUNT(DISTINCT PE.SK_PATIENT_ID) AS PATIENT_COUNT
    FROM
        patient_episodes_with_type PE
        INNER JOIN "Data_Store_OLIDS_Alpha".OLIDS_COMMON.ORGANISATION O ON PE.ORGANISATION_ID_PUBLISHER = O.ID
    WHERE
        O.ORGANISATION_CODE IS NOT NULL
    GROUP BY
        O.ORGANISATION_CODE,
        PE.EPISODE_TYPE_DISPLAY
),
type_pivot AS (
    SELECT
        PRACTICE_CODE,
        SUM(
            CASE
                WHEN EPISODE_TYPE_DISPLAY = 'Regular' THEN PATIENT_COUNT
                ELSE 0
            END
        ) AS TYPE_REGULAR,
        SUM(
            CASE
                WHEN EPISODE_TYPE_DISPLAY = 'Temporary' THEN PATIENT_COUNT
                ELSE 0
            END
        ) AS TYPE_TEMPORARY,
        SUM(
            CASE
                WHEN EPISODE_TYPE_DISPLAY IN ('Emergency', 'Immediately Necessary') THEN PATIENT_COUNT
                ELSE 0
            END
        ) AS TYPE_EMERGENCY,
        SUM(
            CASE
                WHEN EPISODE_TYPE_DISPLAY = 'Private' THEN PATIENT_COUNT
                ELSE 0
            END
        ) AS TYPE_PRIVATE,
        SUM(
            CASE
                WHEN EPISODE_TYPE_DISPLAY IN (
                    'Externally Registered',
                    'Community Registered',
                    'Walk-In Patient',
                    'Pre Registration'
                ) THEN PATIENT_COUNT
                ELSE 0
            END
        ) AS TYPE_OTHER_REGISTRATIONS,
        SUM(
            CASE
                WHEN EPISODE_TYPE_DISPLAY IN (
                    'Rheumatology',
                    'Dermatology',
                    'Minor Surgery',
                    'Ultrasound',
                    'Rehabilitation',
                    'Contraceptive Services',
                    'Child Health Services',
                    'Maternity Services',
                    'Yellow Fever',
                    'Acupuncture',
                    'Diabetic'
                ) THEN PATIENT_COUNT
                ELSE 0
            END
        ) AS TYPE_CLINICAL_SERVICES,
        SUM(
            CASE
                WHEN EPISODE_TYPE_DISPLAY = 'Other' THEN PATIENT_COUNT
                ELSE 0
            END
        ) AS TYPE_OTHER,
        SUM(
            CASE
                WHEN EPISODE_TYPE_DISPLAY NOT IN (
                    'Regular',
                    'Temporary',
                    'Emergency',
                    'Immediately Necessary',
                    'Private',
                    'Externally Registered',
                    'Community Registered',
                    'Walk-In Patient',
                    'Pre Registration',
                    'Rheumatology',
                    'Dermatology',
                    'Minor Surgery',
                    'Ultrasound',
                    'Rehabilitation',
                    'Contraceptive Services',
                    'Child Health Services',
                    'Maternity Services',
                    'Yellow Fever',
                    'Acupuncture',
                    'Diabetic',
                    'Other',
                    'Unknown Type'
                ) THEN PATIENT_COUNT
                ELSE 0
            END
        ) AS TYPE_UNMAPPED
    FROM
        olids_by_type
    GROUP BY
        PRACTICE_CODE
),
emis_reference AS (
    SELECT
        CODE AS PRACTICE_CODE,
        GP_PRACTICE AS PRACTICE_NAME,
        LIST_SIZE AS EMIS_LIST_SIZE
    FROM
        DATA_LAB_OLIDS_UAT.REFERENCE.EMIS_LIST_SIZE_2025_11_04
    WHERE
        CODE IS NOT NULL
),
final_results AS (
    SELECT
        COALESCE(EMIS.PRACTICE_CODE, OLIDS.PRACTICE_CODE) AS PRACTICE_CODE,
        COALESCE(EMIS.PRACTICE_NAME, OLIDS.PRACTICE_NAME) AS PRACTICE_NAME,
        EMIS.EMIS_LIST_SIZE,
        TP.TYPE_REGULAR AS OLIDS_REGULAR_REGISTRATIONS,
        OLIDS.OLIDS_REGISTERED_PATIENTS,
        (
            TP.TYPE_REGULAR - EMIS.EMIS_LIST_SIZE
        ) AS DIFFERENCE,
        ROUND(
            100.0 * (
                TP.TYPE_REGULAR - EMIS.EMIS_LIST_SIZE
            ) / NULLIF(EMIS.EMIS_LIST_SIZE, 0),
            2
        ) AS DIFFERENCE_PCT,
        CASE
            WHEN EMIS.EMIS_LIST_SIZE IS NULL THEN 'OLIDS Only'
            WHEN OLIDS.OLIDS_REGISTERED_PATIENTS IS NULL THEN 'EMIS Only'
            WHEN TP.TYPE_REGULAR IS NULL THEN 'No Regular Episodes'
            WHEN ABS(
                100.0 * (
                    TP.TYPE_REGULAR - EMIS.EMIS_LIST_SIZE
                ) / NULLIF(EMIS.EMIS_LIST_SIZE, 0)
            ) >= 20 THEN 'Major Difference (±20%+)'
            WHEN ABS(
                100.0 * (
                    TP.TYPE_REGULAR - EMIS.EMIS_LIST_SIZE
                ) / NULLIF(EMIS.EMIS_LIST_SIZE, 0)
            ) >= 5 THEN 'Moderate Difference (5-20%)'
            WHEN ABS(
                100.0 * (
                    TP.TYPE_REGULAR - EMIS.EMIS_LIST_SIZE
                ) / NULLIF(EMIS.EMIS_LIST_SIZE, 0)
            ) >= 1 THEN 'Good Match (1-5%)'
            ELSE 'Excellent Match (<1%)'
        END AS MATCH_CATEGORY,
        TP.TYPE_REGULAR,
        ROUND(
            100.0 * TP.TYPE_REGULAR / NULLIF(OLIDS.OLIDS_REGISTERED_PATIENTS, 0),
            2
        ) AS TYPE_REGULAR_PCT_OF_OLIDS,
        ROUND(
            100.0 * TP.TYPE_REGULAR / NULLIF(EMIS.EMIS_LIST_SIZE, 0),
            2
        ) AS TYPE_REGULAR_PCT_MATCH_EMIS,
        CASE
            WHEN EMIS.EMIS_LIST_SIZE IS NULL THEN 'No EMIS Data'
            WHEN TP.TYPE_REGULAR IS NULL THEN 'No Regular Episodes'
            WHEN ABS(
                100.0 * TP.TYPE_REGULAR / NULLIF(EMIS.EMIS_LIST_SIZE, 0) - 100
            ) < 1 THEN 'Excellent Match (<1% diff)'
            WHEN ABS(
                100.0 * TP.TYPE_REGULAR / NULLIF(EMIS.EMIS_LIST_SIZE, 0) - 100
            ) <= 20 THEN 'Good Match (1-20% diff)'
            ELSE 'Poor Match (>20% diff)'
        END AS REGULAR_MATCH_CATEGORY,
        (
            TP.TYPE_REGULAR + TP.TYPE_TEMPORARY + TP.TYPE_EMERGENCY
        ) AS TYPE_REG_TEMP_EMERG_TOTAL,
        ROUND(
            100.0 * (
                TP.TYPE_REGULAR + TP.TYPE_TEMPORARY + TP.TYPE_EMERGENCY
            ) / NULLIF(EMIS.EMIS_LIST_SIZE, 0),
            2
        ) AS TYPE_REG_TEMP_EMERG_PCT_MATCH_EMIS,
        CASE
            WHEN EMIS.EMIS_LIST_SIZE IS NULL THEN 'No EMIS Data'
            WHEN (
                TP.TYPE_REGULAR + TP.TYPE_TEMPORARY + TP.TYPE_EMERGENCY
            ) IS NULL THEN 'No Episodes'
            WHEN ABS(
                100.0 * (
                    TP.TYPE_REGULAR + TP.TYPE_TEMPORARY + TP.TYPE_EMERGENCY
                ) / NULLIF(EMIS.EMIS_LIST_SIZE, 0) - 100
            ) < 1 THEN 'Excellent Match (<1% diff)'
            WHEN ABS(
                100.0 * (
                    TP.TYPE_REGULAR + TP.TYPE_TEMPORARY + TP.TYPE_EMERGENCY
                ) / NULLIF(EMIS.EMIS_LIST_SIZE, 0) - 100
            ) <= 20 THEN 'Good Match (1-20% diff)'
            ELSE 'Poor Match (>20% diff)'
        END AS REG_TEMP_EMERG_MATCH_CATEGORY,
        TP.TYPE_TEMPORARY,
        ROUND(
            100.0 * TP.TYPE_TEMPORARY / NULLIF(OLIDS.OLIDS_REGISTERED_PATIENTS, 0),
            2
        ) AS TYPE_TEMPORARY_PCT,
        TP.TYPE_EMERGENCY,
        ROUND(
            100.0 * TP.TYPE_EMERGENCY / NULLIF(OLIDS.OLIDS_REGISTERED_PATIENTS, 0),
            2
        ) AS TYPE_EMERGENCY_PCT,
        TP.TYPE_PRIVATE,
        ROUND(
            100.0 * TP.TYPE_PRIVATE / NULLIF(OLIDS.OLIDS_REGISTERED_PATIENTS, 0),
            2
        ) AS TYPE_PRIVATE_PCT,
        TP.TYPE_OTHER_REGISTRATIONS,
        ROUND(
            100.0 * TP.TYPE_OTHER_REGISTRATIONS / NULLIF(OLIDS.OLIDS_REGISTERED_PATIENTS, 0),
            2
        ) AS TYPE_OTHER_REGISTRATIONS_PCT,
        TP.TYPE_CLINICAL_SERVICES,
        ROUND(
            100.0 * TP.TYPE_CLINICAL_SERVICES / NULLIF(OLIDS.OLIDS_REGISTERED_PATIENTS, 0),
            2
        ) AS TYPE_CLINICAL_SERVICES_PCT,
        TP.TYPE_OTHER,
        ROUND(
            100.0 * TP.TYPE_OTHER / NULLIF(OLIDS.OLIDS_REGISTERED_PATIENTS, 0),
            2
        ) AS TYPE_OTHER_PCT,
        TP.TYPE_UNMAPPED,
        ROUND(
            100.0 * TP.TYPE_UNMAPPED / NULLIF(OLIDS.OLIDS_REGISTERED_PATIENTS, 0),
            2
        ) AS TYPE_UNMAPPED_PCT
    FROM
        emis_reference EMIS
        FULL OUTER JOIN olids_registrations OLIDS ON EMIS.PRACTICE_CODE = OLIDS.PRACTICE_CODE
        LEFT JOIN type_pivot TP ON COALESCE(EMIS.PRACTICE_CODE, OLIDS.PRACTICE_CODE) = TP.PRACTICE_CODE
)
SELECT
    *
FROM
    final_results
WHERE
    MATCH_CATEGORY != 'OLIDS Only'
ORDER BY
    OLIDS_REGULAR_REGISTRATIONS DESC NULLS LAST,
    PRACTICE_CODE;

