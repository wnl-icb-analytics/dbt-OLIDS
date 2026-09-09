{{ config(database='REFERENCE', schema='reference_terminology', alias='patient_referral_snomed_codes', tags=['terminology']) }}

-- The agreed referral definition is <<3457005 |Patient referral (procedure)|.
-- Historical inclusion requires one explicit, non-ambiguous active successor.
WITH current_codes AS (
    SELECT DISTINCT "SubtypeID" AS code
    FROM {{ source('ukhfd_snomed', 'dim_SCT_Transitive_Closures') }}
    WHERE "SupertypeID" = 3457005
),
historical_successors AS (
    SELECT h."OldConceptId" AS code, MIN(h."NewConceptId") AS active_code
    FROM {{ source('nhsd_snomed', 'SCT_History') }} AS h
    INNER JOIN {{ source('nhsd_snomed', 'SCT_Concept') }} AS old_concept
        ON h."OldConceptId" = old_concept."Id" AND NOT old_concept."Active"
    INNER JOIN {{ source('nhsd_snomed', 'SCT_Concept') }} AS successor
        ON h."NewConceptId" = successor."Id" AND successor."Active"
    WHERE h."IsAmbiguous" = FALSE
    GROUP BY h."OldConceptId"
    -- Count all active successors before restricting to referrals.
    HAVING COUNT(DISTINCT h."NewConceptId") = 1
),
eligible_codes AS (
    SELECT code, code AS active_code, 'current_descendant' AS classification_basis
    FROM current_codes
    UNION ALL
    SELECT h.code, h.active_code, 'unambiguous_historical_successor' AS classification_basis
    FROM historical_successors AS h
    INNER JOIN current_codes AS c ON h.active_code = c.code
)
SELECT
    c.code::VARCHAR AS code,
    d."Term" AS code_name,
    c.active_code::VARCHAR AS active_code,
    c.classification_basis
FROM eligible_codes AS c
LEFT JOIN {{ source('nhsd_snomed', 'SCT_Description') }} AS d
    ON c.code = d."ConceptId"
    AND d."Active" = TRUE
    AND d."DescriptionType" = 'P'
