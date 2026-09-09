-- Aggregate only: semantic tags come from public SNOMED fully specified names.
WITH additions AS
  (SELECT 'allergy_intolerance' AS source_entity,
          mapped_concept_code,
          target_system
   FROM OLIDS_ENGINEERING.CONFORMED.ALLERGY_INTOLERANCE
   UNION ALL SELECT 'referral_request',
                    mapped_concept_code,
                    target_system
   FROM OLIDS_ENGINEERING.CONFORMED.REFERRAL_REQUEST),
     fsn AS
  (SELECT "ConceptId",
          regexp_substr("Term", '[(]([^()]*)[)]$', 1, 1, 'e', 1) AS semantic_tag
   FROM "Dictionary"."NHSD_SnomedReportingModel"."SCT_Description"
   WHERE "TypeId"=900000000000003001 QUALIFY row_number() over(PARTITION BY "ConceptId"
                                                               ORDER BY "Active" DESC, "EffectiveTime" DESC, "Id" DESC)=1)
SELECT a.source_entity,
       a.target_system,
       coalesce(f.semantic_tag, 'no matching fully specified name') AS semantic_tag,
       count(*) AS record_count,
       count(DISTINCT a.mapped_concept_code) AS code_count
FROM additions a
LEFT JOIN fsn f ON try_to_number(a.mapped_concept_code)=f."ConceptId"
GROUP BY a.source_entity,
         a.target_system,
         coalesce(f.semantic_tag, 'no matching fully specified name')
ORDER BY a.source_entity,
         record_count DESC;


SELECT 'allergy_intolerance' AS source_entity,
       date_precision_source_code,
       date_precision_source_display,
       count(*) AS record_count
FROM OLIDS_ENGINEERING.CONFORMED.ALLERGY_INTOLERANCE
GROUP BY date_precision_source_code,
         date_precision_source_display
UNION ALL
SELECT 'referral_request',
       date_precision_source_code,
       date_precision_source_display,
       count(*)
FROM OLIDS_ENGINEERING.CONFORMED.REFERRAL_REQUEST
GROUP BY date_precision_source_code,
         date_precision_source_display;