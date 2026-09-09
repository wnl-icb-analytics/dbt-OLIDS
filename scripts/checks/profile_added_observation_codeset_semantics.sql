-- Aggregate only. Distinct public reference memberships prevent dictionary fanout.
WITH additions AS
  (SELECT 'allergy_intolerance' AS source_entity,
          mapped_concept_code,
          count(*) AS record_count
   FROM OLIDS_ENGINEERING.CONFORMED.ALLERGY_INTOLERANCE
   WHERE coalesce(lds_is_deleted, FALSE)=FALSE
     AND person_id IS NOT NULL
   GROUP BY mapped_concept_code
   UNION ALL SELECT 'referral_request',
                    mapped_concept_code,
                    count(*)
   FROM OLIDS_ENGINEERING.CONFORMED.REFERRAL_REQUEST
   WHERE coalesce(lds_is_deleted, FALSE)=FALSE
     AND person_id IS NOT NULL
   GROUP BY mapped_concept_code),
     memberships AS
  (SELECT DISTINCT SOURCE,
                   cluster_id,
                   code
   FROM STAGING.REFERENCE.STG_REFERENCE_COMBINED_CODESETS
   WHERE cluster_id IN ('DXT_CHEMO_COD',
                        'AST_COD',
                        'DIAB_COD',
                        'FOOTEXAM_COD',
                        'ASTADM_COD')),
     fsn AS
  (SELECT "ConceptId",
          regexp_substr("Term", '[(]([^()]*)[)]$', 1, 1, 'e', 1) AS semantic_tag
   FROM "Dictionary"."NHSD_SnomedReportingModel"."SCT_Description"
   WHERE "TypeId"=900000000000003001 QUALIFY row_number() over(PARTITION BY "ConceptId"
                                                               ORDER BY "Active" DESC, "EffectiveTime" DESC, "Id" DESC)=1)
SELECT a.source_entity,
       m.source AS code_set_source,
       m.cluster_id,
       coalesce(f.semantic_tag, 'no matching fully specified name') AS semantic_tag,
       sum(a.record_count) AS matching_records,
       count(DISTINCT a.mapped_concept_code) AS code_count
FROM additions a
JOIN memberships m ON a.mapped_concept_code=m.code
LEFT JOIN fsn f ON try_to_number(a.mapped_concept_code)=f."ConceptId"
GROUP BY a.source_entity,
         m.source,
         m.cluster_id,
         coalesce(f.semantic_tag, 'no matching fully specified name')
ORDER BY a.source_entity,
         m.cluster_id,
         m.source,
         matching_records DESC;-- The labels below were first read from public code-list/terminology tables only.
with fsn as (
select "ConceptId","Term" from "Dictionary"."NHSD_SnomedReportingModel"."SCT_Description" where "TypeId"=900000000000003001 qualify row_number() over(partition by "ConceptId" order by "Active" desc,"EffectiveTime" desc,"Id" desc)=1
), memberships as (
select distinct source,cluster_id,code from STAGING.REFERENCE.STG_REFERENCE_COMBINED_CODESETS where (source='PCD' and cluster_id='FOOTEXAM_COD') or (source='UKHSA_COVID' and cluster_id='ASTADM_COD')
)
select m.source,m.cluster_id,count(*) as matching_records,count_if(f."Term"='Refer to diabetic foot screener (procedure)') as explicit_foot_screener_referral_records,count_if(f."Term"='Emergency hospital admission for asthma (procedure)') as emergency_asthma_admission_coded_records
from OLIDS_ENGINEERING.CONFORMED.REFERRAL_REQUEST r join memberships m on r.mapped_concept_code=m.code left join fsn f on try_to_number(r.mapped_concept_code)=f."ConceptId"
where coalesce(r.lds_is_deleted,false)=false and r.person_id is not null group by m.source,m.cluster_id;
