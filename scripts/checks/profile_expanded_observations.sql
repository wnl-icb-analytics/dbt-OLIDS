-- PRE-EXPANSION BASELINE ONLY: OBSERVATION is the native-only table here.
-- After release, restrict every OBSERVATION reference to source_entity = 'observation'
-- before rerunning these comparisons. Otherwise added records match themselves.
-- Read-only, aggregate-only diagnostics. Run on the existing OLIDS Large warehouse.

SELECT 'observation' AS entity,
       count(*) AS ROW_COUNT,
       count(DISTINCT id) AS ids,
       count_if(id IS NULL) AS null_ids,
       count_if(lds_is_deleted) AS deleted,
       count_if(person_id IS NULL) AS null_person,
       min(clinical_effective_date) AS min_date,
       max(clinical_effective_date) AS max_date
FROM OLIDS_ENGINEERING.CONFORMED.OBSERVATION
UNION ALL
SELECT 'allergy_intolerance',
       count(*),
       count(DISTINCT id),
       count_if(id IS NULL),
       count_if(lds_is_deleted),
       count_if(person_id IS NULL),
       min(clinical_effective_date),
       max(clinical_effective_date)
FROM OLIDS_ENGINEERING.CONFORMED.ALLERGY_INTOLERANCE
UNION ALL
SELECT 'referral_request',
       count(*),
       count(DISTINCT id),
       count_if(id IS NULL),
       count_if(lds_is_deleted),
       count_if(person_id IS NULL),
       min(clinical_effective_date),
       max(clinical_effective_date)
FROM OLIDS_ENGINEERING.CONFORMED.REFERRAL_REQUEST;


SELECT 'allergy_intolerance' AS entity,
       count(*) AS ROW_COUNT,
       count_if(allergy_intolerance_source_concept_id IS NULL) AS no_concept,
       count_if(mapped_concept_code IS NULL) AS no_mapped_code,
       count_if(clinical_status IS NOT NULL) AS clinical_status_rows,
       count_if(verification_status IS NOT NULL) AS verification_rows,
       count_if(category IS NOT NULL) AS category_rows,
       count_if(medication_name IS NOT NULL) AS medication_rows,
       count_if(multi_lex_action IS NOT NULL) AS action_rows
FROM OLIDS_ENGINEERING.CONFORMED.ALLERGY_INTOLERANCE;


SELECT count(*) AS ROW_COUNT,
       count_if(referral_request_source_concept_id IS NULL) AS no_concept,
       count_if(mapped_concept_code IS NULL) AS no_mapped_code,
       count_if(value IS NOT NULL) AS value_rows,
       count_if(MODE IS NOT NULL) AS mode_rows,
       count_if(is_outgoing_referral IS NOT NULL) AS outgoing_rows,
       count_if(referral_request_priority_source_code IS NOT NULL) AS priority_rows,
       count_if(referral_request_type_source_code IS NOT NULL) AS type_rows,
       count_if(referral_request_specialty_source_code IS NOT NULL) AS specialty_rows
FROM OLIDS_ENGINEERING.CONFORMED.REFERRAL_REQUEST;

WITH extras AS
  (SELECT 'allergy_intolerance' AS entity,
          id,
          lds_source_record_id,
          patient_id,
          clinical_effective_date,
          mapped_concept_code
   FROM OLIDS_ENGINEERING.CONFORMED.ALLERGY_INTOLERANCE
   UNION ALL SELECT 'referral_request',
                    id,
                    lds_source_record_id,
                    patient_id,
                    clinical_effective_date,
                    mapped_concept_code
   FROM OLIDS_ENGINEERING.CONFORMED.REFERRAL_REQUEST)
SELECT e.entity,
       count(*) AS same_id_rows,
       count_if(e.lds_source_record_id=o.lds_source_record_id) AS same_source_record_rows,
       count_if(e.patient_id=o.patient_id) AS same_patient_rows,
       count_if(e.mapped_concept_code=o.mapped_concept_code) AS same_mapped_code_rows
FROM extras e
INNER JOIN OLIDS_ENGINEERING.CONFORMED.OBSERVATION o ON e.id=o.id
GROUP BY e.entity;


SELECT count(*) AS allergy_referral_id_collisions
FROM OLIDS_ENGINEERING.CONFORMED.ALLERGY_INTOLERANCE a
INNER JOIN OLIDS_ENGINEERING.CONFORMED.REFERRAL_REQUEST r ON a.id=r.id;

WITH extras AS
  (SELECT 'allergy_intolerance' AS entity,
          id,
          lds_source_record_id,
          patient_id,
          clinical_effective_date,
          mapped_concept_code
   FROM OLIDS_ENGINEERING.CONFORMED.ALLERGY_INTOLERANCE
   UNION ALL SELECT 'referral_request',
                    id,
                    lds_source_record_id,
                    patient_id,
                    clinical_effective_date,
                    mapped_concept_code
   FROM OLIDS_ENGINEERING.CONFORMED.REFERRAL_REQUEST)
SELECT e.entity,
       count(*) AS matched_pairs,
       count(DISTINCT e.id) AS added_records_with_same_source_record,
       count_if(e.id=o.id) AS same_id_pairs,
       count_if(e.patient_id=o.patient_id
                AND e.clinical_effective_date=o.clinical_effective_date
                AND e.mapped_concept_code=o.mapped_concept_code) AS same_patient_date_code_pairs
FROM extras e
JOIN OLIDS_ENGINEERING.CONFORMED.OBSERVATION o ON e.lds_source_record_id=o.lds_source_record_id
GROUP BY e.entity;

WITH extras AS
  (SELECT 'allergy_intolerance' AS entity,
          id,
          patient_id,
          clinical_effective_date,
          mapped_concept_code
   FROM OLIDS_ENGINEERING.CONFORMED.ALLERGY_INTOLERANCE
   UNION ALL SELECT 'referral_request',
                    id,
                    patient_id,
                    clinical_effective_date,
                    mapped_concept_code
   FROM OLIDS_ENGINEERING.CONFORMED.REFERRAL_REQUEST)
SELECT e.entity,
       count(*) AS matching_pairs,
       count(DISTINCT e.id) AS added_records_sharing_patient_date_code
FROM extras e
JOIN OLIDS_ENGINEERING.CONFORMED.OBSERVATION o ON e.patient_id=o.patient_id
AND e.clinical_effective_date=o.clinical_effective_date
AND e.mapped_concept_code=o.mapped_concept_code
GROUP BY e.entity;


SELECT count(*) AS ROW_COUNT,
       count_if(date_recorded IS NOT NULL) AS recorded_date_rows,
       count_if(is_review IS NOT NULL) AS review_rows,
       count_if(is_confidential IS NOT NULL) AS confidentiality_rows,
       count_if(clinical_effective_date>source_extraction_date::date) AS future_source_date_rows
FROM OLIDS_ENGINEERING.CONFORMED.ALLERGY_INTOLERANCE;


SELECT count(*) AS ROW_COUNT,
       count_if(recorded_datetime IS NOT NULL) AS recorded_date_rows,
       count_if(is_review IS NOT NULL) AS review_rows,
       count_if(clinical_effective_date>source_extraction_date::date) AS future_source_date_rows
FROM OLIDS_ENGINEERING.CONFORMED.REFERRAL_REQUEST;


SELECT system$typeof(uuid_string('6ba7b811-9dad-11d1-80b4-00c04fd430c8', 'olids:allergy_intolerance:synthetic')::UUID) AS generated_id_type;


SELECT count(*) AS matched_pairs,
       count_if(equal_null(r.publisher_organisation_id, o.publisher_organisation_id)) AS same_publisher,
       count_if(equal_null(r.publisher_organisation_code, o.publisher_organisation_code)) AS same_publisher_code,
       count_if(equal_null(r.patient_id, o.patient_id)
                AND equal_null(r.person_id, o.person_id)) AS same_patient_person,
       count_if(equal_null(r.referral_request_source_concept_id, o.observation_source_concept_id)) AS same_native_concept,
       count_if(equal_null(r.clinical_effective_date, o.clinical_effective_date)
                AND equal_null(r.clinical_effective_date_precision_source_concept_id, o.clinical_effective_date_precision_source_concept_id)) AS same_date_precision,
       count_if(equal_null(r.encounter_id, o.encounter_id)
                AND equal_null(r.practitioner_id, o.practitioner_id)
                AND equal_null(r.author_organisation_id, o.author_organisation_id)) AS same_encounter_practitioner_author,
       count_if(equal_null(r.recorded_datetime, o.date_recorded)) AS same_recorded_datetime,
       count_if(equal_null(r.is_review, o.is_review)
                AND equal_null(r.lds_is_deleted, o.lds_is_deleted)) AS same_review_deleted,
       count_if(o.result_value IS NULL
                AND o.result_text IS NULL
                AND o.result_date IS NULL
                AND o.result_value_units_source_concept_id IS NULL) AS native_no_result,
       count_if(equal_null(r.source_extraction_date, o.source_extraction_date)
                AND equal_null(r.lds_transform_datetime, o.lds_transform_datetime)) AS same_extraction_transform
FROM OLIDS_ENGINEERING.CONFORMED.REFERRAL_REQUEST r
JOIN OLIDS_ENGINEERING.CONFORMED.OBSERVATION o ON r.id=o.id
AND r.lds_source_record_id=o.lds_source_record_id;


SELECT 'allergy_intolerance' AS entity,
       count(*) AS ROW_COUNT,
       count(DISTINCT lds_source_record_id) AS source_record_ids,
       count_if(lds_source_record_id IS NULL) AS null_source_record_ids
FROM OLIDS_ENGINEERING.CONFORMED.ALLERGY_INTOLERANCE
UNION ALL
SELECT 'referral_request',
       count(*),
       count(DISTINCT lds_source_record_id),
       count_if(lds_source_record_id IS NULL)
FROM OLIDS_ENGINEERING.CONFORMED.REFERRAL_REQUEST;
