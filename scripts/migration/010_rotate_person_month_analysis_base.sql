-- Run only at the #257 person id cutover.
-- Idempotent: only rows still matching old ids are updated.
UPDATE REPORTING.OLIDS_PERSON_ANALYTICS.person_month_analysis_base AS t
SET person_id = x.new_person_id
FROM OLIDS_ENGINEERING.PSEUDONYMISATION.person_id_crosswalk AS x
WHERE t.person_id = x.old_person_id;
