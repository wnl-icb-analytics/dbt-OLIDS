-- Medication models join bnf_latest without a dedupe guard since the
-- concept-map fanout QUALIFYs were removed; duplication here would fan
-- out medication rows silently.
SELECT
    snomed_code,
    COUNT(*) AS n
FROM data_lab_olids_ncl.reference.bnf_latest
GROUP BY snomed_code
HAVING COUNT(*) > 1
