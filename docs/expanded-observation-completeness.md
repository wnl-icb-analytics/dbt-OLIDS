# Added observation completeness and meaning

Structural compatibility is proven for the existing 48 observation columns:
unchanged native content and types, unchanged IDs and no expanded ID collisions.
The agreed observations contract is recorded clinical codes with their dates and
context. Source-table placement is provenance, not a separate clinical eligibility
rule. Adding codes held in allergy and referral tables restores coverage for
consumers that already interpret the same codes in native observations.

EMIS document-related routing is a suspected explanation supplied by the project
owner; this review has not independently verified that mechanism. The decision to
include recorded codes does not depend on proving that explanation. Counts and
measure populations can change as coverage improves. No clinical rules or warehouse
models were changed for this review.

## Coverage

The rendered added SELECT branches were profiled on 9 September 2026, without
scanning native observations. There are 2,222,322 allergy records and 23,634,902
referrals. All 51 output columns were checked for both non-null and non-blank values.
No populated value was blank, so the two counts agree throughout.

Both entities have complete patient/person IDs, generated and original IDs,
source-delivery identifiers, publisher/author organisation IDs, publisher codes,
practitioner IDs, native and mapped clinical codes and labels, code systems,
recorded dates and extraction/processing provenance. Population is the existing
filtered NCL conformed population.

Material gaps:

- Allergy clinical dates are present on 2,191,190 records; 31,132 are missing.
  Referral clinical dates are present on 23,631,418 records; 3,484 are missing.
  The source precision is `Unknown` on exactly those missing-date populations.
- Allergy encounter IDs are present on 1,796,254 records; 426,068 are missing.
  Referral encounter IDs are absent throughout the source.
- Allergy mapped concept UUIDs are missing on 66 records, although mapped code,
  display and system are complete. Referral mapped concept UUIDs are complete.
- Mapped date-precision code/display are absent on both entities. Supplied source
  precision code/display are complete, including the explicit `Unknown` category.
- Referrals do not supply provider organisation or confidentiality fields.
  Allergy provider organisation and confidentiality fields are complete.
- Supplied general ages are missing on 31,891 allergies and 4,206 referrals.
  Baby and neonatal ages apply to their respective age groups; their lower counts
  are not a general completeness failure.

Result values, result text/date/units, problem flags/end dates, parent observation
links, episodicity and primary status are deliberately null on added records.
Neither source supplies those observation meanings. Setting false, zero or a
substituted date would invent information. Allergy medication names are complete;
referrals correctly have no allergy medication name.

| Output column | Allergy non-null/non-blank | Referral non-null/non-blank |
| --- | ---: | ---: |
| `id` | 2,222,322 | 23,634,902 |
| `lds_source_record_id` | 2,222,322 | 23,634,902 |
| `patient_id` | 2,222,322 | 23,634,902 |
| `person_id` | 2,222,322 | 23,634,902 |
| `publisher_organisation_id` | 2,222,322 | 23,634,902 |
| `provider_organisation_id` | 2,222,322 | 0 |
| `author_organisation_id` | 2,222,322 | 23,634,902 |
| `encounter_id` | 1,796,254 | 0 |
| `practitioner_id` | 2,222,322 | 23,634,902 |
| `parent_observation_id` | 0 | 0 |
| `clinical_effective_date` | 2,191,190 | 23,631,418 |
| `clinical_effective_date_precision_source_concept_id` | 2,222,322 | 23,634,902 |
| `date_precision_source_code` | 2,222,322 | 23,634,902 |
| `date_precision_source_display` | 2,222,322 | 23,634,902 |
| `date_precision_code` | 0 | 0 |
| `date_precision_display` | 0 | 0 |
| `result_value` | 0 | 0 |
| `result_value_units_source_concept_id` | 0 | 0 |
| `result_unit_source_code` | 0 | 0 |
| `result_unit_source_display` | 0 | 0 |
| `result_unit_code` | 0 | 0 |
| `result_unit_display` | 0 | 0 |
| `result_date` | 0 | 0 |
| `result_text` | 0 | 0 |
| `is_problem` | 0 | 0 |
| `is_review` | 2,222,322 | 23,634,902 |
| `problem_end_date` | 0 | 0 |
| `observation_source_concept_id` | 2,222,322 | 23,634,902 |
| `source_code` | 2,222,322 | 23,634,902 |
| `source_display` | 2,222,322 | 23,634,902 |
| `source_system` | 2,222,322 | 23,634,902 |
| `mapped_concept_id` | 2,222,256 | 23,634,902 |
| `mapped_concept_code` | 2,222,322 | 23,634,902 |
| `mapped_concept_display` | 2,222,322 | 23,634,902 |
| `target_system` | 2,222,322 | 23,634,902 |
| `age_at_event` | 2,190,431 | 23,630,696 |
| `age_at_event_baby` | 65,483 | 240,560 |
| `age_at_event_neonate` | 6,727 | 33,203 |
| `episodicity_source_concept_id` | 0 | 0 |
| `is_primary` | 0 | 0 |
| `date_recorded` | 2,222,322 | 23,634,902 |
| `is_problem_deleted` | 0 | 0 |
| `is_confidential` | 2,222,322 | 0 |
| `lds_is_deleted` | 2,222,322 | 23,634,902 |
| `publisher_organisation_code` | 2,222,322 | 23,634,902 |
| `clinical_system` | 2,222,322 | 23,634,902 |
| `source_extraction_date` | 2,222,322 | 23,634,902 |
| `lds_transform_datetime` | 2,222,322 | 23,634,902 |
| `source_entity` | 2,222,322 | 23,634,902 |
| `source_record_id` | 2,222,322 | 23,634,902 |
| `allergy_medication_name` | 2,222,322 | 0 |

## Clinical meaning

All mapped codes identify SNOMED CT as their target system. Latest available fully
specified names from the Dictionary SNOMED reporting model provide these semantic
tags. Tags classify the code; they do not establish what occurred to a patient.
Missing fully specified names below do not mean missing displayed labels: the
conformed mapped labels remain complete.

| Semantic tag | Allergy records | Referral records |
| --- | ---: | ---: |
| Situation | 996,425 | 549 |
| Disorder | 683,400 | 0 |
| Finding | 488,010 | 1,007,200 |
| Procedure | 936 | 22,367,243 |
| Record artifact | 51,571 | 6 |
| Navigational concept | 41 | 0 |
| Special concept | 2 | 0 |
| Substance | 1 | 0 |
| Qualifier value | 0 | 16 |
| No matching fully specified name | 1,936 | 259,888 |

The added records match existing consumer code lists as follows:

| Added entity and code list | Matching records | SNOMED tag |
| --- | ---: | --- |
| Allergy, PCD `AST_COD` | 5,983 | Disorder |
| Allergy, each UKHSA COVID/flu `AST_COD` | 5,986 | Disorder |
| Allergy, each UKHSA COVID/flu `DIAB_COD` | 1,328 | Disorder |
| Allergy, each UKHSA COVID/flu `DXT_CHEMO_COD` | 1,764 | Disorder |
| Referral, PCD `FOOTEXAM_COD` | 10,466 | Procedure |
| Referral, each UKHSA COVID/flu `ASTADM_COD` | 7,639 | Procedure |

These are record/code-list overlaps, not numbers of newly classified people.
The allergy matches above are disorder codes, not products or substances.
Consumers apply their existing code and date definitions to those records;
their placement in an allergy table does not invalidate the coded evidence.
Missing allergy status still limits uses that specifically need a confirmed active
allergy list. The combined table supplies recorded evidence, not an independently
verified account of care.

Public reference terms make the referral examples more specific. All 10,466 foot
examination code-list matches use "Refer to diabetic foot screener (procedure)".
That is referral evidence, not proof that screening occurred. The existing foot
consumer incorrectly derives checked flags from this code regardless of its source
entity; that defect is separate from including the record. All 7,639 asthma
admission matches use "Emergency hospital admission for asthma (procedure)" in the
referral source. Their table placement does not turn an admission code into a
planned referral. The terms were first retrieved from public
reference tables without clinical joins; aggregate assertions then confirmed the
matches without returning source clinical codes or records.

Existing code-list consumers keep their interpretation rules and gain recorded
evidence. No blanket native-observation filter is required. Provenance remains
available for tracing records and for consumers with a specific source requirement.
The Valproate companion removes its duplicate referral input. The foot-check
interpretation defect needs its own correction; it is not evidence that the
combined observations contract is invalid. It is tracked in
[dbt-analytics issue 1125](https://github.com/wnl-icb-analytics/dbt-analytics/issues/1125).

## Reproducible checks

- [Column completeness](../scripts/checks/profile_added_observation_completeness.sql)
  uses the actual rendered added projections, with no native observation scan.
- [Semantic tags and date precision](../scripts/checks/profile_added_observation_semantics.sql)
  joins only public terminology metadata and emits aggregates.
- [Consumer code-list semantics](../scripts/checks/profile_added_observation_codeset_semantics.sql)
  deduplicates public code-list memberships before counting added records.

All queries are read-only and return aggregate counts, public code-list names or
public reference categories. No patient records, source clinical tokens or clinical
value examples are included. [PR 298](https://github.com/wnl-icb-analytics/dbt-OLIDS/pull/298)
and its [analytics companion](https://github.com/wnl-icb-analytics/dbt-analytics/pull/1123)
remain drafts pending coordinated publication and warehouse validation.
