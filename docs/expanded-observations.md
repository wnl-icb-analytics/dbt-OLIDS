# Expanded observations

The observations contract is recorded clinical codes with their dates and context.
Source entity records delivery provenance; it does not override the coded meaning.
Recovering coded records held in allergy and referral tables fits that contract.
The [complete column and clinical-meaning review](expanded-observation-completeness.md)
records coverage and consumer effects. Coordinated deployment and warehouse
validation remain outstanding.

`CONFORMED.OBSERVATION` and its existing `STABLE.OBSERVATION` publication combine
native observations, recorded allergies and referral requests. One row represents
one source-entity record. A count of these rows is not a count of distinct clinical
actions. Diagnostic orders, procedure requests and medications remain separate.

The expansion reuses conformed allergy and referral models, including their patient
and practice filtering and referral revision selection. Native observation fields,
IDs and selection remain unchanged. The model remains a full snapshot replacement.
No incremental processing or new database/schema is introduced.

## Fields and identity

| Field | Meaning |
| --- | --- |
| `id` | Existing UUID for native observations. Added records use a deterministic UUID for their entity and original ID. |
| `source_entity` | `observation`, `allergy_intolerance` or `referral_request`. |
| `source_record_id` | Original entity UUID. Join this to the corresponding entity table's `id`. |
| `lds_source_record_id` | Unchanged source delivery identifier, retained independently of the entity UUID. |
| `observation_source_concept_id` | The source entity's own observation, allergy or referral concept. Source and mapped code fields retain the conformed mapping. |
| `clinical_effective_date` and precision fields | Supplied source clinical date and precision, without substitution or correction. |
| `date_recorded` | Supplied `date_recorded`, or referral `recorded_datetime`. |
| `allergy_medication_name` | Supplied medication name for an allergy record. Null for other entities. |

Added IDs use Snowflake `UUID_STRING` with the fixed UUID namespace
`6ba7b811-9dad-11d1-80b4-00c04fd430c8` and name
`olids:<source_entity>:<source_record_id>`, cast to UUID. Existing observation IDs
and their parent links are unchanged. The same original ID in different entities
therefore produces different observation IDs.

Added records do not supply measurement values, result text, result dates or units.
Their result fields remain null. No problem or problem-deletion status is inferred.
Allergy review and confidentiality flags pass through unchanged. Referrals have
no source confidentiality field, so that field is null. Other entity details remain
available through `source_entity` and `source_record_id`; the observation table does
not repeat referral administration fields.

The current feed does not populate allergy clinical status, verification status or
category. These records are recorded allergy evidence, not a confirmed active
allergy list. Referral value and specialty are also entirely null. These fields
have not been added as empty observation columns.

## Source profile

Aggregate checks against `OLIDS_ENGINEERING.CONFORMED` on 9 September 2026 found:

| Entity | Rows | Distinct IDs | Missing source concepts | Missing mapped codes |
| --- | ---: | ---: | ---: | ---: |
| Native observations | 1,563,092,325 | 1,563,092,325 | Excluded by the existing source rule | Existing mapping gate unchanged |
| Allergy intolerance | 2,222,322 | 2,222,322 | 0 | 0 |
| Referral requests | 23,634,902 | 23,634,902 | 0 | 0 |

All three populations have non-null IDs and person IDs and no source-deleted rows
in this snapshot. Both added entities also have unique, non-null
`lds_source_record_id` values. Their recorded dates and review flags are populated
throughout. Allergy medication names and confidentiality flags are populated
throughout. Five allergies and 15 referrals have a clinical date after their own
extraction date. Those source dates remain unchanged.

### Overlap

One referral shares both its entity ID and delivery record ID with a native
observation. Publisher, patient, person, native concept, clinical date and precision,
recorded date and review/deletion flags agree. Encounter/practitioner/author context
does not agree in full. The native observation also has result content, and the
extraction/transform timestamps differ. Neither representation is discarded:
shared identity alone does not establish which content should take precedence.

No allergy shares a delivery record ID with a native observation. No allergy and
referral entity IDs collide. Matching patient, date and mapped code is broader:
9,157 referral records have 9,745 matching observation pairs; 20 allergy records
have 23 pairs. These matches are not used for deduplication.

The aggregate queries are retained in
[`profile_expanded_observations.sql`](../scripts/checks/profile_expanded_observations.sql).
They return no patient-level records or clinical value examples.
The script captures the pre-expansion baseline. After release, its observation
references must select `source_entity = 'observation'` before reuse.

## Consumer consequences

`DATA_LAKE.OLIDS.OBSERVATION` is already a shared analytical input. Existing
dbt-analytics staging removes deleted records and null person IDs, so added records
will enter its code-list and condition models. The existing raw and staging models
explicitly project columns; a companion change must expose source provenance before
publication. Consumers must distinguish records from completed clinical actions.

Valproate referral processing already combines observations and referral requests;
its companion change must prevent a second union of the same referrals. The CLTCS
latest-100 observation selection can change as new records become eligible. Existing
measurement fields remain unchanged, and added records have no measurement values.

The current practice lookup includes active NCL practices only. A closed practice's
history can leave the next snapshot. This expansion preserves that existing
population rule. The legacy Synapse pipeline is unchanged.

## Validation and release

Targeted dbt compilation passed for the two changed models and their selected
tests. The existing stable ID grain tests remain in place. New checks cover source
entity values, original IDs, added-source record reconciliation and synthetic shared
IDs across entities. The native observation mapping-quality gate remains scoped to
native source observations.

Read-only execution of the compiled candidate returned 1,588,949,549 rows, including
all 25,857,224 added records. No output UUIDs collide across entities. The native
population remains 1,563,092,325 rows, and its aggregate hash across all 48 existing
columns exactly matches the existing conformed table. Its 499,087,422 populated
numeric result values are unchanged; added records supply none.

Aggregate-wrapped source reconciliation returned zero failures, including checks
of actual generated IDs and unchanged native IDs. Synthetic shared-ID checks also
returned zero failures. Zero-row result metadata confirms that all 48 existing
column types are unchanged. Explicit null casts retain the original VARCHAR widths.
The three new types are `VARCHAR(32)`, UUID and `VARCHAR(16777216)` respectively.

The full candidate preservation aggregate took 318.7 seconds elapsed, including
300.2 seconds execution, on `WH_WNL_OLIDS_L`. Operator statistics report 133.5 GB
scanned and 714.8 GB local spill, chiefly 679.3 GB in the existing native observation
window function. Remote spill was approximately 8.9 MB. This measures the candidate
query with aggregate validation, not a materialised build or the complete nightly
pipeline. The existing wide-row deduplication cost is tracked separately in
[issue 278](https://github.com/wnl-icb-analytics/dbt-OLIDS/issues/278).

Initial independent review checked identity, source mapping, nulls, types,
reconciliation and downstream provenance. Test and diagnostic-scope gaps were
corrected before the draft PR. The subsequent full completeness and code-list
review identified consumer effects and a separate foot-examination interpretation
defect. Source-table placement alone is not a reason to exclude a recorded code.

No warehouse model build or publication has run. This repository has only the
tracked `stable` target, which writes production `OLIDS_ENGINEERING` layers. A
targeted build and the downstream companion release need an approved deployment
step; an isolated Git worktree does not isolate those warehouse objects.
