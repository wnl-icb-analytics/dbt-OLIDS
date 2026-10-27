# GP2GP copy links

`stable_gp2gp_copy_link` publishes `STABLE.GP2GP_COPY_LINK`. One row represents
one inferred copy of an observation, medication statement or medication order,
keyed by `(evidence_kind, copy_record_id)`. The population inherits the stable
tables' filtered NCL EMIS patient spine.

The model reads the existing stable flags. It retains non-deleted copies with
an original in the same evidence table for the same person. Null copy deletion
flags count as non-deleted. Original existence does not apply the consumer's
deletion or copy filters; consumers join their retained original records.

`gp2gp_original_id` identifies the inferred original. `patient_id`, publisher
organisation fields, `clinical_effective_date`, `date_recorded` and
`lds_is_deleted` all describe the copy. Several copies can link one original
to the same holding patient record. Downstream visibility checks use existence
or distinct holding keys to avoid multiplying clinical evidence.

## Inference and time

The existing GP2GP heuristic matches person, mapped concept, clinical date and
value across different patient records, with strictly later recorded chronology.
Observation source entities remain separate. Medication matching uses quantity;
observation matching uses result value. Copy timing must satisfy the heuristic's
recording-lag or registration rule. `macros/gp2gp_flags.sql` defines these rules.

Links are inferred provenance, not authoritative transfer identifiers. Tied or
missing chronology and unmatched transfers can leave content without a link.
An absent link does not prove that clinical content is absent. Generic GP2GP
imports without an inferred original do not create links.

This is a full snapshot with no as-at cutoff or current-registration restriction.
A refresh removes deleted or missing copies and updates changed original links.
The table clusters by `(person_id, gp2gp_original_id)` for downstream original
lookups. Queries also match `evidence_kind` because IDs belong to separate tables.

## Publication and dbt-analytics

The stable folder supplies materialisation, metadata hooks and the `stable` tag.
The normal OLIDS schedule includes this model and then runs
`scripts/checks/deploy_data_lake_views.py`. That procedure publishes
`OLIDS_ENGINEERING.STABLE` to `DATA_LAKE.OLIDS`, so the production view is
`DATA_LAKE.OLIDS.GP2GP_COPY_LINK`. A development build alone does not publish it.

The existing `olids` entry in dbt-analytics's
`scripts/sources/source_mappings.yml` already covers `DATA_LAKE.OLIDS`. It does
not need a second entry for this table. After publication, source generation
discovers `GP2GP_COPY_LINK` and creates `raw_olids_gp2gp_copy_link`. The generator
maps the published column `GP2GP_ORIGINAL_ID` to `gp2_gp_original_id`.

The analytics follow-up needs one staging projection,
`stg_olids_gp2gp_copy_link`, with documented grain and columns. Modelling helpers
read that staging model through `ref()`. Shared observation and medication
staging keep their original-wins filters.

Practice-record visibility matches the retained original on evidence kind,
person and original ID, then matches the copy's `patient_id` to the selected
registration's patient record. Publisher code alone does not establish that
record. As-at queries apply their cutoff to both copy dates, with an explicit
null-recorded-date policy, before ranking or counting evidence. Evidence can
predate registration. Clinical or programme scope remains a downstream decision.

## Validation

The model has grain, required-key and evidence-kind tests. The relationship test
checks original existence for the same person within each evidence kind and
returns aggregate failure counts only. Test failures are not stored.

Development validation selects `stable_gp2gp_copy_link` and its tests, using an
existing development profile and database. Aggregate checks compare link counts
by evidence kind with non-deleted flagged-copy counts and report the share with
a same-person original. No patient-level output or measured counts belong in
this document.
