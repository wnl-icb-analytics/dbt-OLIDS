# OLIDS longitudinal outputs

## Medication orders in clinical history

Clinical records contain expanded observations and medication orders. Standalone
medication statements remain in their detail table but do not add clinical rows
or appointment-clinical links. Existing observation and order IDs are unchanged.
Person and clinical-date clustering is unchanged.

Orders retain their own dates, codes, medication names, doses, quantities and
durations. The clinical output adds these prescribing fields and the source
code and label for authorisation type from the current linked statement.
The statement must be non-deleted and belong to the same person. Missing or
mismatched statements leave the order in place with null authorisation fields.
Current statement context is not a historical authorisation status and does not
overwrite order details. The empty quantity-description field is omitted.
Supplied duration and quantity can be zero or negative; they are retained, not
interpreted as validated treatment durations or administered quantities.

The 10 September profile has 384,607,642 orders. Of these, 384,602,541 have a
same-person statement with a labelled authorisation type; 118 have no available
statement and 4,983 point to a different person. There are 3,383,873 statements
with no matching order for the same person. They remain available separately.
The earlier profiles below include standalone statements and describe the
previous population.

Conformed models define shared OLIDS data. Stable models store the event stream
and clinical history once, on the existing OLIDS warehouse. Analytics exposes
thin views and supplies the later cross-source event stream. No `fct_` model is
added to dbt-OLIDS.

| Output | One row represents | Storage |
|---|---|---|
| `appointment_booking` | Current recorded booking for an appointment with a booking timestamp | View over the prepared appointment snapshot |
| `referral_request` | Terminology-defined referral, including qualifying observations | Existing stable table, selected in conformed |
| `healthcare_event` | Current booking, appointment slot or coded patient referral milestone | Stable table clustered by person and event date |
| `clinical_record` | Expanded observation or medication order | Stable table clustered by person and clinical date |
| `appointment_clinical_record` | Recorded encounter path between an appointment and clinical record for the same person | Stable table clustered by appointment |

A slot is not proof of attendance. The feed cannot reconstruct all rebookings,
cancellations or status transitions after source updates. Patient-left times
are appointment detail, not discharge events. General encounters, test requests
and procedure requests do not become healthcare milestones.

Clinical records remain separate from the event stream. Expanded observations
already include allergy and referral-request content, so neither source is added
again. Medication orders supply the prescribing rows; statements provide authorisation
context and remain separately available in their detail table. Prescribing quantity is not an observation result. The current source
has no populated result text or mapped result-unit fields, so those columns are
omitted. Supplied numeric values, result dates and source unit codes and labels
remain available. The existing code-clustered detail tables are unchanged.

## Referral definition

The agreed definition is `<<3457005 |Patient referral (procedure)|`, applied to
codes across expanded observations. Source entity alone does not classify a
referral. Source and mapped codes must identify SNOMED before matching; numeric
local codes must not accidentally match a SNOMED identifier. A record matching
both branches produces one event, with preference for the matching source code.

`REFERENCE.TERMINOLOGY.PATIENT_REFERRAL_SNOMED_CODES` uses the UKHFD current
transitive closure and the NHS SNOMED reporting model's preferred labels.
Inactive codes qualify through one explicitly non-ambiguous active successor
in the referral set. Each retains its own latest preferred label. This is
succession-based inclusion, not a reconstruction of an old ECL release.
Ambiguous-only historical candidates remain unclassified.

The new `reference_terminology` schema mapping affects only this model.
`dbt_project.yml` and existing schema mappings are unchanged. The normal full
build includes the reference through its dependency on the event model.
Neither builds nor analyst queries call a terminology server.

## Corrected referral table

`conformed_referral_request_source` retains the original deduplicated source
content. Expanded observations read this model, preserving non-referral codes.
`conformed_referral_request` then classifies expanded observations with the
shared terminology reference. Stable remains a pass-through of that definition;
the event stream reads the same canonical referrals. This dependency order avoids
a cycle and keeps selection in conformed.

The 21,763,458 qualifying original referral rows retain their IDs and every
existing field unchanged. The 361,842 added observations have deterministic
namespaced IDs, with no collisions against original referrals. `observation_id`
links every referral to the same expanded observation. Every original referral
field compared equal in the aggregate validation. Added observations have no
inferred UBRN, requester, recipient, direction, priority, specialty or mode.
The original `value` field remains source-specific; observation results remain
in observations and clinical records. `referral_snomed_code/name` explain
eligibility without replacing the existing source or mapped code fields.

Medication records preserve their supplied referral-request IDs. In this
snapshot, 35,343 medication-order links and 8,714 medication-statement links
point to source records excluded as non-referrals. Their clinical content is
still available through observation provenance. No existing analytics model
joins these IDs to referrals. The stable freshness row now describes the
canonical referral population; landing audit counts still describe the source.

Valproate already reads observations and referrals twice, tracked separately in
analytics issue #1126. All 519 qualifying ARAF referral rows for 284 people remain
original referrals; this change adds no further duplicate ARAF rows in the
profiled snapshot. Future native observation referrals can expose that existing
programme defect. No programme code is changed here.

## Identity and time

`person_id` is consistent across practice registrations; `patient_id` is
practice-specific. Event and clinical IDs include a record-type namespace to
avoid collisions between source entities. `source_record_id` links to the named
detail table. Expanded observation IDs remain unchanged.

The event and clinical outputs obtain `sk_patient_id` from the conformed patient
only when both patient and person match, retaining rows without an approved
hash. The appointment facts in analytics retain their existing representative
`dim_person_pseudo` mapping. These refresh independently and can differ. Use
`person_id` for OLIDS person history and recorded links.

Referral dates remain date values with day, month, year or unknown precision.
`event_at` is null for referrals; no midnight timestamp is invented. Missing,
historical and future dates remain available. Record-entry time is not a
substitute for clinical effective time. Appointment milestones use their actual
supplied booking or scheduled timestamp.

## Validation on 9 September 2026

Validation returned aggregates only. Before PR #298's scheduled publication,
read-only candidates reproduced its observation expansion from the existing
conformed allergy and referral tables with the exact merged ID and field rules.
Production models read the single expanded observation table.

| Event type | Rows | Distinct event IDs |
|---|---:|---:|
| Booking | 78,207,418 | 78,207,418 |
| Appointment slot | 78,821,602 | 78,821,602 |
| Coded referral | 22,125,300 | 22,125,300 |

The 179,154,320 event rows retain unique keys within each event type; the ID
namespace separates types. Every referral has a code and label. Of the
23,634,902 referral-request records, 21,763,458 qualify and 1,871,444 do not.
Another 361,842 qualifying referrals occur in native observations; none occur
in allergies. There are 2,879 missing referral dates and 9,258 referrals with
partial or unknown source precision. Missing dates are included in that latter
count. Referral-request content has no provider ID, so its provider fields
remain null; publisher details are complete and do not imply a destination.

The UKHFD set exactly matched the terminology-service expansion: 1,487 codes,
zero differences, import 2 September 2026. Another 1,144 historical codes have
one eligible successor; 80 ambiguous-only candidates are excluded. All 2,631
codes have exactly one preferred label. Current clinical mappings already use
active successors, so no candidate event required the historical-code branch.

| Clinical record type | Rows | Missing approved hash | Missing clinical date |
|---|---:|---:|---:|
| Expanded observation | 1,588,949,549 | 128,441 | 988,183 |
| Medication order | 384,524,265 | 37,944 | 0 |
| Medication statement | 98,440,569 | 9,078 | 0 |

The 2,071,914,383 rows reconcile to their input populations without join
multiplication. Every row has person and patient IDs, source code and label,
source precision code and label, record-entry time, publisher details and
extraction time. Mapped codes and labels are complete for observations; they
are missing together on 2,943 orders and 2,131 statements. Numeric observation
results exist on 499,087,422 rows; source unit code-label pairs on 483,516,055.
Optional result fields are not expected on medication rows. All retained output
columns are populated somewhere. The conformed patient lookup has 2,874,868
unique, non-null IDs.

Synthetic tests of compiled SQL cover source/mapped SNOMED membership, numeric
codes from other systems, dual matches, historical labels, partial and missing
dates, deleted records and unbooked slots. All seven expected event rows match.
The separate relationship fixtures cover reassignment, deletion and different
practice registrations, with seven initial and five later rows and no failures.

## Release checks

Both PRs remain drafts until the stable outputs are built and published and the
analytics views are built in DEV. No immediate production build was triggered.
The full clinical table has over two billion rows: aggregate validation does
not establish table-write, clustering or daily rebuild performance. Measure
those costs on the scheduled Large warehouse before treating the design as
ready to merge. Analytics currently retains the repository-required grain tests
at staging and reporting, including repeated full uniqueness checks. Measure
that exact daily selection on its configured warehouse too; view materialisation
alone does not make the analytics workload cheap. Existing source watermarks may skip an unchanged snapshot;
merge order alone does not guarantee publication.

Compile the changed models with the tracked `stable` profile. The scripts
`check_healthcare_event_fixtures.py` and `check_appointment_link_fixtures.py`
accept compiled conformed SQL and emit aggregate-only synthetic checks. The
event fixture also takes the compiled canonical referral SQL as its second
argument, testing source-ID retention and source-entity collision protection.
`profile_olids_output.py` accepts compiled SQL, its model YAML and `--group-by`
(`event_type` or `source_record_type`). Add `--distinct-key healthcare_event_id`
for the event grain check. Clinical grain tests run on the materialised output.
Repeat completeness and count reconciliation against published data, then
validate person-filtered history queries and the downstream DEV facts.
