# Recorded appointment links

`appointment_clinical_record` contains one appointment, clinical record type and
clinical record ID with a recorded encounter path. The conformed view defines
the joins; the stable table materialises their result on the existing OLIDS
Large warehouse. The normal deployment publishes it to `DATA_LAKE.OLIDS`.

The relationship uses `clinical.encounter_id -> encounter.id` and
`encounter.appointment_id -> appointment.id`. All three records must have the
same `person_id` and must not be deleted. A practice registration has its own
`patient_id`, so matching patient IDs is not required. The relation carries the
appointment's patient ID. It does not infer links from dates or establish
attendance.

The population inherits the filtered NCL patient spine and the existing
appointment table's requirement for a patient and scheduled start. It retains
patient-associated blocked slots and all current appointment statuses.
Observations already include allergies and referrals after PR #298. The model
reads that expanded table once; medication orders and statements have separate
record types. `clinical_record_id` is the namespaced key shared with the new
`clinical_record` output. `source_record_id` is the detail table's `id`, including
the minted observation IDs for added allergies and referrals. Clinical codes,
labels, dates and results are available in the clinical output and detail tables.

## Snapshot processing

The stable table uses the established full snapshot build. A new snapshot
replaces changed links and removes deleted or absent records. No timestamp
watermark is treated as a clinical change feed. This replaces issue #1016's
earlier proposed analytics incremental build: the joins now belong in
dbt-OLIDS, which already rebuilds the source snapshots on Large compute.

The conformed relationship is a view and its stable table clusters by
appointment ID for drill-down. The separate person-clustered `clinical_record`
and `healthcare_event` outputs are described in [OLIDS longitudinal outputs](olids-longitudinal-outputs.md).

The OLIDS schedule starts at 02:30 UTC and analytics at 04:00 UTC. There is no
cross-project completion dependency, and unchanged source watermarks can skip
the OLIDS build. Publish the stable object before merging its analytics
companion. A successful build and publication are required to verify the final
runtime and downstream integration.

## Validation on 9 September 2026

Both relationship models and their four tests compile. Independent architecture review found
no SQL or grain blocker. Synthetic checks execute the compiled model against two
snapshots. They cover different people, missing paths, deletion flags, slot
reassignment, a moved link, a record removed without a tombstone, null deletion
flags and identical IDs across distinct medication record types. The expected
seven initial and five later rows match exactly, with zero differences.

Read-only profiling used the current conformed inputs. Since PR #298 had not
yet published its expanded observation snapshot, the profile added the
already-conformed allergy and referral inputs with the exact UUID rule from
that PR. This substitution is validation-only; the model has one observation
branch. Metadata confirmed the observation expansion was not yet published.

| Record type | Linked rows | Distinct appointment/type/record keys | Appointments with links |
|---|---:|---:|---:|
| Observation, including allergies | 56,163,795 | 56,163,795 | 12,222,479 |
| Medication order | 11,844,151 | 11,844,151 | 1,890,517 |
| Medication statement | 3,772,790 | 3,772,790 | 1,904,474 |

The total is 71,780,736 links, with no missing values in the original six output columns. The revised model
adds a namespaced clinical key and retains the detail key; its synthetic checks
pass. Recheck the final seven-column materialisation after publication.
The observation count reconciles to 56,023,167 native observations and 140,628
added allergies. Referrals currently have no populated encounter ID.
Appointment counts overlap across record types and must not be summed.

The source had 15,092,617 encounters with an appointment ID; 15,797 referenced
an appointment outside the retained appointment table. Matched paths had no
different-person records. There were 71 observation links with different
patient IDs for the same person, which remain included.

The uncached aggregate query over all six candidate columns took 20.4 seconds
on `WH_WNL_OLIDS_L`. Operator statistics reported about 69.2 GB scanned and
34.5 MB remote spill. This measures the link query and aggregate validation,
not table creation, clustering or the complete overnight build. The issue's
2.5 times WNL capacity assumption implies roughly 179.5 million links; it is
not a measured WNL runtime.

## Repeat the checks

Compile `conformed_appointment_clinical_record` and
`stable_appointment_clinical_record` with the tracked `stable` profile. Run
`check_appointment_link_fixtures.py` with the compiled conformed SQL path and
execute its output in Snowflake. It returns aggregate counts only.

`profile_appointment_links.py` accepts the same compiled path and emits
aggregate profiling SQL against the actual conformed inputs. After the
observation expansion publishes, it includes those added rows automatically.
Compare its counts and output hashes with the stable table after building.
Do not add the pre-expansion validation branches after publication.
