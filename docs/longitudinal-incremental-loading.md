# Incremental longitudinal outputs

`stable_healthcare_event` and `stable_clinical_record` retain their existing
columns and grains. Normal builds merge records whose source extraction is at
or after the stored watermark for their source record type. Equal extraction
times are replayed; null times are always reconsidered. Clinical dates are not
load watermarks, so newly delivered historical records can load immediately.

A post-build comparison of source record type and source ID removes withdrawn
records, including records no longer in the filtered patient spine. Events also
compare event type so a withdrawn booking does not survive beside its slot.
These natural keys avoid deriving UUIDs over the whole conformed history.
The monthly full refresh handles changes
without a newer extraction timestamp. These include label changes and updated
statement context on an unchanged medication order. Standalone statements do
not become clinical records.

On the first day of each month, the scheduled workflow builds the usual upstream
selection, then fully refreshes only these two stable outputs. It bypasses the
unchanged-watermark skip for that run. It does not fully refresh the
pseudonymisation indexes. Source freshness checks still apply.

Both stable outputs store `sk_patient_id` as text, preserving the approved key
values. This matches the cross-system analyst interface and permits direct
filtering without converting the stored numeric key at query time.

Healthcare events cluster by `sk_patient_id`, then
`coalesce(event_at, event_date::timestamp_ntz)`. Clinical records cluster by
`sk_patient_id`, then `clinical_record_date`: their source dates do not establish
clinical clock times. The storage fallback does not populate missing event
timestamps. Consumers still need `ORDER BY` for guaranteed presentation order.

Runtime hooks select `WH_WNL_OLIDS_L` for initial builds and full refreshes when
the profile role is `DBT_ADMIN`. Other roles retain their profile warehouse.
Normal increments use the profile warehouse, which is already L in the scheduled
workflow. A final hook restores the configured warehouse.

Deploy the text key and physical clustering with a full refresh of these two
models or a metadata-preserving rewrite of their existing prepared snapshots.
An incremental merge alone does not change the stored key type. Code worktrees do not isolate
warehouse objects: the established stable target points to `OLIDS_ENGINEERING`.
Compilation and read-only aggregate checks can validate this change without
rebuilding production tables before review.

## Validation on 11 September 2026

Both models compiled against the established stable target. A read-only count
of the compiled incremental queries selected 138,875 healthcare events and
1,010,772 clinical records. The stored clinical population has 1,974,867,939
rows. These counts include replay of the extraction boundary and are not counts
of newly created clinical events.

The source extraction field is populated on all retained observation and order
rows in the inspected snapshot. Observations span 249 extraction timestamps;
orders span 186. Extraction times therefore support selective loading rather
than selecting every row as a newly dated snapshot.

The initial validation did not rebuild production. A later person-lookup
correction rewrites the existing prepared snapshots on L with text keys and
cross-system person clustering. Full-source refresh performance, merge
performance and scheduled monthly execution remain deployment checks.
A read-only withdrawal comparison across both full outputs took 60
seconds on L and found no withdrawn keys. The preceding UUID-based comparison
also found none and took 164 seconds; these are single measurements, with
different warehouse cache states, not a controlled speed comparison.
The related dbt-analytics change exercises receipt boundaries,
corrections, withdrawn records and monthly reconciliation with synthetic data,
and checks repeat increments against full-source aggregate fingerprints.

The snapshot rewrites completed in 31 seconds for 179,290,676 events and 489
seconds for 1,974,867,939 clinical records. Counts and whole-row fingerprints
matched before and after, comparing the original numeric key as text. Grants
and column metadata were preserved. The same cross-system clinical lookup now
assigns one OLIDS partition, about 19 MB, instead of 6,569 partitions and about
123 GB. This is a plan comparison, not a post-change execution benchmark.
Merge this change before the next scheduled build to retain the corrected key
type and clustering.
