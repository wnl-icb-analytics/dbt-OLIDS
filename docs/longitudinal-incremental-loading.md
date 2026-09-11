# Incremental longitudinal outputs

`stable_healthcare_event` and `stable_clinical_record` retain their existing
columns and grains. Normal builds merge records whose source extraction is at
or after the stored watermark for their source record type. Equal extraction
times are replayed; null times are always reconsidered. Clinical dates are not
load watermarks, so newly delivered historical records can load immediately.

A post-build key comparison removes withdrawn records, including records no
longer in the filtered patient spine. The monthly full refresh handles changes
without a newer extraction timestamp. These include label changes and updated
statement context on an unchanged medication order. Standalone statements do
not become clinical records.

On the first day of each month, the scheduled workflow builds the usual upstream
selection, then fully refreshes only these two stable outputs. It bypasses the
unchanged-watermark skip for that run. It does not fully refresh the
pseudonymisation indexes. Source freshness checks still apply.

Healthcare events cluster by `person_id`, then
`coalesce(event_at, event_date::timestamp_ntz)`. Clinical records cluster by
`person_id`, then `clinical_record_date`: their source dates do not establish
clinical clock times. The storage fallback does not populate missing event
timestamps. Consumers still need `ORDER BY` for guaranteed presentation order.

Runtime hooks select `WH_WNL_OLIDS_L` for initial builds and full refreshes when
the profile role is `DBT_ADMIN`. Other roles retain their profile warehouse.
Normal increments use the profile warehouse, which is already L in the scheduled
workflow. A final hook restores the configured warehouse.

Deploy the new physical clustering with a full refresh of these two models, or
allow the monthly full refresh to apply it. Code worktrees do not isolate
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

Production tables have not been rebuilt for this validation. The first full
refresh, merge performance and scheduled monthly execution remain deployment
checks. The related dbt-analytics change exercises receipt boundaries,
corrections, withdrawn records and monthly reconciliation with synthetic data,
and checks repeat increments against full-source aggregate fingerprints.
