# Postmortem — under-counted Saturday acuity (synthetic)

## Impact
Ops dashboard showed 1,104 acuity events for 2025-08-16; EHR extract control showed 1,347. Gold mart under-count: 243 events (18%).

## Timeline
| When | What |
|------|------|
| Sat 02:00 | Normal bronze drop for 2025-08-16 (partial — EHR delayed 2 facilities) |
| Sat 06:00 | Silver watermark advanced to 2025-08-16T06:00Z |
| Mon 01:15 | Late bronze files for facilities F-017, F-022 land |
| Mon 06:00 | Silver incremental skip — watermark already past |
| Mon 09:40 | Ops pages on acuity discrepancy |
| Mon 11:10 | Quarantine path found empty; late-file detector flags 2 partitions |
| Mon 14:00 | Silver backfill for event_date + quarantine writer deployed |

## Root cause
1. Watermark advanced on partial partition completeness.
2. Invalid acuity codes filtered out rather than written to `quarantine_incident_events`.
3. No partition-completeness signal from bronze `_file_manifest`.

## Fix
- Completeness gate: silver waits for expected facility count per event_date.
- Quarantine table for schema/code rejects.
- Gold rebuild job keyed by event_date for surgical backfill.
