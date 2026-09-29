# Late-Arriving Facility Incidents: Bronze Quarantine and a PySpark Shuffle That Tripled Runtime

Faiz Elahi — [LinkedIn](https://www.linkedin.com/in/faizilahi) — [pendataco.com](https://pendataco.com) — [GitHub](https://github.com/faizilahi)

All incident, facility, and acuity rows in this repository are generated.

## The gold mart that under-counted overnight acuity

A Databricks Unity Catalog lakehouse serving clinical operations published `gold_facility_incident_daily`. After a holiday weekend, ops reported that Saturday acuity volume looked ~18% low versus the source EHR extract. Bronze had accepted the files. Silver had fewer rows than bronze for the same `event_date`. The missing slice was late-arriving files whose `_landing_ts` fell after the silver watermark, plus malformed acuity codes that silver silently dropped instead of quarantining.

This lab rebuilds that incident with a local PySpark pipeline (no Databricks workspace required): bronze ingest with file metadata, silver conform + quarantine table, gold aggregate, and an `explain(True)` dump of the shuffle that dominated runtime once the join key cardinality spiked.

## Medallion flow that failed

```
raw CSV drops
   -> bronze_incident_events (append, partition by event_date)
   -> silver_incident_clean  (watermark on _landing_ts; bad acuity -> quarantine)
   -> gold_facility_incident_daily (facility × event_date metrics)
```

Late files for `event_date=2025-08-16` landed Monday with `_landing_ts` past the silver high-water mark. Silver never reprocessed that partition. Quarantine was empty because the drop path used `filter` instead of writing rejects.

## PySpark job and the shuffle

`src/jobs/silver_conform.py` broadcasts the acuity reference when it is small, then intentionally demonstrates the slow path: a wide join on `facility_id` without broadcast once the dim is large enough to force an `Exchange hashpartitioning`. The README section **Shuffle anatomy** walks the physical plan stages captured in `docs/shuffle_explain.txt`.

## Layout

| Path | Role |
|------|------|
| `src/jobs/` | bronze_ingest, silver_conform, gold_aggregate, run_pipeline |
| `src/quality/` | acuity code validator, late-file detector |
| `sql/` | quarantine audit and gold rebuild for a single event_date |
| `scripts/generate_incident_files.py` | synthetic EHR drops including late + bad rows |
| `docs/incident_postmortem.md` | timeline and root cause |

## Run

```powershell
cd databricks-lakehouse-medallion-lab
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python scripts/generate_incident_files.py
python -m src.jobs.run_pipeline
```

Expect quarantine rows for invalid acuity codes, a late-file report for 2025-08-16, and `docs/shuffle_explain.txt` written from the Spark plan.
