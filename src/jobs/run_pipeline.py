"""Run bronze -> silver (w/ quarantine) -> gold and print late-file findings."""
from __future__ import annotations

import csv
import sys
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.jobs.bronze_ingest import ingest_bronze
from src.jobs.gold_aggregate import build_gold
from src.jobs.silver_conform import conform_silver
from src.quality.late_files import late_file_report


def main() -> None:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("facility-incident-medallion")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = ROOT / "data" / "raw" / "incident_events.csv"
    fac = ROOT / "data" / "raw" / "dim_facility.csv"
    bronze = ROOT / "data" / "bronze" / "incident_events"
    silver = ROOT / "data" / "silver" / "incident_clean"
    quarantine = ROOT / "data" / "quarantine" / "incident_events"
    gold = ROOT / "data" / "gold" / "facility_incident_daily"
    explain = ROOT / "docs" / "shuffle_explain.txt"

    print("=== Bronze ingest ===")
    bdf = ingest_bronze(spark, raw, bronze)
    print(f"bronze_rows={bdf.count()}")

    print("=== Silver conform + quarantine + shuffle explain ===")
    sdf, qdf = conform_silver(spark, bronze, fac, silver, quarantine, explain)
    print(f"silver_rows={sdf.count()} quarantine_rows={qdf.count()}")

    print("=== Gold aggregate ===")
    gdf = build_gold(spark, silver, gold)
    print(f"gold_rows={gdf.count()}")
    gdf.orderBy("event_date", "facility_id").show(15, truncate=False)

    # Late file detection vs Saturday watermark
    with raw.open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    late = late_file_report(rows, "2025-08-16T06:00:00Z")
    print(f"=== Late files vs watermark ({len(late)}) ===")
    for item in late[:10]:
        print(item)

    # Control vs gold for the problem day
    control = sum(1 for r in rows if r["event_date"] == "2025-08-16")
    gold_day = gdf.filter("event_date = '2025-08-16'").agg({"incident_count": "sum"}).collect()
    gold_sum = int(gold_day[0][0]) if gold_day else 0
    print(f"=== 2025-08-16 control_rows={control} gold_incident_sum={gold_sum} ===")
    print(f"shuffle explain written to {explain}")
    spark.stop()


if __name__ == "__main__":
    main()
