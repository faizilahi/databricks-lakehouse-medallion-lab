"""Bronze ingest: append raw events with landing metadata, partition by event_date."""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def ingest_bronze(spark: SparkSession, raw_csv: Path, bronze_path: Path) -> DataFrame:
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(raw_csv))
        .withColumn("_ingest_batch_id", F.lit(datetime_batch()))
        .withColumn("_bronze_written_at", F.current_timestamp())
    )
    (
        df.write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(str(bronze_path))
    )
    # File manifest for completeness checks
    manifest = (
        df.groupBy("event_date", "facility_id", "_source_file")
        .agg(
            F.count("*").alias("row_count"),
            F.min("_landing_ts").alias("min_landing_ts"),
            F.max("_landing_ts").alias("max_landing_ts"),
        )
    )
    manifest.write.mode("overwrite").json(str(bronze_path.parent / "bronze_manifest"))
    return df


def datetime_batch() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
