"""Gold aggregate: facility × event_date incident metrics."""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_gold(spark: SparkSession, silver_path: Path, gold_path: Path) -> DataFrame:
    silver = spark.read.parquet(str(silver_path))
    gold = (
        silver.groupBy("facility_id", "event_date", "region")
        .agg(
            F.count("*").alias("incident_count"),
            F.countDistinct("acuity_code").alias("distinct_acuity_codes"),
            F.avg("length_of_stay_hrs").alias("avg_los_hrs"),
            F.sum(F.when(F.col("severity_band") == "HIGH", 1).otherwise(0)).alias(
                "high_severity_count"
            ),
        )
        .withColumn("_gold_built_at", F.current_timestamp())
    )
    gold.write.mode("overwrite").partitionBy("event_date").parquet(str(gold_path))
    return gold
