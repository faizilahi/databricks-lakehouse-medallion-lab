"""Silver conform: validate acuity, quarantine rejects, join facility dim with shuffle demo."""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.quality.acuity_rules import VALID_ACUITY


def conform_silver(
    spark: SparkSession,
    bronze_path: Path,
    facility_csv: Path,
    silver_path: Path,
    quarantine_path: Path,
    explain_path: Path,
    force_shuffle: bool = True,
) -> tuple[DataFrame, DataFrame]:
    bronze = spark.read.parquet(str(bronze_path))
    valid = F.col("acuity_code").isin(list(VALID_ACUITY))

    clean = bronze.filter(valid).withColumn("_silver_status", F.lit("CLEAN"))
    quarantine = (
        bronze.filter(~valid)
        .withColumn("_silver_status", F.lit("QUARANTINE"))
        .withColumn("_reject_reason", F.lit("INVALID_ACUITY_CODE"))
    )

    facilities = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(facility_csv))
    )

    # Inflate facility dim to force a hash exchange when force_shuffle=True
    if force_shuffle:
        # Cross-join padding keys so join input is large enough that Spark plans Exchange
        pad = spark.range(0, 50).withColumnRenamed("id", "pad_id")
        facilities_wide = facilities.crossJoin(pad).withColumn(
            "facility_join_key",
            F.concat(F.col("facility_id"), F.lit("-"), F.col("pad_id").cast(StringType())),
        )
        clean_keyed = clean.withColumn(
            "facility_join_key",
            F.concat(F.col("facility_id"), F.lit("-"), F.lit("0")),
        )
        # Disable broadcast to guarantee shuffle exchange in the plan
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        joined = clean_keyed.join(facilities_wide, on="facility_join_key", how="left")
    else:
        joined = clean.join(F.broadcast(facilities), on="facility_id", how="left")

    # Capture physical plan for shuffle documentation
    explain_path.parent.mkdir(parents=True, exist_ok=True)
    plan = joined._jdf.queryExecution().simpleString()
    # Also get string from explain
    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        joined.explain(True)
    explain_path.write_text(
        "=== simpleString ===\n"
        + str(plan)
        + "\n\n=== explain(True) ===\n"
        + buf.getvalue(),
        encoding="utf-8",
    )

    out_cols = [
        "incident_id",
        "facility_id",
        "event_date",
        "event_ts",
        "acuity_code",
        "severity_band",
        "length_of_stay_hrs",
        "region",
        "beds",
        "_landing_ts",
        "_silver_status",
    ]
    # region/beds may be null if shuffle path duplicated — take first match columns
    for c in ("region", "beds"):
        if c not in joined.columns:
            joined = joined.withColumn(c, F.lit(None))

    silver = joined.select(*[c for c in out_cols if c in joined.columns])
    silver.write.mode("overwrite").partitionBy("event_date").parquet(str(silver_path))
    quarantine.write.mode("overwrite").parquet(str(quarantine_path))
    return silver, quarantine
