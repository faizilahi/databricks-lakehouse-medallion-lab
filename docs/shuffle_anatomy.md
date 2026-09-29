# Shuffle anatomy (from the local Spark plan)

When `force_shuffle=True`, silver disables auto-broadcast and joins incidents to an inflated facility dimension. The physical plan in `shuffle_explain.txt` shows:

1. **Scan parquet** — bronze partitions for the read window.
2. **Filter** — acuity code IN (valid set); rejects go to the quarantine write path.
3. **Exchange hashpartitioning(facility_join_key)** — both sides shuffled so matching keys land on the same executor.
4. **SortMergeJoin** or **ShuffledHashJoin** — depending on Spark version and stats.
5. **Project** — drop pad columns before parquet write.

Interview talking point: broadcast the small dim when it fits; once facility attributes grow (or you pad for SCD/history), the Exchange dominates stage time. Measure with `spark.sql.shuffle.partitions` and skew on hot `facility_id` values.
