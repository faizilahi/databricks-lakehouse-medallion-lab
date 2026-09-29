SELECT
    event_date,
    facility_id,
    _source_file,
    _landing_ts,
    COUNT(*) AS late_rows
FROM bronze_incident_events
WHERE event_date = DATE '2025-08-16'
  AND CAST(_landing_ts AS TIMESTAMP) > TIMESTAMP '2025-08-16 06:00:00'
GROUP BY event_date, facility_id, _source_file, _landing_ts
ORDER BY _landing_ts;
