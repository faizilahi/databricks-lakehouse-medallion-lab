-- Surgical gold rebuild for a single event_date after late bronze lands
DELETE FROM gold_facility_incident_daily
WHERE event_date = DATE '2025-08-16';

INSERT INTO gold_facility_incident_daily
SELECT
    facility_id,
    event_date,
    region,
    COUNT(*) AS incident_count,
    COUNT(DISTINCT acuity_code) AS distinct_acuity_codes,
    AVG(length_of_stay_hrs) AS avg_los_hrs,
    SUM(CASE WHEN severity_band = 'HIGH' THEN 1 ELSE 0 END) AS high_severity_count,
    CURRENT_TIMESTAMP AS _gold_built_at
FROM silver_incident_clean
WHERE event_date = DATE '2025-08-16'
GROUP BY facility_id, event_date, region;
