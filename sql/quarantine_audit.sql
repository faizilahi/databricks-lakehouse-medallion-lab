-- Audit quarantined incident rows by reject reason and event_date
SELECT
    event_date,
    _reject_reason,
    acuity_code,
    COUNT(*) AS rejected_rows,
    COUNT(DISTINCT facility_id) AS facilities_touched
FROM quarantine_incident_events
GROUP BY event_date, _reject_reason, acuity_code
ORDER BY event_date, rejected_rows DESC;
