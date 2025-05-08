SELECT
  subject.reference AS patient_id,
  COUNT(*) AS num_visits,
  MIN(from_iso8601_timestamp(period.start)) AS first_visit,
  MAX(from_iso8601_timestamp(period.start)) AS last_visit
FROM fhir_datastore.encounter
WHERE period.start IS NOT NULL
GROUP BY subject.reference
HAVING COUNT(*) > 1;
