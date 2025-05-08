SELECT
  subject.reference AS patient_id,
  COUNT(*) AS obs_count
FROM fhir_datastore.observation
WHERE subject.reference IS NOT NULL
GROUP BY subject.reference
HAVING COUNT(*) > 50
ORDER BY obs_count DESC;
