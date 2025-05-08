SELECT
  vaccinecode.text AS vaccine_type,
  COUNT(*) AS total_administered
FROM fhir_datastore.immunization
GROUP BY vaccinecode.text
ORDER BY total_administered DESC
LIMIT 10;
