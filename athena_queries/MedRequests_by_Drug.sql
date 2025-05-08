SELECT
  medicationcodeableconcept.text AS drug,
  COUNT(*) AS request_count
FROM fhir_datastore.medicationrequest
GROUP BY medicationcodeableconcept.text
ORDER BY request_count DESC
LIMIT 10;
