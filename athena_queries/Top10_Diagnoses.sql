SELECT
  code.text AS condition,
  COUNT(*) AS occurrences
FROM fhir_datastore.condition
WHERE code.text IS NOT NULL
GROUP BY code.text
ORDER BY occurrences DESC
LIMIT 10;
