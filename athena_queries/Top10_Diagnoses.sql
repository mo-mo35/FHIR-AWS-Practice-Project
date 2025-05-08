SELECT
  code AS condition,
  COUNT(*) AS occurrences
FROM fhir_datastore.condition
WHERE code IS NOT NULL
GROUP BY code
ORDER BY occurrences DESC
LIMIT 10;
