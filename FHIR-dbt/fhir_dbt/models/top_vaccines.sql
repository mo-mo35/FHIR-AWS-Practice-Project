SELECT
  vaccine_text AS vaccine_type,
  COUNT(*) AS total_administered
FROM {{source ('fhir', 'immunizations')}}
GROUP BY vaccine_text
ORDER BY total_administered DESC
LIMIT 10