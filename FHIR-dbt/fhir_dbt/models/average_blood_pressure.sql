SELECT
  date_trunc('month', CAST(effective_date AS TIMESTAMP)) AS month,
  AVG(value) AS avg_bp
FROM  {{source ('fhir', 'observations')}}
WHERE code_text = 'Blood Pressure'
  AND value IS NOT NULL
GROUP BY 1
ORDER BY 1