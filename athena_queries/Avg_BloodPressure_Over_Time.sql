SELECT
  date_trunc(
    'month',
    CAST(from_iso8601_timestamp(effectivedatetime) AS timestamp)
  ) AS month,
  AVG(comp.valuequantity.value) AS avg_bp
FROM fhir_datastore.observation
CROSS JOIN UNNEST(component) AS t(comp)
WHERE code.text = 'Blood Pressure'
  AND comp.valuequantity.value IS NOT NULL
GROUP BY 1
ORDER BY 1;
