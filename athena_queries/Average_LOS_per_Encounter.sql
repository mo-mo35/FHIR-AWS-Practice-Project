WITH los_per_encounter AS (
  SELECT 
    e."class"."code" AS encounter_type,
    date_diff(
      'hour',
      from_iso8601_timestamp(e.period."start"),
      from_iso8601_timestamp(e.period."end")
    ) AS los_hours
  FROM "fhir_datastore"."encounter" AS e
  WHERE e.period."start" IS NOT NULL
    AND e.period."end" IS NOT NULL
)
SELECT 
  encounter_type,
  COUNT(*) AS encounter_count,
  ROUND(AVG(los_hours), 2) AS avg_los_hours
FROM los_per_encounter
GROUP BY encounter_type
ORDER BY avg_los_hours DESC;
