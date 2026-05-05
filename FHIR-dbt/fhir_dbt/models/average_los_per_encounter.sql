WITH los_per_encounter AS (
    SELECT 
        type AS encounter_type,
        datediff('hour', CAST(start AS TIMESTAMP), CAST("end" AS TIMESTAMP)) AS los_hours
    FROM {{source('fhir', 'encounters')}}
    WHERE start IS NOT NULL AND "end" IS NOT NULL
)
SELECT encounter_type, COUNT(*) AS encounter_count,
    ROUND(AVG(los_hours), 2) AS avg_los_hours
FROM los_per_encounter
GROUP BY encounter_type
ORDER BY avg_los_hours DESC