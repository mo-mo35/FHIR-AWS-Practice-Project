SELECT
    strftime(CAST(start AS TIMESTAMP), '%A') AS day_of_week,
    COUNT(*) AS encounters
FROM {{ source('fhir', 'encounters') }}
WHERE start IS NOT NULL
GROUP BY day_of_week
ORDER BY
    CASE day_of_week
        WHEN 'Sunday' THEN 1
        WHEN 'Monday' THEN 2
        WHEN 'Tuesday' THEN 3
        WHEN 'Wednesday' THEN 4
        WHEN 'Thursday' THEN 5
        WHEN 'Friday' THEN 6
        WHEN 'Saturday' THEN 7
        ELSE 8
    END