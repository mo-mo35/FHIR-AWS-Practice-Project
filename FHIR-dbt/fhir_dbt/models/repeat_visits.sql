SELECT
    patient_id,
    COUNT(*) AS num_visits,
    MIN(CAST(start AS TIMESTAMP)) AS first_visit,
    MAX(CAST(start AS TIMESTAMP)) AS last_visit
FROM {{ source('fhir', 'encounters') }}
WHERE start IS NOT NULL
GROUP BY patient_id
HAVING COUNT(*) > 1