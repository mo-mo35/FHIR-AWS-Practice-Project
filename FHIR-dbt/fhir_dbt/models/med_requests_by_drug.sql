SELECT
    medication AS drug,
    COUNT(*) AS request_count
FROM {{ source('fhir', 'medications') }}
WHERE medication IS NOT NULL
GROUP BY medication
ORDER BY request_count DESC
LIMIT 10