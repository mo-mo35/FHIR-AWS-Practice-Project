WITH condition_counts AS (
    SELECT
        patient_id,
        COUNT(DISTINCT code_text) AS num_conditions
    FROM {{ source('fhir', 'conditions') }}
    WHERE patient_id IS NOT NULL
    GROUP BY patient_id
),
patient_ages AS (
    SELECT
        id AS patient_id,
        datediff('year', CAST(birthDate AS DATE), current_date) AS age
    FROM {{ source('fhir', 'patients') }}
    WHERE birthDate IS NOT NULL
),
merged AS (
    SELECT
        cc.patient_id,
        cc.num_conditions,
        CASE
            WHEN pa.age < 18 THEN '0-17'
            WHEN pa.age BETWEEN 18 AND 35 THEN '18-35'
            WHEN pa.age BETWEEN 36 AND 55 THEN '36-55'
            WHEN pa.age BETWEEN 56 AND 75 THEN '56-75'
            ELSE '76+'
        END AS age_group,
        CASE
            WHEN cc.num_conditions BETWEEN 0 AND 2 THEN '0-2'
            WHEN cc.num_conditions BETWEEN 3 AND 5 THEN '3-5'
            WHEN cc.num_conditions BETWEEN 6 AND 8 THEN '6-8'
            ELSE '9+'
        END AS condition_bracket
    FROM condition_counts cc
    JOIN patient_ages pa ON cc.patient_id = pa.patient_id
)
SELECT
    age_group,
    condition_bracket,
    COUNT(*) AS patient_count,
    ROUND(AVG(num_conditions), 2) AS avg_num_conditions
FROM merged
GROUP BY age_group, condition_bracket
ORDER BY age_group, condition_bracket