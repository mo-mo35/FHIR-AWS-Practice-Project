WITH ages AS (
  SELECT
    id,
    gender,
    date_diff(
      'year',
      date_parse(birthdate, '%Y-%m-%d'),
      current_date
    ) AS age
  FROM fhir_datastore.patient
),
brackets AS (
  SELECT
    id,
    gender,
    CASE
      WHEN age < 18 THEN '0–17'
      WHEN age BETWEEN 18 AND 35 THEN '18–35'
      WHEN age BETWEEN 36 AND 55 THEN '36–55'
      WHEN age BETWEEN 56 AND 75 THEN '56–75'
      ELSE '76+'
    END AS age_bracket
  FROM ages
)
SELECT
  gender,
  age_bracket,
  COUNT(*) AS patient_count
FROM brackets
GROUP BY gender, age_bracket
ORDER BY age_bracket, gender;
