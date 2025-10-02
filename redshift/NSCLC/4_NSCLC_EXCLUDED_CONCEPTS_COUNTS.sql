WITH 
mets AS (
  SELECT person_id, min(measurement_date) as first_measurement_date 
  FROM @cdm_database_schema.measurement m
  INNER JOIN #Codesets cs 
    ON m.measurement_concept_id = cs.concept_id
  WHERE cs.codeset_id = 6
  GROUP BY person_id
),
diags AS (
  SELECT person_id, condition_start_date 
  FROM @cdm_database_schema.condition_occurrence co
  INNER JOIN #Codesets cs 
    ON co.condition_concept_id = cs.concept_id
  WHERE cs.codeset_id = 12
),
excluded_concepts AS (
  SELECT cs.concept_id 
  FROM #Codesets cs
  LEFT JOIN #Codesets cs2 
    ON cs.concept_id = cs2.concept_id
    AND cs2.codeset_id = 12
  WHERE cs.codeset_id = 13
  AND cs2.concept_id is null
),
combined_population AS (
  SELECT DISTINCT 
    mets.person_id, 
    mets.first_measurement_date, 
    diags.condition_start_date, 
    DATEDIFF(day, mets.first_measurement_date, diags.condition_start_date) as days_to_diag,
    ROW_NUMBER() OVER (PARTITION BY mets.person_id  ORDER BY ABS(DATEDIFF(day, mets.first_measurement_date, diags.condition_start_date)), DATEDIFF(day, mets.first_measurement_date, diags.condition_start_date) ) as rn  
  FROM mets
  INNER JOIN diags 
    ON mets.person_id = diags.person_id
),
diags_to_exclude AS (
  SELECT person_id, condition_concept_id,
         min(condition_start_date) as condition_start_date,
         count(*) as n_conditions
  FROM @cdm_database_schema.condition_occurrence co
  INNER JOIN excluded_concepts cs 
    ON co.condition_concept_id = cs.concept_id
  WHERE co.person_id not in (select person_id from combined_population)
  GROUP BY person_id, condition_concept_id
)
SELECT c.concept_name, 
       COUNT(DISTINCT d.person_id) as excluded_patient_count,
       COUNT(DISTINCT case when d.n_conditions > 1 then d.person_id end) as excluded_patient_count_multiple,
       COUNT(DISTINCT case when d.n_conditions > 2 then d.person_id end) as excluded_patient_count_multiple_2,
       COUNT(DISTINCT case when d.n_conditions > 3 then d.person_id end) as excluded_patient_count_multiple_3
FROM diags_to_exclude d
JOIN   @cdm_database_schema.concept c on c.concept_id = d.condition_concept_id
GROUP BY d.condition_concept_id, c.concept_name
ORDER BY excluded_patient_count DESC;