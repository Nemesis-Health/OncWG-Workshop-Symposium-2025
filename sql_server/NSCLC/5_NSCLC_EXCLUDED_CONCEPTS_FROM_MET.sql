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
    ROW_NUMBER() OVER (PARTITION BY mets.person_id ORDER BY ABS(DATEDIFF(day, mets.first_measurement_date, diags.condition_start_date)), DATEDIFF(day, mets.first_measurement_date, diags.condition_start_date)) as rn  
  FROM mets
  INNER JOIN diags 
    ON mets.person_id = diags.person_id
),
diags_to_exclude AS (
  SELECT person_id, 
         condition_concept_id,
         condition_start_date
  FROM @cdm_database_schema.condition_occurrence co
  INNER JOIN excluded_concepts cs 
    ON co.condition_concept_id = cs.concept_id
  WHERE co.person_id not in (select person_id from combined_population)
),
combined_population_exclusions AS (
  SELECT DISTINCT 
    mets.person_id, 
    mets.first_measurement_date, 
    diags_to_exclude.condition_start_date, 
    diags_to_exclude.condition_concept_id,
    DATEDIFF(day, mets.first_measurement_date, diags_to_exclude.condition_start_date) as days_to_diag,
    ROW_NUMBER() OVER (PARTITION BY mets.person_id, diags_to_exclude.condition_concept_id ORDER BY ABS(DATEDIFF(day, mets.first_measurement_date, diags_to_exclude.condition_start_date)), DATEDIFF(day, mets.first_measurement_date, diags_to_exclude.condition_start_date)) as rn  
  FROM mets
  INNER JOIN diags_to_exclude 
    ON mets.person_id = diags_to_exclude.person_id
),
combined_population_min_time AS (
  SELECT 
    person_id, 
    days_to_diag,
    condition_concept_id,
    ROW_NUMBER() OVER (PARTITION BY condition_concept_id ORDER BY days_to_diag) as order_nr 
  FROM combined_population_exclusions
  WHERE rn = 1
),
combined_population_min_time_n AS (
  SELECT condition_concept_id,COUNT(DISTINCT person_id) as n FROM combined_population_min_time GROUP BY condition_concept_id
)
SELECT 
  CASE WHEN n < 5 THEN -5 ELSE n END as n,
  n.condition_concept_id,
  c.concept_name,
  CASE WHEN n < 5 THEN NULL ELSE MIN(days_to_diag) END as min_days_to_diag,
  CASE WHEN n < 5 THEN NULL ELSE MIN(CASE
            WHEN order_nr < .05 * n
                THEN 9999
            ELSE days_to_diag
            END) END AS q5_days_to_diag,
  CASE WHEN n < 5 THEN NULL ELSE MIN(CASE
            WHEN order_nr < .10 * n
                THEN 9999
            ELSE days_to_diag
            END) END AS q10_days_to_diag,
  CASE WHEN n < 5 THEN NULL ELSE MIN(CASE
            WHEN order_nr < .25 * n
                THEN 9999
            ELSE days_to_diag
            END) END AS q25_days_to_diag,
  CASE WHEN n < 5 THEN NULL ELSE MIN(CASE
            WHEN order_nr < .50 * n
                THEN 9999
            ELSE days_to_diag
            END) END AS median_days_to_diag,
  CASE WHEN n < 5 THEN NULL ELSE MIN(CASE
            WHEN order_nr < .75 * n
                THEN 9999
            ELSE days_to_diag
            END) END AS q75_days_to_diag,
  CASE WHEN n < 5 THEN NULL ELSE MIN(CASE
            WHEN order_nr < .9 * n
                THEN 9999
            ELSE days_to_diag
            END) END AS q90_days_to_diag,
  CASE WHEN n < 5 THEN NULL ELSE MIN(CASE
            WHEN order_nr < .95 * n
                THEN 9999
            ELSE days_to_diag
            END) END AS q95_days_to_diag,
  CASE WHEN n < 5 THEN NULL ELSE MAX(days_to_diag) END AS max_days_to_diag
FROM combined_population_min_time
JOIN combined_population_min_time_n as n
    ON combined_population_min_time.condition_concept_id = n.condition_concept_id
JOIN @cdm_database_schema.concept c on c.concept_id = combined_population_min_time.condition_concept_id
GROUP BY n, n.condition_concept_id, c.concept_name;