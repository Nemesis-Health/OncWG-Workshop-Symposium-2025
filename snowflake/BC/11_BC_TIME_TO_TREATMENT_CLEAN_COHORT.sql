WITH 
mets AS (
  SELECT person_id, min(measurement_date) as first_measurement_date 
  FROM @cdm_database_schema.measurement m
  INNER JOIN c40zm6coCodesets cs 
    ON m.measurement_concept_id = cs.concept_id
  WHERE cs.codeset_id = 6
  GROUP BY person_id
),
diags AS (
  SELECT person_id, condition_start_date 
  FROM @cdm_database_schema.condition_occurrence co
  INNER JOIN c40zm6coCodesets cs 
    ON co.condition_concept_id = cs.concept_id
  WHERE cs.codeset_id = 15
),
excluded_concepts AS (
  SELECT cs.concept_id 
  FROM c40zm6coCodesets cs
  LEFT JOIN c40zm6coCodesets cs2 
    ON cs.concept_id = cs2.concept_id
    AND cs2.codeset_id = 15
  WHERE cs.codeset_id = 13
  AND cs2.concept_id is null
),
diags_to_exclude AS (
  SELECT person_id, condition_concept_id,condition_start_date 
  FROM @cdm_database_schema.condition_occurrence co
  INNER JOIN excluded_concepts cs 
    ON co.condition_concept_id = cs.concept_id
),
l01_exposures AS (
  SELECT person_id, drug_exposure_start_date 
  FROM @cdm_database_schema.drug_exposure de
  INNER JOIN c40zm6coCodesets cs 
    ON de.drug_concept_id = cs.concept_id
  WHERE cs.codeset_id = 11
),
combined_population_treated AS (
  SELECT DISTINCT 
    mets.person_id, 
    DATEDIFF(day, min(mets.first_measurement_date), min(de.drug_exposure_start_date)) as days_to_treatment  
   FROM mets
  INNER JOIN l01_exposures de
    ON mets.person_id = de.person_id
    AND de.drug_exposure_start_date >= mets.first_measurement_date
  WHERE mets.person_id not in (select person_id from diags_to_exclude)
  AND mets.person_id in (select person_id from diags)
  GROUP BY mets.person_id
),
combined_population_treated_ranked AS (
  SELECT DISTINCT 
    person_id, 
    days_to_treatment,
    row_number() over (order by days_to_treatment) as order_nr
   FROM combined_population_treated
),
combined_population_treated_n AS (
  SELECT COUNT(DISTINCT person_id) as n FROM combined_population_treated_ranked
)
SELECT 
  n,
  MIN(days_to_treatment) as min_days_to_treatment,
   MIN(CASE
            WHEN order_nr < .05 * n
                THEN 9999
            ELSE days_to_treatment
            END) AS q5_days_to_diag,
 MIN(CASE
            WHEN order_nr < .10 * n
                THEN 9999
            ELSE days_to_treatment
            END) AS q10_days_to_diag,
     MIN(CASE
            WHEN order_nr < .25 * n
                THEN 9999
            ELSE days_to_treatment
            END) AS q25_days_to_diag,
    MIN(CASE
            WHEN order_nr < .50 * n
                THEN 9999
            ELSE days_to_treatment
            END) AS median_days_to_diag,
    MIN(CASE
            WHEN order_nr < .75 * n
                THEN 9999
            ELSE days_to_treatment
            END) AS q75_days_to_diag,
    MIN(CASE
            WHEN order_nr < .9 * n
                THEN 9999
            ELSE days_to_treatment
            END) AS q90_days_to_diag,
        MIN(CASE
            WHEN order_nr < .95 * n
                THEN 9999
            ELSE days_to_treatment
            END) AS q95_days_to_diag,
    MAX(days_to_treatment) AS max_days_to_treatment
FROM combined_population_treated_ranked
JOIN combined_population_treated_n n
    ON 1 = 1
GROUP BY n;