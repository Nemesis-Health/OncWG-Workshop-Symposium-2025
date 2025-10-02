with 
mets as (
   select person_id, min(measurement_date) as first_measurement_date 
   from @cdm_database_schema.measurement m
  inner join c40zm6cocodesets cs 
    on m.measurement_concept_id = cs.concept_id
  where cs.codeset_id = 6
   group by  1 ),
diags as (
  select person_id, condition_start_date 
  from @cdm_database_schema.condition_occurrence co
  inner join c40zm6cocodesets cs 
    on co.condition_concept_id = cs.concept_id
  where cs.codeset_id = 12
),
excluded_concepts as (
  select cs.concept_id 
  from c40zm6cocodesets cs
  left join c40zm6cocodesets cs2 
    on cs.concept_id = cs2.concept_id
    and cs2.codeset_id = 12
  where cs.codeset_id = 13
  and cs2.concept_id is null
),
diags_to_exclude as (
  select person_id, condition_concept_id,condition_start_date 
  from @cdm_database_schema.condition_occurrence co
  inner join excluded_concepts cs 
    on co.condition_concept_id = cs.concept_id
),
l01_exposures as (
  select person_id, drug_exposure_start_date 
  from @cdm_database_schema.drug_exposure de
  inner join c40zm6cocodesets cs 
    on de.drug_concept_id = cs.concept_id
  where cs.codeset_id = 11
),
combined_population_treated as (
   select distinct 
    mets.person_id, 
    DATE_DIFF(IF(SAFE_CAST(min(de.drug_exposure_start_date)  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(min(de.drug_exposure_start_date)  AS STRING)),SAFE_CAST(min(de.drug_exposure_start_date)  AS DATE)), IF(SAFE_CAST(min(mets.first_measurement_date)  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(min(mets.first_measurement_date)  AS STRING)),SAFE_CAST(min(mets.first_measurement_date)  AS DATE)), DAY) as days_to_treatment  
    from mets
  inner join l01_exposures de
    on mets.person_id = de.person_id
    and de.drug_exposure_start_date >= mets.first_measurement_date
  where mets.person_id not in (select person_id from diags_to_exclude)
  and mets.person_id in (select person_id from diags)
   group by  mets.person_id
 ),
combined_population_treated_ranked as (
  select distinct 
    person_id, 
    days_to_treatment,
    row_number() over (order by days_to_treatment) as order_nr
   from combined_population_treated
),
combined_population_treated_n as (
  select count(distinct person_id) as n from combined_population_treated_ranked
)
 select n,
  min(days_to_treatment) as min_days_to_treatment,
   min(case
            when order_nr < .05 * n
                then 9999
            else days_to_treatment
            end) AS val_q5_days_to_diag,
 min(case
            when order_nr < .10 * n
                then 9999
            else days_to_treatment
            end) AS val_q10_days_to_diag,
     min(case
            when order_nr < .25 * n
                then 9999
            else days_to_treatment
            end) AS val_q25_days_to_diag,
    min(case
            when order_nr < .50 * n
                then 9999
            else days_to_treatment
            end) as median_days_to_diag,
    min(case
            when order_nr < .75 * n
                then 9999
            else days_to_treatment
            end) AS val_q75_days_to_diag,
    min(case
            when order_nr < .9 * n
                then 9999
            else days_to_treatment
            end) AS val_q90_days_to_diag,
        min(case
            when order_nr < .95 * n
                then 9999
            else days_to_treatment
            end) AS val_q95_days_to_diag,
    max(days_to_treatment) as max_days_to_treatment
 from combined_population_treated_ranked
join combined_population_treated_n n
    on 1 = 1
 group by  1 ;