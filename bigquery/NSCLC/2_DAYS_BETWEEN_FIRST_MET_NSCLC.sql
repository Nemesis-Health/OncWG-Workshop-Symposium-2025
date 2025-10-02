-------
--- Query to find days between metastastic date and NSCLC date
-------
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
combined_population as (
  select distinct 
    mets.person_id, 
    mets.first_measurement_date, 
    diags.condition_start_date, 
    DATE_DIFF(IF(SAFE_CAST(diags.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(diags.condition_start_date  AS STRING)),SAFE_CAST(diags.condition_start_date  AS DATE)), IF(SAFE_CAST(mets.first_measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(mets.first_measurement_date  AS STRING)),SAFE_CAST(mets.first_measurement_date  AS DATE)), DAY) as days_to_diag,
    row_number() over (partition by mets.person_id order by abs(DATE_DIFF(IF(SAFE_CAST(diags.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(diags.condition_start_date  AS STRING)),SAFE_CAST(diags.condition_start_date  AS DATE)), IF(SAFE_CAST(mets.first_measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(mets.first_measurement_date  AS STRING)),SAFE_CAST(mets.first_measurement_date  AS DATE)), DAY)), DATE_DIFF(IF(SAFE_CAST(diags.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(diags.condition_start_date  AS STRING)),SAFE_CAST(diags.condition_start_date  AS DATE)), IF(SAFE_CAST(mets.first_measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(mets.first_measurement_date  AS STRING)),SAFE_CAST(mets.first_measurement_date  AS DATE)), DAY)) as rn  
  from mets
  inner join diags 
    on mets.person_id = diags.person_id
),
combined_population_min_time as (
  select 
    person_id, 
    days_to_diag,
    row_number() over (order by days_to_diag) as order_nr 
  from combined_population
  where rn = 1
),
combined_population_min_time_n as (
  select count(distinct person_id) as n from combined_population_min_time
)
 select case when n < 5 then -5 else n end as n,
  case when n < 5 then null else min(days_to_diag) end as min_days_to_diag,
  case when n < 5 then null else min(case
            when order_nr < .05 * n
                then 9999
            else days_to_diag
            end) end AS val_q5_days_to_diag,
  case when n < 5 then null else min(case
            when order_nr < .10 * n
                then 9999
            else days_to_diag
            end) end AS val_q10_days_to_diag,
  case when n < 5 then null else min(case
            when order_nr < .25 * n
                then 9999
            else days_to_diag
            end) end AS val_q25_days_to_diag,
  case when n < 5 then null else min(case
            when order_nr < .50 * n
                then 9999
            else days_to_diag
            end) end as median_days_to_diag,
  case when n < 5 then null else min(case
            when order_nr < .75 * n
                then 9999
            else days_to_diag
            end) end AS val_q75_days_to_diag,
  case when n < 5 then null else min(case
            when order_nr < .9 * n
                then 9999
            else days_to_diag
            end) end AS val_q90_days_to_diag,
  case when n < 5 then null else min(case
            when order_nr < .95 * n
                then 9999
            else days_to_diag
            end) end AS val_q95_days_to_diag,
  case when n < 5 then null else max(days_to_diag) end as max_days_to_diag
 from combined_population_min_time
join combined_population_min_time_n as n
    on 1=1
 group by  1 ;