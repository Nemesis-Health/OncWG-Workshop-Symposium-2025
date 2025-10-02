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
  where cs.codeset_id = 15
),
excluded_concepts as (
  select cs.concept_id 
  from c40zm6cocodesets cs
  left join c40zm6cocodesets cs2 
    on cs.concept_id = cs2.concept_id
    and cs2.codeset_id = 15
  where cs.codeset_id = 13
  and cs2.concept_id is null
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
diags_to_exclude as (
  select person_id, 
         condition_concept_id,
         condition_start_date
  from @cdm_database_schema.condition_occurrence co
  inner join excluded_concepts cs 
    on co.condition_concept_id = cs.concept_id
  where co.person_id in (select person_id from combined_population)
),
combined_population_exclusions as (
  select distinct 
    mets.person_id, 
    mets.first_measurement_date, 
    diags_to_exclude.condition_start_date, 
    diags_to_exclude.condition_concept_id,
    DATE_DIFF(IF(SAFE_CAST(diags_to_exclude.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(diags_to_exclude.condition_start_date  AS STRING)),SAFE_CAST(diags_to_exclude.condition_start_date  AS DATE)), IF(SAFE_CAST(mets.first_measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(mets.first_measurement_date  AS STRING)),SAFE_CAST(mets.first_measurement_date  AS DATE)), DAY) as days_to_diag,
    row_number() over (partition by mets.person_id, diags_to_exclude.condition_concept_id order by abs(DATE_DIFF(IF(SAFE_CAST(diags_to_exclude.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(diags_to_exclude.condition_start_date  AS STRING)),SAFE_CAST(diags_to_exclude.condition_start_date  AS DATE)), IF(SAFE_CAST(mets.first_measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(mets.first_measurement_date  AS STRING)),SAFE_CAST(mets.first_measurement_date  AS DATE)), DAY)), DATE_DIFF(IF(SAFE_CAST(diags_to_exclude.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(diags_to_exclude.condition_start_date  AS STRING)),SAFE_CAST(diags_to_exclude.condition_start_date  AS DATE)), IF(SAFE_CAST(mets.first_measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(mets.first_measurement_date  AS STRING)),SAFE_CAST(mets.first_measurement_date  AS DATE)), DAY)) as rn  
  from mets
  inner join diags_to_exclude 
    on mets.person_id = diags_to_exclude.person_id
),
combined_population_min_time as (
  select 
    person_id, 
    days_to_diag,
    condition_concept_id,
    row_number() over (partition by condition_concept_id order by days_to_diag) as order_nr 
  from combined_population_exclusions
  where rn = 1
),
combined_population_min_time_n as (
   select condition_concept_id,count(distinct person_id) as n  from combined_population_min_time  group by  1 )
 select case when n < 5 then -5 else n end as n,
  n.condition_concept_id,
  c.concept_name,
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
    on combined_population_min_time.condition_concept_id = n.condition_concept_id
join @cdm_database_schema.concept c on c.concept_id = combined_population_min_time.condition_concept_id
 group by  1, n.condition_concept_id, c.concept_name ;