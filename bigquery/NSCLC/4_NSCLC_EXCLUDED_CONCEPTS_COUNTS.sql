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
   select person_id, condition_concept_id,
         min(condition_start_date) as condition_start_date,
         count(*) as n_conditions
   from @cdm_database_schema.condition_occurrence co
  inner join excluded_concepts cs 
    on co.condition_concept_id = cs.concept_id
  where co.person_id in (select person_id from combined_population)
   group by  1, 2 )
   select c.concept_name, 
       case when count(distinct d.person_id) < 5 then -5 else count(distinct d.person_id) end as excluded_patient_count,
       case when count(distinct d.person_id) < 5 then null else count(distinct case when d.n_conditions > 1 then d.person_id end) end as excluded_patient_count_multiple,
       case when count(distinct d.person_id) < 5 then null else count(distinct case when d.n_conditions > 2 then d.person_id end) end as excluded_patient_count_multiple_2,
       case when count(distinct d.person_id) < 5 then null else count(distinct case when d.n_conditions > 3 then d.person_id end) end as excluded_patient_count_multiple_3
   from diags_to_exclude d
join   @cdm_database_schema.concept c on c.concept_id = d.condition_concept_id
  group by  d.condition_concept_id, c.concept_name
   order by  excluded_patient_count desc ;