-- Databricks notebook source
use f1_processed;

-- COMMAND ----------

select f1_processed.races_cleaned.race_year, f1_processed.constructors_cleaned.name, f1_processed.drivers_cleaned.name, f1_processed.results_cleaned.position, f1_processed.results_cleaned.points 
from f1_processed.results_cleaned 
join f1_processed.drivers_cleaned on (f1_processed.results_cleaned.driver_id == f1_processed.drivers_cleaned.driver_id) 
join f1_processed.constructors_cleaned on (f1_processed.results_cleaned.constructor_id = f1_processed.constructors_cleaned.constructor_id) 
join f1_processed.races_cleaned on (f1_processed.results_cleaned.race_id = f1_processed.races_cleaned.race_id)

-- COMMAND ----------

select 
      f1_processed.races_cleaned.race_year, 
      f1_processed.constructors_cleaned.name as team_name, 
      f1_processed.drivers_cleaned.name as driver_name, 
      f1_processed.results_cleaned.position, 
      f1_processed.results_cleaned.points, 
      11 - f1_processed.results_cleaned.position as calculated_points  
from f1_processed.results_cleaned 
join f1_processed.drivers_cleaned on (f1_processed.results_cleaned.driver_id == f1_processed.drivers_cleaned.driver_id) 
join f1_processed.constructors_cleaned on (f1_processed.results_cleaned.constructor_id = f1_processed.constructors_cleaned.constructor_id) 
join f1_processed.races_cleaned on (f1_processed.results_cleaned.race_id = f1_processed.races_cleaned.race_id)
where f1_processed.results_cleaned.position <= 10


-- COMMAND ----------

create table f1_presentation.calculated_race_results 
using parquet 
as 
select 
      f1_processed.races_cleaned.race_year, 
      f1_processed.constructors_cleaned.name as team_name, 
      f1_processed.drivers_cleaned.name as driver_name, 
      f1_processed.results_cleaned.position, 
      f1_processed.results_cleaned.points, 
      11 - f1_processed.results_cleaned.position as calculated_points  
from f1_processed.results_cleaned 
join f1_processed.drivers_cleaned on (f1_processed.results_cleaned.driver_id == f1_processed.drivers_cleaned.driver_id) 
join f1_processed.constructors_cleaned on (f1_processed.results_cleaned.constructor_id = f1_processed.constructors_cleaned.constructor_id) 
join f1_processed.races_cleaned on (f1_processed.results_cleaned.race_id = f1_processed.races_cleaned.race_id)
where f1_processed.results_cleaned.position <= 10;


-- COMMAND ----------

select * from f1_presentation.calculated_race_results;

-- COMMAND ----------

