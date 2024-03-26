-- Databricks notebook source
select * from f1_presentation.calculated_race_results;

-- COMMAND ----------

select 
      driver_name, 
      count(1) as total_races, 
      sum(calculated_points) as total_races, 
      avg(calculated_points) as avg_points 
from f1_presentation.calculated_race_results 
group by driver_name 
having count(1) >= 50 
order by avg_points desc; 

-- COMMAND ----------

select 
      driver_name, 
      count(1) as total_races, 
      sum(calculated_points) as total_races, 
      avg(calculated_points) as avg_points 
from f1_presentation.calculated_race_results 
where race_year between 2005 and 2020 
group by driver_name 
having count(1) >= 50 
order by avg_points desc; 