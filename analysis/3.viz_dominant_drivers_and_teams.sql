-- Databricks notebook source
-- MAGIC %python
-- MAGIC title = """<h1 style = "color:Black; text-align:center; font-family:Ariel">Report on Dominant Drivers and Teams</h1>"""
-- MAGIC displayHTML(title)

-- COMMAND ----------

select * from f1_presentation.calculated_race_results;

-- COMMAND ----------

select 
      driver_name, 
      count(1) as total_races, 
      sum(calculated_points) as total_points, 
      avg(calculated_points) as avg_points, 
      rank() over(order by avg(calculated_points) desc) driver_ranking
from f1_presentation.calculated_race_results 
group by driver_name 
having count(1) >= 50 
order by avg_points desc;

-- COMMAND ----------

create or replace temp view v_dominant_drivers 
as 
select 
      driver_name, 
      count(1) as total_races, 
      sum(calculated_points) as total_points, 
      avg(calculated_points) as avg_points, 
      rank() over(order by avg(calculated_points) desc) driver_ranking
from f1_presentation.calculated_race_results 
group by driver_name 
having count(1) >= 50 
order by avg_points desc; 

-- COMMAND ----------

select 
      race_year, 
      driver_name, 
      count(1) as total_races, 
      sum(calculated_points) as total_points, 
      avg(calculated_points) as avg_points 
from f1_presentation.calculated_race_results 
where driver_name in (select driver_name from v_dominant_drivers where driver_ranking <= 10)
group by race_year, driver_name 
order by avg_points desc; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Team Viz

-- COMMAND ----------

create or replace temp view v_dominant_teams 
as 
select 
      team_name, 
      count(1) as total_races, 
      sum(calculated_points) as total_points, 
      avg(calculated_points) as avg_points, 
      rank() over(order by avg(calculated_points) desc) team_ranking
from f1_presentation.calculated_race_results 
group by team_name 
having count(1) >= 100 
order by avg_points desc; 

-- COMMAND ----------

select 
      race_year, 
      team_name, 
      count(1) as total_races, 
      sum(calculated_points) as total_points, 
      avg(calculated_points) as avg_points 
from f1_presentation.calculated_race_results 
where team_name in (select team_name from v_dominant_teams where team_ranking <= 5)
group by race_year, team_name 
order by avg_points desc; 