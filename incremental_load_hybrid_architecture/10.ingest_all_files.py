# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run('1.ingest_circuits_file', 0, {'p_source_data' : 'Ergast API', 'p_file_date' : '2021-04-18'})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_races_file", 0, {"p_source_data": "Ergast API", 'p_file_date' : '2021-03-21'})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest_constructors_file", 0, {"p_source_data": "Ergast API", 'p_file_date' : '2021-03-21'})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_drivers_file", 0, {"p_source_data": "Ergast API", 'p_file_date' : '2021-03-21'})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingest_results_file", 0, {"p_source_data": "Ergast API", 'p_file_date' : '2021-03-21'})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_pitstops_file", 0, {"p_source_data": "Ergast API", 'p_file_date' : '2021-03-21'})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest_lap_times_multiple_files", 0, {"p_source_data": "Ergast API", 'p_file_date' : '2021-03-21'})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingest_qualifying_multiple_files", 0, {"p_source_data": "Ergast API", 'p_file_date' : '2021-03-21'})
v_result

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) 
# MAGIC from f1_processed.results_cleaned 
# MAGIC group by race_id 
# MAGIC order by race_id desc;

# COMMAND ----------

