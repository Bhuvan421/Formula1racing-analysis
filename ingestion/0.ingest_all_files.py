# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run('1.ingest_circuits_file', 0, {'p_source_data' : 'Ergast API'})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_races_file", 0, {"p_source_data": "Ergast API"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest_constructors_file", 0, {"p_source_data": "Ergast API"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_drivers_file", 0, {"p_source_data": "Ergast API"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingest_results_file", 0, {"p_source_data": "Ergast API"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_pitstops_file", 0, {"p_source_data": "Ergast API"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest_lap_times_multiple_files", 0, {"p_source_data": "Ergast API"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingest_qualifying_multiple_files", 0, {"p_source_data": "Ergast API"})
v_result

# COMMAND ----------

