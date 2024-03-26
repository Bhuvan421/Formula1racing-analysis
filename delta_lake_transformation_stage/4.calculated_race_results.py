# Databricks notebook source
dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

spark.sql(f"""
              CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
              (
              race_year INT,
              team_name STRING,
              driver_id INT,
              driver_name STRING,
              race_id INT,
              position INT,
              points INT,
              calculated_points INT,
              created_date TIMESTAMP,
              updated_date TIMESTAMP
              )
              USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
              CREATE OR REPLACE TEMP VIEW race_result_updated
              AS
              SELECT races_cleaned.race_year,
                     constructors_cleaned.name AS team_name,
                     drivers_cleaned.driver_id,
                     drivers_cleaned.name AS driver_name,
                     races_cleaned.race_id,
                     results_cleaned.position,
                     results_cleaned.points,
                     11 - results_cleaned.position AS calculated_points
                FROM f1_processed.results_cleaned 
                JOIN f1_processed.drivers_cleaned ON (results_cleaned.driver_id = drivers_cleaned.driver_id)
                JOIN f1_processed.constructors_cleaned ON (results_cleaned.constructor_id = constructors_cleaned.constructor_id)
                JOIN f1_processed.races_cleaned ON (results_cleaned.race_id = races_cleaned.race_id)
               WHERE results_cleaned.position <= 10
                AND results_cleaned.file_date = '{v_file_date}'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from race_result_updated;

# COMMAND ----------

spark.sql(f"""
              MERGE INTO f1_presentation.calculated_race_results tgt
              USING race_result_updated upd
              ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
              WHEN MATCHED THEN
                UPDATE SET tgt.position = upd.position,
                           tgt.points = upd.points,
                           tgt.calculated_points = upd.calculated_points,
                           tgt.updated_date = current_timestamp
              WHEN NOT MATCHED
                THEN INSERT (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, created_date ) 
                     VALUES (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, current_timestamp)
       """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM race_result_updated;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM f1_presentation.calculated_race_results;

# COMMAND ----------

