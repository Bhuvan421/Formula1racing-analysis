# Databricks notebook source
# MAGIC %run "../../data_ingestion_configuration_functions/1.data_ingestion_configuration"

# COMMAND ----------

# MAGIC %run "../../data_ingestion_configuration_functions/2.data_ingestion_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

drivers_df = spark.read.format('delta').load(f'{processed_folder}/drivers_cleaned')
drivers_df.printSchema()

# COMMAND ----------

constructors_df = spark.read.format('delta').load(f'{processed_folder}/constructors_cleaned')
constructors_df.printSchema()

# COMMAND ----------

circuits_df = spark.read.format('delta').load(f'{processed_folder}/circuits_cleaned')
circuits_df.printSchema()

# COMMAND ----------

races_df = spark.read.format('delta').load(f'{processed_folder}/races_cleaned')
races_df.printSchema()

# COMMAND ----------

results_df = spark.read.format('delta').load(f'{processed_folder}/results_cleaned')
results_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC There are few columns from multiple tables with same column name, we need to chnage them and add any necessary columns ad join the tables.

# COMMAND ----------

drivers_df = spark.read.format('delta').load(f'{processed_folder}/drivers_cleaned') \
            .withColumnRenamed('number', 'driver_number') \
            .withColumnRenamed('name', 'driver_name') \
            .withColumnRenamed('nationality', 'driver_nationality')
drivers_df.printSchema()

# COMMAND ----------

constructors_df = spark.read.format('delta').load(f'{processed_folder}/constructors_cleaned') \
                .withColumnRenamed('name', 'team')
constructors_df.printSchema()

# COMMAND ----------

circuits_df = spark.read.format('delta').load(f'{processed_folder}/circuits_cleaned') \
            .withColumnRenamed('location', 'circuit_location')
circuits_df.printSchema()

# COMMAND ----------

races_df = spark.read.format('delta').load(f'{processed_folder}/races_cleaned') \
        .withColumnRenamed('circuit_name', 'race_name') \
        .withColumnRenamed('race_timestamp', 'race_date')
races_df.printSchema()

# COMMAND ----------

results_df = spark.read.format('delta').load(f"{processed_folder}/results_cleaned") \
            .filter(f"file_date = '{v_file_date}'") \
            .withColumnRenamed('time', 'race_time') \
            .withColumnRenamed("race_id", "result_race_id") \
            .withColumnRenamed('file_date', 'result_file_date')

results_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Now, we can join the tables:
# MAGIC * We will join races_df with circuits_df as they are connected
# MAGIC * We will then join the drivers_df, constructors_df and results_df with the above joined table

# COMMAND ----------

race_circuit_df = races_df.join(circuits_df, races_df['circuit_id'] == circuits_df['circuit_id'], 'inner') \
                .select(races_df['race_id'], races_df['race_year'], races_df['race_name'], races_df['race_date'], circuits_df['circuit_location'])
display(race_circuit_df.head(3))

# COMMAND ----------

race_results_df = results_df.join(race_circuit_df, results_df['result_race_id'] == race_circuit_df['race_id'], 'inner') \
                .join(drivers_df, results_df['driver_id'] == drivers_df['driver_id']) \
                .join(constructors_df, results_df['constructor_id'] == constructors_df['constructor_id'])
display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select('race_id', 'race_year', 'race_name','race_date', 'circuit_location', 'driver_name', 'driver_number', 'driver_nationality', 
                                  'team', 'grid', 'fastest_lap', 'race_time', 'points', 'position', 'result_file_date') \
                        .withColumn('created_date', current_timestamp()) \
                        .withColumnRenamed('result_file_date', 'file_date')

display(final_df)

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

# final_df.write.mode('overwrite').parquet(f'{presentation_folder}/race_results')

# COMMAND ----------

# final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.race_results')

# COMMAND ----------

# overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_id')

# COMMAND ----------

merge_condition = "targetTable.race_id = sourceTable.race_id AND targetTable.driver_name = sourceTable.driver_name"
merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_folder, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.race_results
# MAGIC where race_year = 2021;

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) 
# MAGIC from f1_presentation.race_results
# MAGIC group by race_id 
# MAGIC order by race_id desc;;