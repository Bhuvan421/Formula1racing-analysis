# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text('p_source_data', '')
v_source_data = dbutils.widgets.get('p_source_data')

# COMMAND ----------

# MAGIC %run "../data_ingestion_configuration_functions/1.data_ingestion_configuration"

# COMMAND ----------

# MAGIC %run "../data_ingestion_configuration_functions/2.data_ingestion_functions"

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/mnt/'))

# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula1racingstoragedl/'))

# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula1racingstoragedl/raw/'))

# COMMAND ----------

# results_df = spark.read \
#         .format('json') \
#         .load('dbfs:/mnt/formula1racingstoragedl/raw/results.json')

# display(results_df.head(5))

# COMMAND ----------

results_df = spark.read \
        .format('json') \
        .load(f'{raw_folder}/results.json')

display(results_df.head(5))

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

# COMMAND ----------

results_df = spark.read \
        .format('json') \
        .schema(results_schema) \
        .load(f'{raw_folder}/results.json')

display(results_df.head(5))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_df = results_df.withColumnRenamed("resultId", "result_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumnRenamed("driverId", "driver_id") \
                        .withColumnRenamed("constructorId", "constructor_id") \
                        .withColumnRenamed("positionText", "position_text") \
                        .withColumnRenamed("positionOrder", "position_order") \
                        .withColumnRenamed("fastestLap", "fastest_lap") \
                        .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                        .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                        .withColumn('data_source', lit(v_source_data)) 

# COMMAND ----------

results_df = add_ingestion_datetime(results_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_df = results_df.drop(col("statusId"))

# COMMAND ----------

display(results_df.head(5))

# COMMAND ----------

# results_df.write.mode('overwrite').partitionBy('race_id').parquet(f'{processed_folder}/results_cleaned')

# COMMAND ----------

results_df.write.mode('overwrite').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results_cleaned')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.results_cleaned;

# COMMAND ----------

display(dbutils.fs.ls(f'{processed_folder}/results_cleaned'))

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

