# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

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

# MAGIC %md
# MAGIC #### When the json data is stored as a line-by-line object rather than in a single line, we need to specify its a multiline. Default is set to False.

# COMMAND ----------

# pitstop_df = spark.read \
#             .format('json') \
#             .option('multiline', True) \
#             .load('dbfs:/mnt/formula1racingstoragedl/raw/pit_stops.json')

# display(pitstop_df.head(5))

# COMMAND ----------

pitstop_df = spark.read \
            .format('json') \
            .option('multiline', True) \
            .load(f'{raw_folder}/{v_file_date}/pit_stops.json')

display(pitstop_df.head(5))

# COMMAND ----------

pitstop_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pitstop_df = spark.read \
            .format('json') \
            .option('multiline', True) \
            .schema(pit_stops_schema) \
            .load(f'{raw_folder}/{v_file_date}/pit_stops.json')

display(pitstop_df.head(5))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pitstop_df = pitstop_df.withColumnRenamed("driverId", "driver_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumn('data_source', lit(v_data_source)) \
                        .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

pitstop_df = add_ingestion_datetime(pitstop_df)

# COMMAND ----------

# pitstop_df.write.mode("overwrite").parquet(f"{processed_folder}/pitstops_cleaned")

# COMMAND ----------

# pitstop_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.pitstops_cleaned')

# COMMAND ----------

# overwrite_partition(pitstop_df, 'f1_processed', 'pitstops_cleaned', 'race_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overwrite using delta tables

# COMMAND ----------

merge_condition = "targetTable.race_id = sourceTable.race_id AND targetTable.driver_id = sourceTable.driver_id AND targetTable.stop = SourceTable.stop AND targetTable.race_id = sourceTable.race_id"
merge_delta_data(pitstop_df, 'f1_processed', 'pitstops_cleaned', processed_folder, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.pitstops_cleaned;

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) 
# MAGIC from f1_processed.pitstops_cleaned 
# MAGIC group by race_id 
# MAGIC order by race_id desc;

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder}/pitstops_cleaned"))

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

