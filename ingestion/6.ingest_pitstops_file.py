# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

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
            .load(f'{raw_folder}/pit_stops.json')

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
            .load(f'{raw_folder}/pit_stops.json')

display(pitstop_df.head(5))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pitstop_df = pitstop_df.withColumnRenamed("driverId", "driver_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumn('data_source', lit(v_data_source))

# COMMAND ----------

pitstop_df = add_ingestion_datetime(pitstop_df)

# COMMAND ----------

# pitstop_df.write.mode("overwrite").parquet(f"{processed_folder}/pitstops_cleaned")

# COMMAND ----------

pitstop_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.pitstops_cleaned')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.pitstops_cleaned;

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder}/pitstops_cleaned"))

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

