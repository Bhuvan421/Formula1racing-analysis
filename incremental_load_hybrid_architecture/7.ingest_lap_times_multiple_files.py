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
# MAGIC Looks like lap_times is not a file but a folder, we can go further into the path to check where the file/files exists.

# COMMAND ----------

display(dbutils.fs.ls(f'/mnt/formula1racingstoragedl/raw/{v_file_date}/lap_times/'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2 different ways of reading data from multiple files: 

# COMMAND ----------

# laps_df = spark.read \
#         .format('csv') \
#         .load('/mnt/formula1racingstoragedl/raw/lap_times/')

# display(laps_df.head(5))

# COMMAND ----------

laps_df = spark.read \
        .format('csv') \
        .load(f'{raw_folder}/{v_file_date}/lap_times/')

display(laps_df.head(5))

# COMMAND ----------

print("Number of rows: ", laps_df.count())
print("Number of columns: ", len(laps_df.columns))

laps_df.printSchema()

# COMMAND ----------

laps_df = spark.read \
        .format('csv') \
        .load(f'{raw_folder}/{v_file_date}/lap_times/lap_times_split*.csv')

display(laps_df.head(5))

# COMMAND ----------

print("Number of rows: ", laps_df.count())
print("Number of columns: ", len(laps_df.columns))

laps_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

laps_df = spark.read \
        .format('csv') \
        .schema(lap_times_schema) \
        .load(f'{raw_folder}/{v_file_date}/lap_times/lap_times_split*.csv')

display(laps_df.head(5))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

laps_df = laps_df.withColumnRenamed("driverId", "driver_id") \
        .withColumnRenamed("raceId", "race_id") \
        .withColumn('data_source', lit(v_data_source)) \
        .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

laps_df = add_ingestion_datetime(laps_df)

# COMMAND ----------

# laps_df.write.mode("overwrite").parquet(f"{processed_folder}/lap_times_cleaned")

# COMMAND ----------

# laps_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.lap_times_cleaned')

# COMMAND ----------

overwrite_partition(laps_df, 'f1_processed', 'lap_times_cleaned', 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.lap_times_cleaned;

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) 
# MAGIC from f1_processed.lap_times_cleaned
# MAGIC group by race_id 
# MAGIC order by race_id desc;

# COMMAND ----------

display(dbutils.fs.ls(f'{processed_folder}/lap_times_cleaned'))

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

