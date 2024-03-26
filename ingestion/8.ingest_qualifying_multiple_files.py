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
# MAGIC Looks like lap_times is not a file but a folder, we can go further into the path to check where the file/files exists.

# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula1racingstoragedl/raw/qualifying/'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2 different ways of reading data from multiple files: 

# COMMAND ----------

# qualifying_df = spark.read \
#         .format('json') \
#         .option('multiLine', True) \
#         .load('/mnt/formula1racingstoragedl/raw/qualifying/')

# display(qualifying_df.head(5))

# COMMAND ----------

qualifying_df = spark.read \
        .format('json') \
        .option('multiLine', True) \
        .load(f'{raw_folder}/qualifying/')

display(qualifying_df.head(5))

# COMMAND ----------

print("Number of rows: ", qualifying_df.count())
print("Number of columns: ", len(qualifying_df.columns))

qualifying_df.printSchema()

# COMMAND ----------

qualifying_df = spark.read \
        .format('json') \
        .option('multiLine', True) \
        .load(f'{raw_folder}/qualifying/qualifying_split*.json')

display(qualifying_df.head(5))

# COMMAND ----------

print("Number of rows: ", qualifying_df.count())
print("Number of columns: ", len(qualifying_df.columns))

qualifying_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

qualifying_df = spark.read \
        .format('json') \
        .option('multiLine', True) \
        .schema(qualifying_schema) \
        .load(f'{raw_folder}/qualifying/qualifying_split*.json')

display(qualifying_df.head(5))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualifying_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                .withColumnRenamed("driverId", "driver_id") \
                .withColumnRenamed("raceId", "race_id") \
                .withColumnRenamed("constructorId", "constructor_id") \
                .withColumn('data_source', lit(v_data_source))

# COMMAND ----------

qualifying_df = add_ingestion_datetime(qualifying_df)

# COMMAND ----------

# qualifying_df.write.mode("overwrite").parquet(f'{processed_folder}/qualifying_cleaned')

# COMMAND ----------

qualifying_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.qualifying_cleaned')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.qualifying_cleaned;

# COMMAND ----------

display(dbutils.fs.ls(f'{processed_folder}/qualifying_cleaned'))

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

