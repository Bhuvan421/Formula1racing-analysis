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

display(dbutils.fs.ls('/mnt'))

# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula1racingstoragedl'))

# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula1racingstoragedl/raw/'))

# COMMAND ----------

# Here we have our races data.

races_df = spark.read \
        .format('csv') \
        .load(f'{raw_folder}/races.csv')

display(races_df.head(5))

# COMMAND ----------

# races_df = spark.read \
#         .format('csv') \
#         .option('header', True) \
#         .load('dbfs:/mnt/formula1racingstoragedl/raw/races.csv')

# display(races_df.head(5))

# COMMAND ----------

races_df = spark.read \
        .format('csv') \
        .option('header', True) \
        .load(f'{raw_folder}/races.csv')

display(races_df.head(5))

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields = [StructField('raceId', IntegerType(), False), 
                                    StructField("year", IntegerType(), True),
                                    StructField("round", IntegerType(), True),
                                    StructField("circuitId", IntegerType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("date", DateType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("url", StringType(), True) 
])

# COMMAND ----------

races_df = spark.read \
        .format('csv') \
        .option('header', True) \
        .schema(races_schema) \
        .load(f'{raw_folder}/races.csv')

display(races_df.head(5))

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add ingestion data and race_timestamps (concat of date and time col) and select only necessary columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

# races_df = races_df.withColumn('ingestion_date', current_timestamp()) \
#                     .withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
#                     .withColumn('data_source', lit(v_source_data))

# COMMAND ----------

races_df = races_df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
                    .withColumn('data_source', lit(v_source_data))

# COMMAND ----------

races_df = add_ingestion_datetime(races_df)

# COMMAND ----------

display(races_df.head(5))

# COMMAND ----------

races_df = races_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), col('round'), col('circuitId').alias('circuit_id'), 
                           col('name').alias('circuit_name'), col('ingestion_date'), col('race_timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write the cleaned data using Pyspark

# COMMAND ----------

# races_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/formula1racingstoragedl/processed/races_cleaned')

# COMMAND ----------

# races_df.write.mode('overwrite').partitionBy('race_year').parquet(f'{processed_folder}/races_cleaned')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write the cleaned data using Spark SQL

# COMMAND ----------

races_df.write.mode('overwrite').partitionBy('race_year').format('parquet').saveAsTable('f1_processed.races_cleaned')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races_cleaned

# COMMAND ----------

display(dbutils.fs.ls(f'{processed_folder}/races_cleaned'))

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

