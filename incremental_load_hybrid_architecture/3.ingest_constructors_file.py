# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text('p_source_data', '')
v_source_data = dbutils.widgets.get('p_source_data')

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

# constructors_df = spark.read \
#                 .format('json') \
#                 .load('dbfs:/mnt/formula1racingstoragedl/raw/constructors.json')

# display(constructors_df.head(5))

# COMMAND ----------

constructors_df = spark.read \
                .format('json') \
                .load(f'{raw_folder}/{v_file_date}/constructors.json')

display(constructors_df.head(5))

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

constructors_schema = 'constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING'

# COMMAND ----------

constructors_df = spark.read \
                .format('json') \
                .schema(constructors_schema) \
                .load(f'{raw_folder}/{v_file_date}/constructors.json')

display(constructors_df.head(5))

# COMMAND ----------

constructors_df = constructors_df.drop('url')   # here, you could use all the different methods used in ".select" for ".drop" as well, such as: 
                                                # .drop(col('<col_name>')) or .drop(<df_name>['<col_name>'])

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# constructors_df = constructors_df.withColumnRenamed('constructorId', 'constructor_id') \
#                                 .withColumnRenamed('constructorRef', 'constructor_ref') \
#                                 .withColumn('ingestion_date', current_timestamp()) \
#                                 .withColumn('data_source', lit(v_source_data))

# display(constructors_df.head(5))

# COMMAND ----------

constructors_df = constructors_df.withColumnRenamed('constructorId', 'constructor_id') \
                                .withColumnRenamed('constructorRef', 'constructor_ref') \
                                .withColumn('data_source', lit(v_source_data)) \
                                .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

constructors_df = add_ingestion_datetime(constructors_df)

display(constructors_df.head(5))

# COMMAND ----------

# constructors_df.write.mode('overwrite').parquet(f'{processed_folder}/constructors_cleaned')

# COMMAND ----------

constructors_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.constructors_cleaned')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors_cleaned;

# COMMAND ----------

display(dbutils.fs.ls(f'{processed_folder}/constructors_cleaned'))

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

