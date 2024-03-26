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

# drivers_df = spark.read \
#             .format('json') \
#             .load('dbfs:/mnt/formula1racingstoragedl/raw/drivers.json')

# display(drivers_df.head(5))

# COMMAND ----------

drivers_df = spark.read \
            .format('json') \
            .load(f'{raw_folder}/drivers.json')

display(drivers_df.head(5))

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### The name column is hierarchy data, we need to define the schema for this column.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_col_schema = StructType(fields=[StructField('forename', StringType(), True), 
                                     StructField('surname', StringType(), True)])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_col_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

# COMMAND ----------

drivers_df = spark.read \
            .format('json') \
            .schema(drivers_schema) \
            .load(f'{raw_folder}/drivers.json')

display(drivers_df.head(5))

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

# drivers_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
#                         .withColumnRenamed("driverRef", "driver_ref") \
#                         .withColumn("ingestion_date", current_timestamp()) \
#                         .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

drivers_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                        .withColumnRenamed("driverRef", "driver_ref") \
                        .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                        .withColumn('data_source', lit(v_source_data))

# COMMAND ----------

drivers_df = add_ingestion_datetime(drivers_df)

# COMMAND ----------

display(drivers_df.head(5))

# COMMAND ----------

drivers_df = drivers_df.drop(col("url"))

# COMMAND ----------

# drivers_df.write.mode("overwrite").parquet(f"{processed_folder}/drivers_cleaned")

# COMMAND ----------

drivers_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.drivers_cleaned')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.drivers_cleaned;

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder}/drivers_cleaned"))

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

