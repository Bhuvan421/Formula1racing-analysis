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

# results_df = spark.read \
#         .format('json') \
#         .load('dbfs:/mnt/formula1racingstoragedl/raw/results.json')

# display(results_df.head(5))

# COMMAND ----------

results_df = spark.read \
        .format('json') \
        .load(f'{raw_folder}/{v_file_date}/results.json')

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
        .load(f'{raw_folder}/{v_file_date}/results.json')

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
                        .withColumn('data_source', lit(v_source_data)) \
                        .withColumn('file_date', lit(v_file_date))

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

# MAGIC %md
# MAGIC ### Drop duplicates

# COMMAND ----------

results_deduped_df = results_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental writing/pushing data into data lake.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 1:

# COMMAND ----------

# for race_id_list in results_df.select("race_id").distinct().collect():
#   if (spark._jsparkSession.catalog().tableExists('f1_processed.results_cleaned')):
#     spark.sql(f'Alter Table f1_processed.results_cleaned drop if exists partition (race_id = {race_id_list.race_id})')

# COMMAND ----------

# results_df.write.mode('append').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results_cleaned')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 2: -- fatser than method 1
# MAGIC
# MAGIC * Using this method, for the first time, drop the existing data if the data existed is something different from what you want now, and comment it out. From the next run, you donot need it as the if-else statement takes care if the data already exist.
# MAGIC * Using this method requires your input data's `last column` to be the partition column and the remaining columns to be in same order. Therefore, always make sure the order of columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists f1_processed.results_cleaned;

# COMMAND ----------

spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

# COMMAND ----------

results_df.schema.names

# COMMAND ----------

results_df = results_df.select('result_id',
 'driver_id',
 'constructor_id',
 'number',
 'grid',
 'position',
 'position_text',
 'position_order',
 'points',
 'laps',
 'time',
 'milliseconds',
 'fastest_lap',
 'rank',
 'fastest_lap_time',
 'fastest_lap_speed',
 'data_source',
 'file_date',
 'ingestion_date', 
 'race_id')

# COMMAND ----------

# if (spark._jsparkSession.catalog().tableExists('f1_processed.results_cleaned')):
#     results_df.write.mode('overwrite').insertInto('f1_processed.results_cleaned')
# else:
#     results_df.write.mode('overwrite').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results_cleaned')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 2 -- Improved version
# MAGIC
# MAGIC The same code and logic of the above, is now put inside functions and called.

# COMMAND ----------

# def rearrange_partition_column(input_df, partition_col):
#     column_list = []
#     for column_name in input_df.schema.names:
#         if column_name != partition_col:
#             column_list.append(column_name)
#     column_list.append(partition_col)
#     output_df = input_df.select(column_list)
#     return output_df

# COMMAND ----------

# def overwrite_partition(df, db_name, table_name, partition_col):
#     results_df = rearrange_partition_column(df, partition_col)
#     spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
#     if (spark._jsparkSession.catalog().tableExists(f'{db_name}.{table_name}')):
#         results_df.write.mode('overwrite').insertInto(f'{db_name}.{table_name}')
#     else:
#         results_df.write.mode('overwrite').partitionBy(partition_col).format('parquet').saveAsTable(f'{db_name}.{table_name}')

# COMMAND ----------

# overwrite_partition(results_df, 'f1_processed', 'results_cleaned', 'race_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overwrite using Delta format

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

# if (spark._jsparkSession.catalog().tableExists('f1_processed.results_cleaned')):
#     deltaTable = DeltaTable.forPath(spark, f"{processed_folder}/results_cleaned")
#     deltaTable.alias('targetTable').merge(results_df.alias('sourceTable'), 
#                                           'targetTable.result_id == sourceTable.result_id AND targetTable.race_id == sourceTable.race_id') \
#               .whenMatchedUpdateAll() \
#               .whenNotMatchedInsertAll() \
#               .execute()
# else:
#     results_df.write.mode('overwrite').partitionBy('race_id').format('delta').saveAsTable('f1_processed.results_cleaned')

# COMMAND ----------

merge_condition = 'targetTable.result_id == sourceTable.result_id AND targetTable.race_id == sourceTable.race_id'
merge_delta_data(results_deduped_df, 'f1_processed', 'results_cleaned', processed_folder, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.results_cleaned;

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) 
# MAGIC from f1_processed.results_cleaned 
# MAGIC group by race_id 
# MAGIC order by race_id desc;

# COMMAND ----------

display(dbutils.fs.ls(f'{processed_folder}/results_cleaned'))

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Duplicates analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) 
# MAGIC from f1_processed.results_cleaned 
# MAGIC where file_date == '2021-03-21'

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, driver_id, count(1) as no_of_races_per_driver_in_a_single_race  
# MAGIC from f1_processed.results_cleaned 
# MAGIC group by race_id, driver_id 
# MAGIC having no_of_races_per_driver_in_a_single_race > 1
# MAGIC order by no_of_races_per_driver_in_a_single_race desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.results_cleaned 
# MAGIC where race_id = 800 and driver_id = 612;

# COMMAND ----------

