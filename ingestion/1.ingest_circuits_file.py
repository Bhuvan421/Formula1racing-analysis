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

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

# display(dbutils.fs.mounts())

# COMMAND ----------

# circuits_df = spark.read.csv('dbfs:/mnt/formula1racingstoragedl/raw/circuits.csv')

# COMMAND ----------

circuits_df = spark.read.csv(f'{raw_folder}/circuits.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC Another way of loading:

# COMMAND ----------

# circuits_df = spark.read \
#             .format('csv') \
#             .load('dbfs:/mnt/formula1racingstoragedl/raw/circuits.csv')

# COMMAND ----------

circuits_df = spark.read \
            .format('csv') \
            .load(f'{raw_folder}/circuits.csv')

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

circuits_df.show(truncate=False)   # gives the full value of entry in a column

# COMMAND ----------

display(circuits_df)  # header is given as data entry.

# COMMAND ----------

# circuits_df = spark.read \
#             .option('header', True) \
#             .csv('dbfs:/mnt/formula1racingstoragedl/raw/circuits.csv')


# COMMAND ----------

circuits_df = spark.read \
            .option('header', True) \
            .csv(f'{raw_folder}/circuits.csv')


# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# circuits_df = spark.read \
#             .format('csv') \
#             .option('header', True) \
#             .option('inferSchema', True) \
#             .load('dbfs:/mnt/formula1racingstoragedl/raw/circuits.csv')

# COMMAND ----------

circuits_df = spark.read \
            .format('csv') \
            .option('header', True) \
            .option('inferSchema', True) \
            .load(f'{raw_folder}/circuits.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC Using inferSchema uses an extra spark job which might be expensive while dealing with larger data. It is preferred, you yourself create the schema and attach the schema to the table while reading the data.

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Lets create a table schema with our preferred datatypes for each column.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

# circuits_df = spark.read \
#             .format('csv') \
#             .option('header', True) \
#             .schema(circuits_schema) \
#             .load('/mnt/formula1racingstoragedl/raw/circuits.csv')

# COMMAND ----------

circuits_df = spark.read \
            .format('csv') \
            .option('header', True) \
            .schema(circuits_schema) \
            .load(f'{raw_folder}/circuits.csv')

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select only necessary columns:
# MAGIC There are 4 ways of selecting the columns,
# MAGIC * Except the first select way (directly giving the column names inside select), all the other 3 ways can be used extensively to apply other column level functions such as changing the name if column, etc.
# MAGIC * But using the first way is simple and easy and clean. Use this if only for selecting columns and not applying any functions on them.

# COMMAND ----------

circuits_df = circuits_df.select('circuitId', 'circuitRef', 'name', 'location', 'country', 'lat', 'lng', 'alt')

# COMMAND ----------

circuits_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, 
                                 circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)

# COMMAND ----------

circuits_df = circuits_df.select(circuits_df['circuitId'], circuits_df['circuitRef'], circuits_df['name'], 
                                 circuits_df['location'], circuits_df['country'], circuits_df['lat'], circuits_df['lng'], circuits_df['alt'])

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_df = circuits_df.select(col('circuitId'), col('circuitRef'), col('name'), 
                                 col('location'), col('country'), col('lat'), col('lng'), col('alt'))

# COMMAND ----------

# MAGIC %md
# MAGIC You can use any of the last 3 ways to for example change a column name:

# COMMAND ----------

circuits_df = circuits_df.select(col('circuitId'), col('circuitRef'), col('name').alias('circuitName'), 
                                 col('location'), col('country'), col('lat'), col('lng'), 
                                 col('alt'))

# COMMAND ----------

# MAGIC %md
# MAGIC Another way of changing the column names:
# MAGIC Question is why this way? 
# MAGIC Because, while you want to select certain columns, you only want to change the column names of few of them. If you use any of the above method, because you are giving the column names inside a ".select", only those columns will be fetched.

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_df = circuits_df.withColumnRenamed('circuitId', 'circuit_id') \
            .withColumnRenamed('circuitRef', 'circuit_ref') \
            .withColumnRenamed('circuitName', 'circuit_name') \
            .withColumnRenamed('lat', 'latitude') \
            .withColumnRenamed('lng', 'longitude') \
            .withColumnRenamed('alt', 'altitude') \
            .withColumn('data_source', lit(v_source_data))

# COMMAND ----------

# MAGIC %md
# MAGIC Limit the number of rows to display:

# COMMAND ----------

display(circuits_df.head(5))

# COMMAND ----------

circuits_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Add a new column:
# MAGIC * If the new column type is predefined column object of PySpark, you can just import and use it.
# MAGIC * But if the column object is not in pySpark, you need to use lit() -- google it

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# circuits_df = circuits_df.withColumn('ingestion_date', current_timestamp())

# display(circuits_df.head(3))

# COMMAND ----------

circuits_df = add_ingestion_datetime(circuits_df)

display(circuits_df.head(3))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write the cleaned data into datalake as a parquet file
# MAGIC
# MAGIC While writing the file back into the datalake, its better to add a method called "mode('overwrite'). This doesnt throw error if you rerun.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using Pyspark

# COMMAND ----------

# circuits_df.write.mode('overwrite').parquet('/mnt/formula1racingstoragedl/processed/circuits_cleaned')

# COMMAND ----------

# circuits_df.write.mode('overwrite').parquet(f'{processed_folder}/circuits_cleaned')

# COMMAND ----------

# df = spark.read \
#     .format('parquet') \
#     .option('header', True) \
#     .load('/mnt/formula1racingstoragedl/processed/circuits_cleaned')

# display(df.head(3))

# COMMAND ----------

# df = spark.read \
#     .format('parquet') \
#     .option('header', True) \
#     .load(f'{processed_folder}/circuits_cleaned')

# display(df.head(3))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing the clean data using spark sql:

# COMMAND ----------

circuits_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.circuits_cleaned')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits_cleaned;

# COMMAND ----------

display(dbutils.fs.ls(f'{processed_folder}/circuits_cleaned'))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

