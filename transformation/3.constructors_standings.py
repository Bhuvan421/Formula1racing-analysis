# Databricks notebook source
# MAGIC %run "../data_ingestion_configuration_functions/1.data_ingestion_configuration"

# COMMAND ----------

# MAGIC %run "../data_ingestion_configuration_functions/2.data_ingestion_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder}/race_results')
display(race_results_df)

# COMMAND ----------

race_years = spark.read.parquet(f'{presentation_folder}/race_results') \
                    .filter(f"file_date == '{v_file_date}'") \
                    .select('race_year') \
                    .distinct() \
                    .collect()

race_years

# COMMAND ----------

race_year_list = []
for year in race_years:
    race_year_list.append(year.race_year)

print(race_year_list)

# COMMAND ----------

from pyspark.sql.functions import col
race_results_df = spark.read.parquet(f'{presentation_folder}/race_results') \
                    .filter(col('race_year').isin(race_year_list))

display(race_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Lets get:
# MAGIC 1. race_year
# MAGIC 2. driver_name
# MAGIC 3. driver_nationality
# MAGIC 4. team
# MAGIC 5. total_points by each driver by year
# MAGIC 6. total_wins by each driver
# MAGIC 7. orderBy the data on wins

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count, desc

# COMMAND ----------

df = race_results_df.groupBy('race_year', 'team') \
                    .agg(sum('points').alias('total_points'), \
                        count(when(col('position') == 1, True)).alias('wins')) \
                    .orderBy(desc('race_year'), desc('total_points'), desc('wins'))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Now, lets add a column 'rankings' based on the total_points earned by each driver.

# COMMAND ----------

from pyspark.sql.functions import rank
from pyspark.sql.window import Window

# COMMAND ----------

year_window = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
final_df = df.withColumn('rankings', rank().over(year_window))
display(final_df)

# COMMAND ----------

display(final_df.filter("race_year == 2020"))

# COMMAND ----------

# final_df.write.mode('overwrite').parquet(f'{presentation_folder}/constructors_standings')

# COMMAND ----------

# final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.constructors_standings')

# COMMAND ----------

overwrite_partition(final_df, 'f1_presentation', 'constructors_standings', 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.constructors_standings;

# COMMAND ----------

display(dbutils.fs.ls(f'{presentation_folder}/constructors_standings'))

# COMMAND ----------

