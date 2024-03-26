# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_datetime(dataframe):
    return dataframe.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

def rearrange_partition_column(input_df, partition_col):
    column_list = []
    for column_name in input_df.schema.names:
        if column_name != partition_col:
            column_list.append(column_name)
    column_list.append(partition_col)
    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

def overwrite_partition(df, db_name, table_name, partition_col):
    results_df = rearrange_partition_column(df, partition_col)
    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    if (spark._jsparkSession.catalog().tableExists(f'{db_name}.{table_name}')):
        results_df.write.mode('overwrite').insertInto(f'{db_name}.{table_name}')
    else:
        results_df.write.mode('overwrite').partitionBy(partition_col).format('parquet').saveAsTable(f'{db_name}.{table_name}')

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_col):
    from delta.tables import DeltaTable
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

    if (spark._jsparkSession.catalog().tableExists(f'{db_name}.{table_name}')):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias('targetTable').merge(input_df.alias('sourceTable'), merge_condition) \
                                    .whenMatchedUpdateAll() \
                                    .whenNotMatchedInsertAll() \
                                    .execute()
    else:
        input_df.write.mode('overwrite').partitionBy(partition_col).format('delta').saveAsTable(f'{db_name}.{table_name}')