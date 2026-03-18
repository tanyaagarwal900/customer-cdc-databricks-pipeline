from pyspark.sql.functions import *

def run_bronze_pipeline():
    bronze_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/schema/customer")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("/mnt/raw/customer_updates/")
    )

    bronze_df = bronze_df.withColumn("ingestion_ts", current_timestamp())

    (bronze_df.writeStream
     .format("delta")
     .option("checkpointLocation", "/mnt/checkpoints/customer_bronze")
     .outputMode("append")
     .table("bronze_customer_events"))
