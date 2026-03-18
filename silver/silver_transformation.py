from pyspark.sql.functions import *
from pyspark.sql.window import Window

def run_silver_pipeline():
    bronze_df = spark.readStream.table("bronze_customer_events")

    silver_df = bronze_df \
        .withColumn("update_ts", to_timestamp("update_ts")) \
        .filter(col("customer_id").isNotNull()) \
        .filter(col("operation").isin("INSERT","UPDATE"))

    window_spec = Window.partitionBy("customer_id","update_ts") \
                        .orderBy(col("ingestion_ts").desc())

    silver_dedup = silver_df.withColumn(
        "rn", row_number().over(window_spec)
    ).filter("rn = 1").drop("rn")

    (silver_dedup.writeStream
     .format("delta")
     .option("checkpointLocation","/mnt/checkpoints/customer_silver")
     .outputMode("append")
     .table("silver_customer_events"))
