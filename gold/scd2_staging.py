from pyspark.sql.functions import *

def prepare_scd2_staging():
    silver_df = spark.read.table("silver_customer_events") \
    .orderBy("customer_id", "update_ts")

    current_dim = spark.table("dim_customer").filter("is_current = true")

    joined_df = silver_df.alias("s").join(
        current_dim.alias("d"),
        "customer_id",
        "left"
    )
    
    new_records = joined_df.filter(
    col("d.customer_id").isNull()
)
    
    changed_records = joined_df.filter(
        col("d.customer_id").isNotNull() & (
            (col("s.city") != col("d.city")) |
            (col("s.email") != col("d.email")) |
            (col("s.status") != col("d.status"))|
            (col("s.name") != col("d.name"))
        )
    )
 
    expire_records = changed_records.select(
        col("d.customer_sk"),
        col("d.customer_id"),
        col("d.name"),
        col("d.email"),
        col("d.city"),
        col("d.status"),
        col("d.start_date"),
        expr("s.update_ts - INTERVAL 1 SECOND").alias("end_date"),
        lit(False).alias("is_current")
    )

    valid_inserts = new_records.union(changed_records)
    
    insert_records = valid_inserts.select(
    col("s.customer_id"),
    col("s.name"),
    col("s.email"),
    col("s.city"),
    col("s.status"),
    col("s.update_ts").alias("start_date"),
    lit(None).cast("timestamp").alias("end_date"),
    lit(True).alias("is_current")
)

    staged_updates = expire_records.union(insert_records)

    staged_updates.createOrReplaceTempView("staged_updates")
