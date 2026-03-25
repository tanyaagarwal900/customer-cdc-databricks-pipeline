from pyspark.sql.functions import *

def prepare_scd2_staging():
    silver_df = spark.read.table("silver_customer_events")

    current_dim = spark.table("dim_customer").filter("is_current = true")

    joined_df = silver_df.alias("s").join(
        current_dim.alias("d"),
        "customer_id",
        "left"
    )

    change_records = joined_df.filter(
        col("d.customer_id").isNotNull() & (
            (col("s.city") != col("d.city")) |
            (col("s.email") != col("d.email")) |
            (col("s.status") != col("d.status"))
        )
    )

    expire_records = change_records.select(
        col("d.customer_sk"),
        col("d.customer_id"),
        col("d.name"),
        col("d.email"),
        col("d.city"),
        col("d.status"),
        col("d.start_date"),
        col("s.update_ts").alias("end_date"),
        lit(False).alias("is_current")
    )

    insert_records = silver_df.select(
        monotonically_increasing_id().alias("customer_sk"),
        col("customer_id"),
        col("name"),
        col("email"),
        col("city"),
        col("status"),
        col("update_ts").alias("start_date"),
        lit(None).cast("timestamp").alias("end_date"),
        lit(True).alias("is_current")
    )

    staged_updates = expire_records.union(insert_records)

    staged_updates.createOrReplaceTempView("staged_updates")
