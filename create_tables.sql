CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_id STRING,
    name STRING,
    email STRING,
    city STRING,
    status STRING,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN
)
USING DELTA;
