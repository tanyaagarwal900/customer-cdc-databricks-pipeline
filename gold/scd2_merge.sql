MERGE INTO dim_customer AS target
USING staged_updates AS source
ON target.customer_id = source.customer_id
AND target.is_current = true
AND target.start_date = source.start_date

WHEN MATCHED AND source.is_current = false
THEN UPDATE SET
target.end_date = source.end_date,
target.is_current = false

WHEN NOT MATCHED
THEN INSERT (
customer_id,
name,
email,
city,
status,
start_date,
end_date,
is_current
)
VALUES (
source.customer_id,
source.name,
source.email,
source.city,
source.status,
source.start_date,
source.end_date,
source.is_current
);
