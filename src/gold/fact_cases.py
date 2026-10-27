from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Gold catalog/schema come from the pipeline configuration (bundle variables gold_catalog / gold_schema)
GOLD = f"{spark.conf.get('gold_catalog')}.{spark.conf.get('gold_schema')}"


@dp.materialized_view(
    name=f"{GOLD}.fact_cases",
    comment="Support case fact: one row per case with keys to dim_accounts, dim_contacts, dim_date and resolution measures",
    table_properties={"quality": "gold"},
)
@dp.expect_or_drop("case_id_not_null", "case_id IS NOT NULL")
@dp.expect("has_account", "account_id IS NOT NULL")
@dp.expect("closed_case_has_closed_date", "NOT is_closed OR closed_date_key IS NOT NULL")
@dp.expect("non_negative_resolution_days", "resolution_days IS NULL OR resolution_days >= 0")
def fact_cases():
    return spark.read.table("silver_cases").select(
        # Keys
        F.col("Id").alias("case_id"),
        F.col("account_id"),
        F.col("contact_id"),
        F.col("owner_id"),
        F.date_format("created_date", "yyyyMMdd").cast("int").alias("created_date_key"),
        F.date_format("closed_date", "yyyyMMdd").cast("int").alias("closed_date_key"),
        # Degenerate attributes
        F.col("case_number"),
        F.col("status"),
        F.col("priority"),
        F.col("origin"),
        F.col("type"),
        F.col("reason"),
        F.col("product"),
        F.col("sla_violation"),
        F.col("is_closed"),
        F.col("is_escalated"),
        # Measures
        F.col("resolution_days"),
    )
