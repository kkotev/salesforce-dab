from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Gold catalog/schema come from the pipeline configuration (bundle variables gold_catalog / gold_schema)
GOLD = f"{spark.conf.get('gold_catalog')}.{spark.conf.get('gold_schema')}"


@dp.materialized_view(
    name=f"{GOLD}.fact_opportunities",
    comment="Opportunity fact: one row per opportunity with keys to dim_accounts, dim_contacts, dim_date and pipeline measures",
    table_properties={"quality": "gold"},
)
@dp.expect_or_drop("opportunity_id_not_null", "opportunity_id IS NOT NULL")
@dp.expect("has_account", "account_id IS NOT NULL")
@dp.expect("has_close_date", "close_date_key IS NOT NULL")
@dp.expect("non_negative_amount", "amount IS NULL OR amount >= 0")
def fact_opportunities():
    return spark.read.table("silver_opportunities").select(
        # Keys
        F.col("Id").alias("opportunity_id"),
        F.col("account_id"),
        F.col("contact_id"),
        F.col("campaign_id"),
        F.col("owner_id"),
        F.date_format("created_date", "yyyyMMdd").cast("int").alias("created_date_key"),
        F.date_format("close_date", "yyyyMMdd").cast("int").alias("close_date_key"),
        # Degenerate attributes
        F.col("Name").alias("opportunity_name"),
        F.col("stage_name"),
        F.col("type"),
        F.col("lead_source"),
        F.col("forecast_category"),
        F.col("fiscal_year"),
        F.col("fiscal_quarter"),
        F.col("is_closed"),
        F.col("is_won"),
        # Measures
        F.col("amount"),
        F.col("expected_revenue"),
        F.col("probability"),
        F.col("days_to_close"),
        F.when(F.col("is_won"), F.col("amount")).otherwise(F.lit(0)).cast("decimal(18,2)").alias("won_amount"),
    )
