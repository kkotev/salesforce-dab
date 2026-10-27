from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Gold catalog/schema come from the pipeline configuration (bundle variables gold_catalog / gold_schema)
GOLD = f"{spark.conf.get('gold_catalog')}.{spark.conf.get('gold_schema')}"


@dp.materialized_view(
    name=f"{GOLD}.dim_accounts",
    comment="Account dimension: one row per Salesforce account with descriptive attributes",
    table_properties={"quality": "gold"},
)
@dp.expect_or_drop("account_id_not_null", "account_id IS NOT NULL")
@dp.expect("account_name_not_null", "account_name IS NOT NULL")
def dim_accounts():
    return spark.read.table("silver_accounts").select(
        F.col("Id").alias("account_id"),
        F.col("Name").alias("account_name"),
        F.col("account_number"),
        F.col("Type").alias("account_type"),
        F.col("Industry").alias("industry"),
        F.col("Rating").alias("rating"),
        F.col("Ownership").alias("ownership"),
        F.col("annual_revenue"),
        F.col("number_of_employees"),
        F.col("number_of_locations"),
        F.col("billing_street"),
        F.col("billing_city"),
        F.col("billing_state"),
        F.col("billing_postal_code"),
        F.col("billing_country_code"),
        F.col("Phone").alias("phone"),
        F.col("Website").alias("website"),
        F.col("customer_priority"),
        F.col("sla"),
        F.col("sla_expiration_date"),
        F.col("is_active"),
        F.col("upsell_opportunity"),
        F.col("owner_id"),
        F.col("created_date"),
        F.col("last_modified_date"),
    )
