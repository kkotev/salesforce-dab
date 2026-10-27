from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Gold catalog/schema come from the pipeline configuration (bundle variables gold_catalog / gold_schema)
GOLD = f"{spark.conf.get('gold_catalog')}.{spark.conf.get('gold_schema')}"


@dp.materialized_view(
    name=f"{GOLD}.dim_leads",
    comment="Lead dimension: one row per Salesforce lead with status and conversion tracking",
    table_properties={"quality": "gold"},
)
@dp.expect_or_drop("lead_id_not_null", "lead_id IS NOT NULL")
@dp.expect("converted_lead_has_date", "NOT is_converted OR converted_date IS NOT NULL")
def dim_leads():
    return spark.read.table("silver_leads").select(
        F.col("Id").alias("lead_id"),
        F.col("full_name"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("salutation"),
        F.col("title"),
        F.col("company"),
        F.col("Email").alias("email"),
        F.col("Phone").alias("phone"),
        F.col("mobile_phone"),
        F.col("website"),
        F.col("lead_source"),
        F.col("status"),
        F.col("is_open"),
        F.col("industry"),
        F.col("rating"),
        F.col("annual_revenue"),
        F.col("number_of_employees"),
        F.col("number_of_locations"),
        F.col("city"),
        F.col("state"),
        F.col("postal_code"),
        F.col("country_code"),
        F.col("product_interest"),
        F.col("current_generators"),
        F.col("is_converted"),
        F.col("converted_date"),
        F.datediff(F.col("converted_date"), F.to_date(F.col("created_date"))).alias("days_to_convert"),
        F.col("converted_account_id"),
        F.col("converted_contact_id"),
        F.col("converted_opportunity_id"),
        F.col("owner_id"),
        F.col("created_date"),
        F.col("last_modified_date"),
    )
