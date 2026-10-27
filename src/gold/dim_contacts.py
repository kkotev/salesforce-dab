from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Gold catalog/schema come from the pipeline configuration (bundle variables gold_catalog / gold_schema)
GOLD = f"{spark.conf.get('gold_catalog')}.{spark.conf.get('gold_schema')}"


@dp.materialized_view(
    name=f"{GOLD}.dim_contacts",
    comment="Contact dimension: one row per Salesforce contact, enriched with account name and industry",
    table_properties={"quality": "gold"},
)
@dp.expect_or_drop("contact_id_not_null", "contact_id IS NOT NULL")
@dp.expect("contact_has_account", "account_id IS NOT NULL")
def dim_contacts():
    contacts = spark.read.table("silver_contacts").alias("c")
    accounts = spark.read.table("silver_accounts").alias("a")
    return contacts.join(accounts, F.col("c.account_id") == F.col("a.Id"), "left").select(
        F.col("c.Id").alias("contact_id"),
        F.col("c.account_id"),
        F.col("a.Name").alias("account_name"),
        F.col("a.Industry").alias("account_industry"),
        F.col("c.full_name"),
        F.col("c.title"),
        F.col("c.department"),
        F.col("c.level"),
        F.col("c.email"),
        F.col("c.phone"),
        F.col("c.mobile_phone"),
        F.col("c.lead_source"),
        F.col("c.languages"),
        F.col("c.birthdate"),
        F.col("c.mailing_city"),
        F.col("c.mailing_state"),
        F.col("c.mailing_postal_code"),
        F.col("c.mailing_country_code"),
        F.col("c.is_email_bounced"),
        F.col("c.owner_id"),
        F.col("c.created_date"),
        F.col("c.last_modified_date"),
    )
