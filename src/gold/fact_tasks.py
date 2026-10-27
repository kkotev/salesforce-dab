from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Gold catalog/schema come from the pipeline configuration (bundle variables gold_catalog / gold_schema)
GOLD = f"{spark.conf.get('gold_catalog')}.{spark.conf.get('gold_schema')}"


def _id_if_prefix(col_name, prefix):
    # Salesforce IDs start with a 3-character object key prefix (003 = Contact, 00Q = Lead, 006 = Opportunity, 500 = Case)
    return F.when(F.col(col_name).startswith(prefix), F.col(col_name))


@dp.materialized_view(
    name=f"{GOLD}.fact_tasks",
    comment="Activity fact: one row per Salesforce task with keys to accounts, contacts, leads, opportunities, cases and dim_date",
    table_properties={"quality": "gold"},
)
@dp.expect_or_drop("task_id_not_null", "task_id IS NOT NULL")
@dp.expect("non_negative_call_duration", "call_duration_seconds IS NULL OR call_duration_seconds >= 0")
def fact_tasks():
    return spark.read.table("silver_tasks").select(
        # Keys (polymorphic WhoId / WhatId resolved by ID prefix)
        F.col("Id").alias("task_id"),
        F.col("account_id"),
        _id_if_prefix("who_id", "003").alias("contact_id"),
        _id_if_prefix("who_id", "00Q").alias("lead_id"),
        _id_if_prefix("what_id", "006").alias("opportunity_id"),
        _id_if_prefix("what_id", "500").alias("case_id"),
        F.col("owner_id"),
        F.date_format("created_date", "yyyyMMdd").cast("int").alias("created_date_key"),
        F.date_format("activity_date", "yyyyMMdd").cast("int").alias("activity_date_key"),
        # Degenerate attributes
        F.col("subject"),
        F.col("status"),
        F.col("priority"),
        F.col("task_subtype"),
        F.col("call_type"),
        F.col("call_disposition"),
        F.col("is_high_priority"),
        F.col("is_closed"),
        F.col("completed_date_time"),
        # Measures
        F.col("call_duration_seconds"),
    )
