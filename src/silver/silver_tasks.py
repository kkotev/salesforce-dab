from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="silver_tasks",
    comment="Cleaned Salesforce tasks and activities",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_id", "Id IS NOT NULL")
def silver_tasks():
    return (
        spark.readStream.table("bronze_salesforce.raw_salesforce.task")
        .filter(F.col("IsDeleted") == False)
        .select(
            F.col("Id"),
            F.col("AccountId").alias("account_id"),
            F.col("WhoId").alias("who_id"),
            F.col("WhatId").alias("what_id"),
            F.col("Subject").alias("subject"),
            F.col("Description").alias("description"),
            F.col("ActivityDate").alias("activity_date"),
            F.col("Status").alias("status"),
            F.col("Priority").alias("priority"),
            F.col("IsHighPriority").alias("is_high_priority"),
            F.col("IsClosed").alias("is_closed"),
            F.col("CompletedDateTime").alias("completed_date_time"),
            F.col("CallDurationInSeconds").alias("call_duration_seconds"),
            F.col("CallType").alias("call_type"),
            F.col("CallDisposition").alias("call_disposition"),
            F.col("TaskSubtype").alias("task_subtype"),
            F.col("IsRecurrence").alias("is_recurrence"),
            F.col("CreatedDate").alias("created_date"),
            F.col("LastModifiedDate").alias("last_modified_date"),
            F.col("OwnerId").alias("owner_id"),
        )
    )
