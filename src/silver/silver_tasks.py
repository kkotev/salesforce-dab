from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Bronze catalog/schema come from the pipeline configuration (bundle variables catalog / bronze_schema)
BRONZE = f"{spark.conf.get('bronze_catalog')}.{spark.conf.get('bronze_schema')}"


@dp.table(
    name="silver_tasks",
    comment="Cleaned Salesforce tasks and activities",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_id", "Id IS NOT NULL")
@dp.expect_all({
    "status_not_null": "status IS NOT NULL",
    "created_date_not_in_future": "created_date IS NULL OR created_date <= current_timestamp()",
})
def silver_tasks():
    return (
        spark.readStream.table(f"{BRONZE}.task")
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
