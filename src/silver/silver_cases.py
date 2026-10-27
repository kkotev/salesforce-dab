from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Bronze catalog/schema come from the pipeline configuration (bundle variables catalog / bronze_schema)
BRONZE = f"{spark.conf.get('bronze_catalog')}.{spark.conf.get('bronze_schema')}"


@dp.table(
    name="silver_cases",
    comment="Cleaned Salesforce support cases with resolution metrics",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_id", "Id IS NOT NULL")
@dp.expect("valid_subject", "Subject IS NOT NULL")
@dp.expect_all({
    "valid_account_id": "account_id IS NOT NULL AND length(account_id) IN (15, 18)",
    "status_not_null": "status IS NOT NULL",
    "created_date_not_in_future": "created_date IS NULL OR created_date <= current_timestamp()",
})
def silver_cases():
    return (
        spark.readStream.table(f"{BRONZE}.case")
        .filter(F.col("IsDeleted") == False)
        .select(
            F.col("Id"),
            F.col("CaseNumber").alias("case_number"),
            F.col("AccountId").alias("account_id"),
            F.col("ContactId").alias("contact_id"),
            F.col("Subject").alias("subject"),
            F.col("Description").alias("description"),
            F.col("Status").alias("status"),
            F.col("Priority").alias("priority"),
            F.col("Origin").alias("origin"),
            F.col("Type").alias("type"),
            F.col("Reason").alias("reason"),
            F.col("IsClosed").alias("is_closed"),
            F.col("ClosedDate").alias("closed_date"),
            F.col("IsEscalated").alias("is_escalated"),
            F.datediff(F.col("ClosedDate"), F.to_date(F.col("CreatedDate")))
                .alias("resolution_days"),
            F.col("ContactPhone").alias("contact_phone"),
            F.col("ContactEmail").alias("contact_email"),
            F.col("EngineeringReqNumber__c").alias("engineering_req_number"),
            F.col("SLAViolation__c").alias("sla_violation"),
            F.col("Product__c").alias("product"),
            F.col("PotentialLiability__c").alias("potential_liability"),
            F.col("CreatedDate").alias("created_date"),
            F.col("LastModifiedDate").alias("last_modified_date"),
            F.col("OwnerId").alias("owner_id"),
        )
    )
