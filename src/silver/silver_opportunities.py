from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="silver_opportunities",
    comment="Cleaned Salesforce opportunities with derived metrics",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_id", "Id IS NOT NULL")
@dp.expect_or_drop("valid_name", "Name IS NOT NULL")
@dp.expect_all({
    "valid_account_id": "account_id IS NOT NULL AND length(account_id) IN (15, 18)",
    "non_negative_amount": "amount IS NULL OR amount >= 0",
    "valid_probability": "probability IS NULL OR probability BETWEEN 0 AND 100",
    "close_date_present": "close_date IS NOT NULL",
    "created_date_not_in_future": "created_date IS NULL OR created_date <= current_timestamp()",
})
def silver_opportunities():
    return (
        spark.readStream.table("bronze_salesforce.raw_salesforce.opportunity")
        .filter(F.col("IsDeleted") == False)
        .select(
            F.col("Id"),
            F.col("AccountId").alias("account_id"),
            F.col("ContactId").alias("contact_id"),
            F.col("Name"),
            F.col("Description").alias("description"),
            F.col("StageName").alias("stage_name"),
            F.col("Amount").cast("decimal(18,2)").alias("amount"),
            F.col("Probability").alias("probability"),
            F.col("ExpectedRevenue").cast("decimal(18,2)").alias("expected_revenue"),
            F.col("CloseDate").alias("close_date"),
            F.col("Type").alias("type"),
            F.col("LeadSource").alias("lead_source"),
            F.col("ForecastCategory").alias("forecast_category"),
            F.col("ForecastCategoryName").alias("forecast_category_name"),
            F.col("IsClosed").alias("is_closed"),
            F.col("IsWon").alias("is_won"),
            F.col("FiscalQuarter").alias("fiscal_quarter"),
            F.col("FiscalYear").alias("fiscal_year"),
            F.col("NextStep").alias("next_step"),
            F.datediff(F.col("CloseDate"), F.to_date(F.col("CreatedDate")))
                .alias("days_to_close"),
            F.col("CampaignId").alias("campaign_id"),
            F.col("DeliveryInstallationStatus__c").alias("delivery_installation_status"),
            F.col("TrackingNumber__c").alias("tracking_number"),
            F.col("OrderNumber__c").alias("order_number"),
            F.col("CurrentGenerators__c").alias("current_generators"),
            F.col("MainCompetitors__c").alias("main_competitors"),
            F.col("CreatedDate").alias("created_date"),
            F.col("LastModifiedDate").alias("last_modified_date"),
            F.col("OwnerId").alias("owner_id"),
        )
    )
