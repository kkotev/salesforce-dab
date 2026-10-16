from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="silver_accounts",
    comment="Cleaned Salesforce accounts",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_id", "Id IS NOT NULL")
@dp.expect_or_drop("valid_name", "Name IS NOT NULL")
def silver_accounts():
    return (
        spark.readStream.table("bronze_salesforce.raw_salesforce.account")
        .filter(F.col("IsDeleted") == False)
        .select(
            F.col("Id"),
            F.col("Name"),
            F.col("Type"),
            F.col("Industry"),
            F.col("AnnualRevenue").cast("decimal(18,2)").alias("annual_revenue"),
            F.col("NumberOfEmployees").alias("number_of_employees"),
            F.col("Rating"),
            F.col("Ownership"),
            F.split(F.col("BillingStreet"), "\n").getItem(0).alias("billing_street"),
            F.col("BillingCity").alias("billing_city"),
            F.col("BillingState").alias("billing_state"),
            F.col("BillingPostalCode").alias("billing_postal_code"),
            F.col("BillingCountryCode").alias("billing_country_code"),
            F.col("Phone"),
            F.col("Website"),
            F.col("AccountNumber").alias("account_number"),
            F.col("CustomerPriority__c").alias("customer_priority"),
            F.col("SLA__c").alias("sla"),
            F.col("Active__c").alias("is_active"),
            F.col("SLAExpirationDate__c").alias("sla_expiration_date"),
            F.col("UpsellOpportunity__c").alias("upsell_opportunity"),
            F.col("NumberofLocations__c").cast("int").alias("number_of_locations"),
            F.col("CreatedDate").alias("created_date"),
            F.col("LastModifiedDate").alias("last_modified_date"),
            F.col("OwnerId").alias("owner_id"),
        )
    )
