from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="silver_leads",
    comment="Cleaned Salesforce leads with conversion tracking",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_id", "Id IS NOT NULL")
@dp.expect_or_drop("valid_name", "full_name IS NOT NULL")
@dp.expect_all({
    "valid_email_format": "Email IS NULL OR Email RLIKE '^[^@ ]+@[^@ ]+[.][^@ ]+$'",
    "status_not_null": "status IS NOT NULL",
    "created_date_not_in_future": "created_date IS NULL OR created_date <= current_timestamp()",
})
def silver_leads():
    return (
        spark.readStream.table("bronze_salesforce.raw_salesforce.lead")
        .filter(F.col("IsDeleted") == False)
        .select(
            F.col("Id"),
            F.concat_ws(" ", F.col("FirstName"), F.col("LastName")).alias("full_name"),
            F.col("FirstName").alias("first_name"),
            F.col("LastName").alias("last_name"),
            F.col("Salutation").alias("salutation"),
            F.col("Title").alias("title"),
            F.col("Company").alias("company"),
            F.col("Email"),
            F.col("Phone"),
            F.col("MobilePhone").alias("mobile_phone"),
            F.col("Website").alias("website"),
            F.col("LeadSource").alias("lead_source"),
            F.col("Status").alias("status"),
            F.col("Industry").alias("industry"),
            F.col("Rating").alias("rating"),
            F.col("AnnualRevenue").cast("decimal(18,2)").alias("annual_revenue"),
            F.col("NumberOfEmployees").alias("number_of_employees"),
            F.col("City").alias("city"),
            F.col("State").alias("state"),
            F.col("PostalCode").alias("postal_code"),
            F.col("CountryCode").alias("country_code"),
            F.col("IsConverted").alias("is_converted"),
            F.col("ConvertedDate").alias("converted_date"),
            F.col("ConvertedAccountId").alias("converted_account_id"),
            F.col("ConvertedContactId").alias("converted_contact_id"),
            F.col("ConvertedOpportunityId").alias("converted_opportunity_id"),
            F.when(F.col("Status").isin("Open - Not Contacted", "Working - Contacted"), True)
                .otherwise(False).alias("is_open"),
            F.col("ProductInterest__c").alias("product_interest"),
            F.col("CurrentGenerators__c").alias("current_generators"),
            F.col("NumberofLocations__c").cast("int").alias("number_of_locations"),
            F.col("CreatedDate").alias("created_date"),
            F.col("LastModifiedDate").alias("last_modified_date"),
            F.col("OwnerId").alias("owner_id"),
        )
    )
