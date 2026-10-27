from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Bronze catalog/schema come from the pipeline configuration (bundle variables catalog / bronze_schema)
BRONZE = f"{spark.conf.get('bronze_catalog')}.{spark.conf.get('bronze_schema')}"


@dp.table(
    name="silver_contacts",
    comment="Cleaned Salesforce contacts with account name",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_id", "Id IS NOT NULL")
@dp.expect_or_drop("valid_name", "full_name IS NOT NULL")
@dp.expect_all({
    "valid_account_id": "account_id IS NOT NULL AND length(account_id) IN (15, 18)",
    "valid_email_format": "email IS NULL OR email RLIKE '^[^@ ]+@[^@ ]+[.][^@ ]+$'",
    "created_date_not_in_future": "created_date IS NULL OR created_date <= current_timestamp()",
})
def silver_contacts():
    return (
        spark.readStream.table(f"{BRONZE}.contact")
        .filter(F.col("IsDeleted") == False)
        .select(
            F.col("Id"),
            F.col("AccountId").alias("account_id"),
            F.concat_ws(" ", F.col("Salutation"), F.col("Name")).alias("full_name"),
            F.col("Email").alias("email"),
            F.col("Phone").alias("phone"),
            F.col("MobilePhone").alias("mobile_phone"),
            F.col("Title").alias("title"),
            F.col("Department").alias("department"),
            F.col("LeadSource").alias("lead_source"),
            F.col("Birthdate").alias("birthdate"),
            F.col("MailingCity").alias("mailing_city"),
            F.col("MailingState").alias("mailing_state"),
            F.col("MailingPostalCode").alias("mailing_postal_code"),
            F.col("MailingCountryCode").alias("mailing_country_code"),
            F.col("IsEmailBounced").alias("is_email_bounced"),
            F.col("Level__c").alias("level"),
            F.col("Languages__c").alias("languages"),
            F.col("CreatedDate").alias("created_date"),
            F.col("LastModifiedDate").alias("last_modified_date"),
            F.col("OwnerId").alias("owner_id"),
        )
    )
