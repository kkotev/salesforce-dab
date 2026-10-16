from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="silver_contacts",
    comment="Cleaned Salesforce contacts with account name",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_id", "Id IS NOT NULL")
@dp.expect_or_drop("valid_name", "full_name IS NOT NULL")
def silver_contacts():
    return (
        spark.readStream.table("bronze_salesforce.raw_salesforce.contact")
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
