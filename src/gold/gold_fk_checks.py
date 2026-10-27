from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Gold catalog/schema come from the pipeline configuration (bundle variables gold_catalog / gold_schema)
GOLD = f"{spark.conf.get('gold_catalog')}.{spark.conf.get('gold_schema')}"

# Orphaned foreign key checks (warn only). Each private view contains only the orphaned keys,
# so it stays empty while data is valid and the expectation metrics count the orphans.


def _orphans(fact, fk, dim, dim_key):
    return (
        spark.read.table(f"{GOLD}.{fact}")
        .where(F.col(fk).isNotNull())
        .select(fk)
        .join(spark.read.table(f"{GOLD}.{dim}").select(F.col(dim_key).alias(fk)), fk, "left_anti")
        .select(F.col(fk), F.lit(False).alias("has_matching_dim"))
    )


@dp.materialized_view(name="check_fk_opportunities_account", private=True, comment="Orphaned fact_opportunities.account_id")
@dp.expect("fact_opportunities_account_fk_valid", "has_matching_dim")
def check_fk_opportunities_account():
    return _orphans("fact_opportunities", "account_id", "dim_accounts", "account_id")


@dp.materialized_view(name="check_fk_opportunities_contact", private=True, comment="Orphaned fact_opportunities.contact_id")
@dp.expect("fact_opportunities_contact_fk_valid", "has_matching_dim")
def check_fk_opportunities_contact():
    return _orphans("fact_opportunities", "contact_id", "dim_contacts", "contact_id")


@dp.materialized_view(name="check_fk_cases_account", private=True, comment="Orphaned fact_cases.account_id")
@dp.expect("fact_cases_account_fk_valid", "has_matching_dim")
def check_fk_cases_account():
    return _orphans("fact_cases", "account_id", "dim_accounts", "account_id")


@dp.materialized_view(name="check_fk_cases_contact", private=True, comment="Orphaned fact_cases.contact_id")
@dp.expect("fact_cases_contact_fk_valid", "has_matching_dim")
def check_fk_cases_contact():
    return _orphans("fact_cases", "contact_id", "dim_contacts", "contact_id")


@dp.materialized_view(name="check_fk_tasks_account", private=True, comment="Orphaned fact_tasks.account_id")
@dp.expect("fact_tasks_account_fk_valid", "has_matching_dim")
def check_fk_tasks_account():
    return _orphans("fact_tasks", "account_id", "dim_accounts", "account_id")


@dp.materialized_view(name="check_fk_tasks_contact", private=True, comment="Orphaned fact_tasks.contact_id")
@dp.expect("fact_tasks_contact_fk_valid", "has_matching_dim")
def check_fk_tasks_contact():
    return _orphans("fact_tasks", "contact_id", "dim_contacts", "contact_id")


@dp.materialized_view(name="check_fk_contacts_account", private=True, comment="Orphaned dim_contacts.account_id")
@dp.expect("dim_contacts_account_fk_valid", "has_matching_dim")
def check_fk_contacts_account():
    return _orphans("dim_contacts", "account_id", "dim_accounts", "account_id")
