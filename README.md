# salesforce-dab

Declarative Automation Bundle for ingesting Salesforce data into Databricks via Lakeflow Connect.

## Pipeline

Ingests 6 Salesforce objects into the `bronze_salesforce.raw_salesforce` schema:

* Account
* Contact
* Lead
* Opportunity
* Case
* Task

## Targets

| Target | Catalog | Schema | Mode |
| --- | --- | --- | --- |
| dev | bronze_salesforce_dev | raw_salesforce_dev | development |
| staging | bronze_salesforce_staging | raw_salesforce_staging | production |
| prod | bronze_salesforce | raw_salesforce | production |

## CI/CD

GitHub Actions workflow (`.github/workflows/deploy.yml`):

* **Pull request to main** — validates the bundle against dev and prod targets
* **Push to main** — deploys the bundle to the prod target

Authentication uses OAuth M2M with a Databricks service principal.