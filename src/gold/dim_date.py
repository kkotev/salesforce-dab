from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Gold catalog/schema come from the pipeline configuration (bundle variables gold_catalog / gold_schema)
GOLD = f"{spark.conf.get('gold_catalog')}.{spark.conf.get('gold_schema')}"


@dp.materialized_view(
    name=f"{GOLD}.dim_date",
    comment="Calendar dimension: one row per day covering full calendar years of all fact dates (date_key = yyyyMMdd)",
    table_properties={"quality": "gold"},
)
@dp.expect_or_drop("date_key_not_null", "date_key IS NOT NULL")
def dim_date():
    opps = spark.read.table("silver_opportunities")
    cases = spark.read.table("silver_cases")
    tasks = spark.read.table("silver_tasks")

    fact_dates = (
        opps.select(F.to_date("created_date").alias("d"))
        .unionByName(opps.select(F.col("close_date").alias("d")))
        .unionByName(cases.select(F.to_date("created_date").alias("d")))
        .unionByName(cases.select(F.to_date("closed_date").alias("d")))
        .unionByName(tasks.select(F.to_date("created_date").alias("d")))
        .unionByName(tasks.select(F.col("activity_date").alias("d")))
    )

    # Expand to whole calendar years: Jan 1 of the earliest year through Dec 31 of the latest year
    bounds = fact_dates.agg(
        F.trunc(F.min("d"), "year").alias("start_date"),
        F.last_day(F.add_months(F.trunc(F.max("d"), "year"), 11)).alias("end_date"),
    ).where("start_date IS NOT NULL")

    return (
        bounds.select(F.explode(F.sequence("start_date", "end_date")).alias("date"))
        .select(
            F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
            F.col("date"),
            F.year("date").alias("year"),
            F.quarter("date").alias("quarter"),
            F.concat(F.year("date"), F.lit("-Q"), F.quarter("date")).alias("year_quarter"),
            F.month("date").alias("month"),
            F.date_format("date", "MMMM").alias("month_name"),
            F.date_format("date", "yyyy-MM").alias("year_month"),
            F.weekofyear("date").alias("week_of_year"),
            F.dayofmonth("date").alias("day_of_month"),
            F.dayofweek("date").alias("day_of_week"),
            F.date_format("date", "EEEE").alias("day_name"),
            F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        )
    )
