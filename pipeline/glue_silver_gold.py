"""AWS Glue job: Silver → Gold aggregations (runs on Glue cluster)."""

import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc   = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job   = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER = "s3://data-lakehouse-silver/transactions/"
GOLD   = "s3://data-lakehouse-gold/"

df = spark.read.parquet(SILVER)

daily = (
    df.filter(F.col("amount") > 0)
    .groupBy("event_date", "region", "category")
    .agg(
        F.count("id").alias("total_events"),
        F.sum("amount").alias("total_revenue"),
        F.avg("amount").alias("avg_order"),
        F.countDistinct("user_id").alias("unique_users"),
    )
)
daily.write.mode("overwrite").parquet(f"{GOLD}daily_aggregates/")

users = (
    df.groupBy("user_id")
    .agg(
        F.count("id").alias("total_orders"),
        F.sum("amount").alias("lifetime_value"),
        F.avg("amount").alias("avg_order_value"),
        F.min("event_time").alias("first_seen"),
        F.max("event_time").alias("last_seen"),
    )
)
users.write.mode("overwrite").parquet(f"{GOLD}user_summary/")
print("Gold layer written.")
job.commit()
