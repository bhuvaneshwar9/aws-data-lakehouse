"""AWS Glue job: Bronze → Silver (runs on Glue cluster)."""

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

BRONZE = "s3://data-lakehouse-bronze/raw/"
SILVER = "s3://data-lakehouse-silver/transactions/"

df = spark.read.option("header", True).option("inferSchema", True).csv(BRONZE)
df = df.dropDuplicates(["id"]).dropna(subset=["id", "user_id", "event_time"])
df = df.withColumn("event_time", F.to_timestamp("event_time"))
df = df.withColumn("amount", F.col("amount").cast("double"))
df = df.filter(F.col("amount").isNotNull())

mean_val = df.agg(F.avg("amount")).collect()[0][0]
std_val  = df.agg(F.stddev("amount")).collect()[0][0]
df = df.withColumn("is_anomaly", F.abs(F.col("amount") - mean_val) > 3 * std_val)
df = df.withColumn("event_date", F.to_date("event_time"))
df = df.withColumn("_processed_at", F.current_timestamp())

df.write.mode("overwrite").partitionBy("event_date").parquet(SILVER)
print(f"Silver layer written: {df.count()} rows")
job.commit()
