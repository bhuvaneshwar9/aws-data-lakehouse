"""
Uploads sample data to S3 Bronze bucket and runs ETL via AWS Glue.
Requires: aws configure + python pipeline/setup_aws.py run first.

Usage: python pipeline/run_pipeline.py
"""

import boto3
import os
import time

BRONZE_BUCKET = "data-lakehouse-bronze"
GLUE_JOB      = "data-lakehouse-bronze-to-silver"
REGION        = "us-east-1"
DATA_FILE     = os.path.join(os.path.dirname(__file__), "..", "data", "sample_transactions.csv")


def upload_data(s3):
    key = "raw/sample_transactions.csv"
    print(f"Uploading data to s3://{BRONZE_BUCKET}/{key} ...")
    s3.upload_file(DATA_FILE, BRONZE_BUCKET, key)
    print("  ✓ Upload complete")


def trigger_glue(glue):
    print(f"\nStarting Glue job: {GLUE_JOB} ...")
    response = glue.start_job_run(JobName=GLUE_JOB)
    run_id   = response["JobRunId"]
    print(f"  ✓ Job run started: {run_id}")

    print("  Waiting for completion", end="", flush=True)
    while True:
        status = glue.get_job_run(JobName=GLUE_JOB, RunId=run_id)["JobRun"]["JobRunState"]
        if status in ("SUCCEEDED", "FAILED", "STOPPED"):
            print(f"\n  Status: {status}")
            break
        print(".", end="", flush=True)
        time.sleep(10)


def main():
    s3   = boto3.client("s3",   region_name=REGION)
    glue = boto3.client("glue", region_name=REGION)
    upload_data(s3)
    trigger_glue(glue)


if __name__ == "__main__":
    main()
