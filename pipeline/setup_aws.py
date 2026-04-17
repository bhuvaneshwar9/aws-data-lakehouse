"""
Creates S3 buckets for Bronze / Silver / Gold layers.
Run this once before run_pipeline.py.

Usage: python pipeline/setup_aws.py
"""

import boto3
import sys

BUCKETS = ["data-lakehouse-bronze", "data-lakehouse-silver", "data-lakehouse-gold"]
REGION  = "us-east-1"


def create_bucket(s3, name: str):
    try:
        if REGION == "us-east-1":
            s3.create_bucket(Bucket=name)
        else:
            s3.create_bucket(
                Bucket=name,
                CreateBucketConfiguration={"LocationConstraint": REGION}
            )
        print(f"  ✓ Created: {name}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"  - Already exists: {name}")
    except Exception as e:
        print(f"  ✗ Failed {name}: {e}")
        sys.exit(1)


def main():
    s3 = boto3.client("s3", region_name=REGION)
    print("Setting up S3 buckets...")
    for bucket in BUCKETS:
        create_bucket(s3, bucket)
    print("\nDone! Run python pipeline/run_pipeline.py next.")


if __name__ == "__main__":
    main()
