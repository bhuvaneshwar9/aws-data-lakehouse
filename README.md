# AWS Data Lakehouse Pipeline

End-to-end data lakehouse on AWS using S3 + Glue + Redshift.  
Implements a **Bronze → Silver → Gold** medallion architecture.

## Architecture
```
CSV Upload → S3 Bronze → Glue ETL → S3 Silver/Gold → Redshift Analytics
```

## Tech Stack
`Python` `AWS S3` `AWS Glue` `AWS Redshift` `PySpark` `boto3`

## Quick Start

```bash
pip install -r requirements.txt

# 1. Configure AWS credentials
aws configure

# 2. Set up S3 buckets
python pipeline/setup_aws.py

# 3. Run the full ETL pipeline
python pipeline/run_pipeline.py

# 4. Run locally without AWS (demo mode)
python pipeline/local_etl.py
```

## Project Structure
```
aws-data-lakehouse/
├── pipeline/
│   ├── setup_aws.py       # creates S3 buckets
│   ├── run_pipeline.py    # uploads data & triggers Glue
│   ├── glue_bronze_silver.py
│   ├── glue_silver_gold.py
│   └── local_etl.py       # runs full ETL locally (no AWS needed)
├── data/
│   └── sample_transactions.csv
└── tests/
    └── test_etl.py
```
