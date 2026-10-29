# AgriPulse-AWS-Data-Engineering-Agricultural-Market-Intelligence-Platform

# Mandi Price Advisory Pipeline

A real-time AWS data pipeline that fetches daily crop prices from 3000+ Indian markets, 
identifies the highest-paying markets per commodity, and sends SMS alerts to farmers.

## Problem Solved
Indian farmers lose money daily because they don't know which nearby mandi is paying 
the best price for their crop. Middlemen exploit this information gap. This pipeline 
closes it with a daily SMS alert costing under ₹3/month to run.

## Architecture
data.gov.in API → Lambda → Kinesis → Firehose → S3 Bronze
                                                      ↓
                                               Glue ETL (Bronze→Silver→Gold)
                                                      ↓
                                    Athena + Redshift + QuickSight Dashboard
                                                      ↓
                                              SNS SMS Alert to Farmers

## AWS Services Used
- **EventBridge** — daily 6 AM IST trigger
- **Lambda** — fetches mandi prices from data.gov.in API
- **Kinesis Data Streams** — 1 shard, real-time data pipeline
- **Kinesis Firehose** — auto-delivers to S3 with date partitioning
- **S3** — medallion architecture (Bronze/Silver/Gold)
- **Glue ETL** — PySpark jobs for Bronze→Silver→Gold transformation
- **Athena** — SQL queries directly on S3 Parquet files
- **Step Functions** — orchestrates Glue jobs + SMS alert automatically
- **SNS** — SMS price alerts to registered farmers
- **QuickSight** — live price dashboard
- **Secrets Manager** — secure API key storage
- **IAM** — least-privilege roles per service

## Data Source
[data.gov.in](https://data.gov.in) — Government of India open mandi price API  
Resource: Variety-wise Daily Market Prices (3000+ markets, updated daily)

## Key Technical Decisions
| Decision | Choice | Reason |
|---|---|---|
| Stream capacity | 1 provisioned shard | ~1000 records/day — right-sized, not over-provisioned |
| Storage format | Parquet + Snappy | 10x cheaper Athena queries vs raw JSON |
| ETL engine | Glue PySpark | Scales to terabytes without code changes |
| Medallion layers | 3 separate S3 buckets | Different access patterns, lifecycle rules per layer |
| Credentials | Secrets Manager | API key never appears in codebase |

## Pipeline Cost
~$3/month total AWS cost

## Project Structure
mandi-pipeline/
├── ingestion/
│   └── lambda_ingest.py
├── etl/
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
├── alerts/
│   └── lambda_price_alert.py
├── orchestration/
│   └── step_functions_dag.json
├── analytics/
│   └── athena_queries.sql
└── README.md

## Real-world Impact
Farmers in Maharashtra, Punjab, Kerala and 15+ other states receive daily SMS alerts 
identifying the highest-paying market for their commodity — enabling better selling 
decisions without internet access or smartphones.
