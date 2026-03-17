# [IaC] Bedrock Logging & Analytics

English | [中文](README_CN.md)

One-click solution to enable Amazon Bedrock invocation logging and analyze token usage & costs with Amazon Athena.

## Architecture

```
Bedrock API Call → Invocation Logging → S3 (JSON.gz) → Athena (SQL Query)
```

**Deployed Resources:**
- AWS Lambda (Custom Resource) — Configures Bedrock invocation logging
- IAM Role — Lambda execution role with Bedrock logging permissions
- Athena WorkGroup — Dedicated workgroup with query result location and 10GB scan limit
- Athena Named Queries — Pre-built analytics queries (7 templates)
- S3 Bucket (optional) — With AES256 encryption, public access block, and lifecycle policy

## Deploy

> **Note:** Replace `aleck31/bedrock-logging-analytics/main` below with your actual GitHub repository path.

| Region | Launch |
|--------|--------|
| us-west-2 (Oregon) | [![Launch Stack](https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png)](https://us-west-2.console.aws.amazon.com/cloudformation/home?region=us-west-2#/stacks/create/review?stackName=bedrock-logging-analytics&templateURL=https://raw.githubusercontent.com/aleck31/bedrock-logging-analytics/main/cf-deploy-template.yaml) |
| us-east-1 (N. Virginia) | [![Launch Stack](https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png)](https://us-east-1.console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/create/review?stackName=bedrock-logging-analytics&templateURL=https://raw.githubusercontent.com/aleck31/bedrock-logging-analytics/main/cf-deploy-template.yaml) |

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| UseExistingBucket | No | `Yes` to use an existing S3 bucket, `No` to create a new one |
| ExistingBucketName | — | Required if UseExistingBucket=Yes. Bucket must allow Bedrock `s3:PutObject` |
| LogPrefix | `bedrock/invocation-logs/` | S3 key prefix for invocation logs |
| LogRetentionDays | 365 | Log expiration in days (new bucket only) |
| AthenaDbName | `bedrock_analytics` | Athena database name |

## Post-Deployment Setup

1. Open the **Athena console** and select the workgroup `bedrock-logging-analytics-workgroup`
2. Go to **Saved queries** tab
3. Run `bedrock-logging-analytics-create-database` first
4. Run `bedrock-logging-analytics-create-table` to create the partitioned table
5. Start using the pre-built analytics queries

## Pre-built Queries

| Query | Description |
|-------|-------------|
| token-usage-by-model | Token consumption and avg latency per model (last 7 days) |
| estimated-cost-by-model | Estimated USD cost per model (last 30 days) |
| usage-by-caller | Token usage by IAM user/role for cost allocation |
| hourly-trend | Hourly invocation and token trend (last 24 hours) |
| daily-trend | Daily invocation and token trend (last 30 days) |
| high-latency-calls | Calls with latency > 5 seconds (last 7 days) |

## CLI Deployment

```bash
# Create new bucket
aws cloudformation create-stack \
  --stack-name bedrock-logging-analytics \
  --template-body file://cf-deploy-template.yaml \
  --parameters ParameterKey=UseExistingBucket,ParameterValue=No \
  --capabilities CAPABILITY_IAM \
  --region us-west-2

# Use existing bucket
aws cloudformation create-stack \
  --stack-name bedrock-logging-analytics \
  --template-body file://cf-deploy-template.yaml \
  --parameters ParameterKey=UseExistingBucket,ParameterValue=Yes \
               ParameterKey=ExistingBucketName,ParameterValue=YOUR_BUCKET_NAME \
  --capabilities CAPABILITY_IAM \
  --region us-west-2
```

## Cleanup

```bash
aws cloudformation delete-stack --stack-name bedrock-logging-analytics --region us-west-2
```

> Deleting the stack will disable Bedrock invocation logging. If a new S3 bucket was created, it is retained (DeletionPolicy: Retain) and must be deleted manually.

## Cost

- **S3 Storage** — Standard rates for log storage (~$0.023/GB/month)
- **Athena** — $5 per TB scanned (partition projection minimizes scan volume)
- **Lambda** — Invoked only during stack create/update/delete (negligible)
