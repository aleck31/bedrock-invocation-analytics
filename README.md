# Bedrock Invocation Analytics

English | [中文](docs/README_CN.md)

Real-time analytics for Amazon Bedrock — monitor token usage, costs, and performance across AWS accounts.

## Architecture

```
Bedrock API → Invocation Logging → S3 (JSON.gz)
                                      │ S3 Event (EventBridge)
                                      ▼
                                   Lambda ETL → DynamoDB (aggregations)
                                                    │
                                                    ▼
                                               WebUI (dashboard)
```

**How it works:**
1. Bedrock writes invocation logs to S3 as compressed JSON
2. Each new log file triggers a Lambda that parses tokens, latency, caller, and calculates cost
3. Aggregated stats are stored in DynamoDB (by model, by caller, totals — hourly/daily/monthly)
4. WebUI reads DynamoDB for sub-second dashboard loading

## WebUI

![WebUI Screenshot](docs/webui_screenshot.png)

**Features:**
- Summary cards: invocations, input/output tokens, estimated cost, avg latency
- Token usage & cost by model and by caller (chart / pie / table views)
- Pie charts: input tokens, output tokens, cost breakdown
- Performance: latency by model (min/avg/max) + latency trend with model selector
- Usage trend over time with model selector
- Pricing settings: view/edit model pricing, sync status
- Multi-account, multi-region support (sidebar selector)
- Responsive layout (desktop & mobile)

## Prerequisites

- [AWS CDK CLI](https://docs.aws.amazon.com/cdk/v2/guide/getting-started.html) (`npm install -g aws-cdk`)
- [uv](https://docs.astral.sh/uv/) (Python package manager)
- AWS credentials configured (`aws configure` or `~/.aws/credentials`)

## Deploy

```bash
# Install dependencies
uv sync

# Bootstrap CDK (first time only)
./deploy.sh bootstrap --region us-west-2 --profile YOUR_PROFILE

# Deploy with existing S3 bucket
./deploy.sh deploy
```

> For existing buckets, enable S3 EventBridge notifications:
> ```bash
> aws s3api put-bucket-notification-configuration --bucket YOUR_BUCKET \
>   --notification-configuration '{"EventBridgeConfiguration": {}}'
> ```

### Deployed Resources

| Resource | Purpose |
|----------|---------|
| Custom Resource | Configures Bedrock invocation logging |
| DynamoDB table × 2 | Usage stats aggregation + model pricing |
| Lambda function × 3 | Log processing (event-driven) + stats rollup (scheduled) + pricing sync (weekly) |
| EventBridge × 4 | S3 trigger + daily/monthly rollup + weekly pricing sync |
| S3 Bucket (optional) | Raw logs with encryption, lifecycle, EventBridge notifications |

## Seed Pricing Data

Pricing data is sourced from [LiteLLM](https://github.com/BerriAI/litellm) (286+ Bedrock models):

```bash
AWS_DEFAULT_REGION=us-west-2 python3 scripts/seed_pricing.py \
  BedrockInvocationAnalytics-model-pricing YOUR_PROFILE
```

## Start WebUI

```bash
./start-webui.sh --region us-west-2 --profile YOUR_PROFILE

# Or if .env.deploy exists (created by deploy.sh), just:
./start-webui.sh
```

Open http://localhost:8080 in your browser.

## Project Structure

```
├── deploy/
│   ├── cdk.json              # CDK config
│   ├── app.py                # CDK app entry
│   ├── stack.py              # Stack definition
│   └── lambda/
│       ├── process_log.py    # ETL: S3 event → parse → DDB aggregation
│       ├── aggregate_stats.py # Rollup: HOURLY → DAILY → MONTHLY
│       └── sync_pricing.py   # Weekly pricing sync from LiteLLM
├── webui/
│   ├── main.py               # Entry point (ui.run)
│   ├── dashboard.py          # Dashboard page
│   ├── pricing.py            # Pricing settings page
│   └── data.py               # DynamoDB data access
├── scripts/
│   └── seed_pricing.py       # Seed pricing from LiteLLM
├── deploy.sh                 # CDK deploy script
├── start-webui.sh            # WebUI launch script
└── pyproject.toml            # Dependencies (managed by uv)
```

## Cleanup

```bash
./deploy.sh destroy --profile YOUR_PROFILE
```

> DynamoDB tables and S3 bucket are retained after stack deletion (RemovalPolicy: RETAIN).

## Cost

| Service | Pricing | Notes |
|---------|---------|-------|
| DynamoDB | Pay-per-request | ~$1.25/M writes, reads negligible |
| Lambda | $0.20/M requests | ~60ms per log file |
| S3 | ~$0.023/GB/month | Auto-transitions to IA after 90 days |

**Monthly estimate** (1M Bedrock invocations):
- Lambda: ~$0.20
- DynamoDB: ~$4 (3 writes per invocation)
- S3: ~$1 (cumulative log storage)
- **Total: ~$5/month**
