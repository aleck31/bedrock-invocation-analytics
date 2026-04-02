# Bedrock Invocation Analytics

English | [中文](docs/README_CN.md)

Real-time analytics for Amazon Bedrock — monitor token usage, costs, and performance across AWS accounts.

## Features
- Summary cards: invocations, input/output tokens, estimated cost, avg latency
- Token usage & cost by model and by caller (chart / pie / table views)
- Pie charts: input tokens, output tokens, cost breakdown
- Performance: latency by model (min/avg/max) + latency trend with model selector
- Usage trend over time with model selector
- Auto refresh (5s / 15s / 30s / 1min / 5min)
- Pricing settings: view/edit model pricing with history, sync status
- Login authentication (configurable via config.yaml)
- Multi-account, multi-region support (sidebar selector)
- Responsive layout (desktop & mobile)

**Screenshot**

![WebUI](docs/webui_screenshot.png)

## Project Structure

```
├── deploy/
│   ├── cdk.json              # CDK config
│   ├── app.py                # CDK app entry (hub/spoke routing)
│   ├── hub_stack.py          # Primary account stack
│   ├── spoke_stack.py        # Spoke account stack
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
├── config.example.yaml       # Multi-account deployment config
├── deploy.sh                 # CDK deploy script (hub/spoke/all)
├── start-webui.sh            # WebUI launch script (reads .env.deploy)
└── pyproject.toml            # Dependencies (managed by uv)
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ Primary Account                                                 │
│                                                                 │
│  S3 Bucket ──→ EventBridge ──→ Lambda: process_log ──┐          │
│  Bedrock Logging                                     │          │
│                                                      ▼          │
│  DynamoDB: usage-stats  ◄────────────────────────── writes      │
│  DynamoDB: model-pricing ◄── Lambda: sync_pricing (weekly)      │
│  Lambda: aggregate_stats (daily/monthly rollup)                 │
│  IAM Role: SpokeWriteRole                                       │
│  WebUI ──→ reads DynamoDB                                       │
└─────────────────────────────────────────────────────────────────┘
       ▲ assume role
       │
┌──────┴──────────────────────────────────────────────────────────┐
│ Spoke Account(s)                                                │
│                                                                 │
│  S3 Bucket ──→ EventBridge ──→ Lambda: process_log              │
│  Bedrock Logging                     │                          │
│                                      ▼                          │
│                        assume SpokeWriteRole → Hub DynamoDB     │
│  SQS DLQ (failed processing)                                    │
└─────────────────────────────────────────────────────────────────┘
```

**How it works:**
1. Each account writes Bedrock invocation logs to its own S3 bucket (same region required by Bedrock)
2. S3 events trigger a Lambda ETL that parses tokens, latency, caller, and calculates cost using pricing data
3. Spoke Lambdas assume a cross-account IAM role to write to the primary account's DynamoDB
4. Stats are aggregated in DynamoDB by model, caller, and totals at hourly/daily/monthly granularity
5. A weekly Lambda syncs model pricing from [LiteLLM](https://github.com/BerriAI/litellm) (286+ Bedrock models)
6. WebUI reads DynamoDB for sub-second dashboard loading across all accounts

## Prerequisites

- [AWS CDK CLI](https://docs.aws.amazon.com/cdk/v2/guide/getting-started.html) (`npm install -g aws-cdk`)
- [uv](https://docs.astral.sh/uv/) (Python package manager)
- AWS credentials configured (`aws configure` or `~/.aws/credentials`)

## Deploy

Configuration is defined in `config.yaml`:

```yaml
log_prefix: bedrock/invocation-logs/

accounts:
  - profile: me
    region: us-west-2
    bucket: my-existing-bucket
    primary: true           # Full stack: DynamoDB, Pricing, Aggregate, WebUI

  - profile: lab
    region: us-west-2
    bucket: ""              # Empty = create new bucket
```

The account marked `primary: true` deploys the full stack. Other accounts deploy a lightweight spoke stack (S3 + Bedrock logging + ETL Lambda) that writes to the primary account's DynamoDB.

```bash
# Install dependencies
uv sync

# Deploy primary account (auto-bootstraps CDK if needed)
./deploy.sh hub

# Deploy spoke account(s)
./deploy.sh spoke              # all spokes
./deploy.sh spoke lab          # specific spoke

# Deploy everything
./deploy.sh all
```

> For existing buckets, enable S3 EventBridge notifications:
> ```bash
> aws s3api put-bucket-notification-configuration --bucket YOUR_BUCKET \
>   --notification-configuration '{"EventBridgeConfiguration": {}}'
> ```

### Deployed Resources

**Primary account (Hub):**

| Resource | Purpose |
|----------|---------|
| Custom Resource | Configures Bedrock invocation logging |
| DynamoDB table × 2 | Usage stats aggregation + model pricing |
| Lambda × 4 | Log processing + stats rollup + pricing sync + Bedrock logging setup |
| EventBridge × 4 | S3 trigger + daily/monthly rollup + weekly pricing sync |
| IAM Role | SpokeWriteRole for cross-account access |
| S3 Bucket (optional) | Raw logs with encryption, lifecycle |

**Spoke accounts:**

| Resource | Purpose |
|----------|---------|
| Custom Resource | Configures Bedrock invocation logging |
| Lambda × 1 | Log processing (assumes hub role to write DynamoDB) |
| EventBridge × 1 | S3 trigger |
| SQS DLQ | Dead-letter queue for failed processing |
| S3 Bucket (optional) | Raw logs |

## Seed Pricing Data

Pricing data is sourced from [LiteLLM](https://github.com/BerriAI/litellm) (286+ Bedrock models):

```bash
AWS_DEFAULT_REGION=us-west-2 python3 scripts/seed_pricing.py \
  BedrockInvocationAnalytics-model-pricing YOUR_PROFILE
```

## Start WebUI

```bash
./start-webui.sh
```

Open http://localhost:8060 in your browser.

## Cleanup

```bash
./deploy.sh destroy              # destroy hub stack
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
