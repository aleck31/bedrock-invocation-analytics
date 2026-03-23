"""
Lambda ETL: Process Bedrock invocation log files from S3.
Triggered by S3 Event via EventBridge.

Flow: S3 (.json.gz) → parse → pricing lookup → DynamoDB aggregation
"""

import gzip
import json
import os
import re
import time
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

USAGE_STATS_TABLE = os.environ["USAGE_STATS_TABLE"]
PRICING_TABLE = os.environ["MODEL_PRICING_TABLE"]

usage_stats_table = dynamodb.Table(USAGE_STATS_TABLE)
pricing_table = dynamodb.Table(PRICING_TABLE)

# Cache pricing lookups within a single Lambda invocation
_pricing_cache: dict[str, dict] = {}


def handler(event, context):
    """EventBridge S3 event handler."""
    detail = event.get("detail", {})
    bucket = detail.get("bucket", {}).get("name")
    key = detail.get("object", {}).get("key")

    if not bucket or not key:
        print(f"Invalid event: {json.dumps(event)}")
        return

    # Skip non-log files
    if not key.endswith(".json.gz"):
        return
    if "/data/" in key or "permission-check" in key:
        return

    # Extract accountId and region from S3 path
    # Pattern: {prefix}AWSLogs/{accountId}/BedrockModelInvocationLogs/{region}/YYYY/MM/DD/HH/file.json.gz
    m = re.search(
        r"AWSLogs/(\d+)/BedrockModelInvocationLogs/([\w-]+)/", key
    )
    if not m:
        print(f"Cannot parse account/region from key: {key}")
        return

    path_account_id = m.group(1)
    path_region = m.group(2)

    try:
        process_file(bucket, key, path_account_id, path_region)
    except Exception as e:
        print(f"Error processing s3://{bucket}/{key}: {e}")
        raise


def process_file(bucket, key, path_account_id, path_region):
    """Download, parse, and aggregate a single log file."""
    resp = s3.get_object(Bucket=bucket, Key=key)
    raw = gzip.decompress(resp["Body"].read()).decode("utf-8")

    # Log files can contain multiple JSON records (NDJSON format)
    for line in raw.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as e:
            print(f"Skipping malformed JSON line in {key}: {e}")
            continue
        process_record(record, path_account_id, path_region)


def process_record(record, path_account_id, path_region):
    """Process a single invocation log record."""
    model_id = record.get("modelId", "unknown")
    timestamp_str = record.get("timestamp", "")
    account_id = record.get("accountId", path_account_id)
    region = record.get("region", path_region)

    inp = record.get("input", {})
    out = record.get("output", {})
    input_tokens = inp.get("inputTokenCount", 0) or 0
    output_tokens = out.get("outputTokenCount", 0) or 0

    # Latency from output body
    output_body = out.get("outputBodyJson", {}) or {}
    metrics = output_body.get("metrics", {}) or {}
    latency_ms = metrics.get("latencyMs", 0) or 0

    # Caller from identity ARN — extract username/role
    identity = record.get("identity", {}) or {}
    caller_arn = identity.get("arn", "")
    caller = extract_caller(caller_arn)

    # Parse hour for aggregation key
    hour_key = parse_hour(timestamp_str)
    if not hour_key:
        print(f"Cannot parse timestamp: {timestamp_str}")
        return

    # Lookup pricing
    pricing = get_pricing(model_id, timestamp_str)
    input_price_micro = pricing.get("input_price_micro", 0)
    output_price_micro = pricing.get("output_price_micro", 0)

    # cost in micro-USD
    cost_micro = (
        input_tokens * input_price_micro // 1000
        + output_tokens * output_price_micro // 1000
    )

    pk = f"{account_id}#{region}"

    # Update 3 aggregation records: MODEL, CALLER, TOTAL
    ttl_val = int(time.time()) + 90 * 86400  # 90 days

    dimensions = [f"MODEL#{model_id}", f"TOTAL"]
    if caller:
        dimensions.append(f"CALLER#{caller}")

    for dim in dimensions:
        sk = f"HOURLY#{hour_key}#{dim}"
        update_aggregation(pk, sk, input_tokens, output_tokens, cost_micro, latency_ms, ttl_val)

    # Auto-register account#region
    register_account(pk)


def update_aggregation(pk, sk, input_tokens, output_tokens, cost_micro, latency_ms, ttl_val):
    """Atomic update of aggregation record."""
    update_expr = (
        "ADD invocations :one, input_tokens :inp, output_tokens :out, "
        "cost_micro_usd :cost, latency_sum_ms :lat "
        "SET #ttl = if_not_exists(#ttl, :ttl)"
    )
    expr_values = {
        ":one": 1,
        ":inp": input_tokens,
        ":out": output_tokens,
        ":cost": cost_micro,
        ":lat": latency_ms,
        ":ttl": ttl_val,
    }
    expr_names = {"#ttl": "ttl"}

    usage_stats_table.update_item(
        Key={"PK": pk, "SK": sk},
        UpdateExpression=update_expr,
        ExpressionAttributeValues=expr_values,
        ExpressionAttributeNames=expr_names,
    )

    # Conditional max_latency_ms update (separate call to avoid complexity)
    if latency_ms > 0:
        try:
            usage_stats_table.update_item(
                Key={"PK": pk, "SK": sk},
                UpdateExpression="SET max_latency_ms = :val",
                ConditionExpression="attribute_not_exists(max_latency_ms) OR max_latency_ms < :val",
                ExpressionAttributeValues={":val": latency_ms},
            )
        except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            pass  # existing max is higher, that's fine


def get_pricing(model_id, timestamp_str):
    """Get pricing for model at given timestamp. Cached per invocation."""
    cache_key = f"{model_id}#{timestamp_str}"
    if cache_key in _pricing_cache:
        return _pricing_cache[cache_key]

    result = {"input_price_micro": 0, "output_price_micro": 0}

    try:
        resp = pricing_table.query(
            KeyConditionExpression=Key("PK").eq(f"MODEL#{model_id}") & Key("SK").lte(timestamp_str),
            ScanIndexForward=False,
            Limit=1,
        )
        items = resp.get("Items", [])
        if items:
            item = items[0]
            # Convert per-1k-token price to micro-USD per 1k tokens
            result["input_price_micro"] = int(float(item.get("input_per_1k", 0)) * 1_000_000)
            result["output_price_micro"] = int(float(item.get("output_per_1k", 0)) * 1_000_000)
    except Exception as e:
        print(f"Pricing lookup failed for {model_id}: {e}")

    _pricing_cache[cache_key] = result
    return result


def register_account(pk):
    """Auto-register account#region to META if not exists."""
    now = datetime.now(timezone.utc).isoformat()
    usage_stats_table.update_item(
        Key={"PK": "META", "SK": f"ACCOUNT#{pk}"},
        UpdateExpression="SET registered_at = if_not_exists(registered_at, :now), last_seen = :now",
        ExpressionAttributeValues={":now": now},
    )


def extract_caller(arn):
    """Extract readable caller name from IAM ARN."""
    if not arn:
        return ""
    # arn:aws:sts::123:assumed-role/RoleName/SessionName → RoleName/SessionName
    # arn:aws:iam::123:user/UserName → UserName
    parts = arn.split("/", 1)
    if len(parts) > 1:
        return parts[1]
    return arn.rsplit(":", 1)[-1]


def parse_hour(timestamp_str):
    """Parse ISO timestamp to hour key like 2026-03-23T05."""
    if not timestamp_str:
        return None
    try:
        # Handle various formats: 2026-03-23T05:30:00Z, 2026-03-23T05:30:00.000Z
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%dT%H")
    except (ValueError, AttributeError):
        return None
