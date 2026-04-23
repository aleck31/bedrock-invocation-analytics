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

# Spoke mode: assume cross-account role to access hub DynamoDB
HUB_ROLE_ARN = os.environ.get("HUB_ROLE_ARN")
_sts = boto3.client("sts") if HUB_ROLE_ARN else None
_hub_creds = None
_hub_creds_expiry = None


def _get_hub_session():
    """Get or refresh cross-account session. STS creds expire after 1h; refresh at 50min."""
    global _hub_creds, _hub_creds_expiry
    now = datetime.now(timezone.utc)
    if _hub_creds is None or now >= _hub_creds_expiry:
        _hub_creds = _sts.assume_role(RoleArn=HUB_ROLE_ARN, RoleSessionName="spoke-etl")["Credentials"]
        _hub_creds_expiry = _hub_creds["Expiration"].replace(tzinfo=timezone.utc) - __import__("datetime").timedelta(minutes=10)
    return boto3.Session(
        aws_access_key_id=_hub_creds["AccessKeyId"],
        aws_secret_access_key=_hub_creds["SecretAccessKey"],
        aws_session_token=_hub_creds["SessionToken"],
    )


def _get_dynamodb():
    return _get_hub_session().resource("dynamodb") if HUB_ROLE_ARN else boto3.resource("dynamodb")


dynamodb = _get_dynamodb()

USAGE_STATS_TABLE = os.environ["USAGE_STATS_TABLE"]
PRICING_TABLE = os.environ["MODEL_PRICING_TABLE"]

usage_stats_table = dynamodb.Table(USAGE_STATS_TABLE)
pricing_table = dynamodb.Table(PRICING_TABLE)

# Cache pricing lookups within a single Lambda invocation
_pricing_cache: dict[str, dict] = {}


def handler(event, context):
    """EventBridge S3 event handler."""
    global dynamodb, usage_stats_table, pricing_table
    if HUB_ROLE_ARN:
        dynamodb = _get_dynamodb()
        usage_stats_table = dynamodb.Table(USAGE_STATS_TABLE)
        pricing_table = dynamodb.Table(PRICING_TABLE)
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
    # Normalize: strip ARN prefix (arn:aws:bedrock:region:account:inference-profile/model → model)
    if model_id.startswith("arn:"):
        model_id = model_id.rsplit("/", 1)[-1]
    timestamp_str = record.get("timestamp", "")
    account_id = record.get("accountId", path_account_id)
    region = record.get("region", path_region)

    inp = record.get("input", {})
    out = record.get("output", {})
    input_tokens = inp.get("inputTokenCount", 0) or 0
    output_tokens = out.get("outputTokenCount", 0) or 0

    # Latency from output body
    output_body = out.get("outputBodyJson", {}) or {}
    if not isinstance(output_body, dict):
        output_body = {}  # streaming responses have list, skip metrics/usage from body
    metrics = output_body.get("metrics", {}) or {}
    latency_ms = metrics.get("latencyMs", 0) or 0

    # Cache tokens from usage block or input top-level (InvokeModelWithResponseStream)
    usage = output_body.get("usage", {}) if isinstance(output_body, dict) else {}
    cache_read_tokens = inp.get("cacheReadInputTokenCount", 0) or usage.get("cacheReadInputTokens", 0) or 0
    cache_write_tokens = inp.get("cacheWriteInputTokenCount", 0) or usage.get("cacheWriteInputTokens", 0) or 0

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
    cache_read_price_micro = pricing.get("cache_read_price_micro", 0)
    cache_write_price_micro = pricing.get("cache_write_price_micro", 0)

    # cost in micro-USD per token type
    cost_input = input_tokens * input_price_micro // 1000
    cost_output = output_tokens * output_price_micro // 1000
    cost_cache_read = cache_read_tokens * cache_read_price_micro // 1000
    cost_cache_write = cache_write_tokens * cache_write_price_micro // 1000
    cost_micro = cost_input + cost_output + cost_cache_read + cost_cache_write

    pk = f"{account_id}#{region}"

    # Update 3 aggregation records: MODEL, CALLER, TOTAL
    ttl_val = int(time.time()) + 90 * 86400  # 90 days

    dimensions = [f"MODEL#{model_id}", f"TOTAL"]
    if caller:
        dimensions.append(f"CALLER#{caller}")

    cost_parts = (cost_input, cost_output, cost_cache_read, cost_cache_write)
    for dim in dimensions:
        sk = f"HOURLY#{hour_key}#{dim}"
        update_aggregation(pk, sk, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, cost_micro, cost_parts, latency_ms, output_tokens and latency_ms, ttl_val)

    # Auto-register account#region
    register_account(pk)


def update_aggregation(pk, sk, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, cost_micro, cost_parts, latency_ms, has_tpot, ttl_val):
    """Atomic update of aggregation record."""
    cost_input, cost_output, cost_cache_read, cost_cache_write = cost_parts
    update_expr = (
        "ADD invocations :one, input_tokens :inp, output_tokens :out, "
        "cache_read_tokens :cr, cache_write_tokens :cw, "
        "cost_micro_usd :cost, cost_input_micro :ci, cost_output_micro :co, "
        "cost_cache_read_micro :ccr, cost_cache_write_micro :ccw, latency_sum_ms :lat "
        "SET #ttl = if_not_exists(#ttl, :ttl)"
    )
    expr_values = {
        ":one": 1,
        ":inp": input_tokens,
        ":out": output_tokens,
        ":cr": cache_read_tokens,
        ":cw": cache_write_tokens,
        ":cost": cost_micro,
        ":ci": cost_input,
        ":co": cost_output,
        ":ccr": cost_cache_read,
        ":ccw": cost_cache_write,
        ":lat": latency_ms,
        ":ttl": ttl_val,
    }
    expr_names = {"#ttl": "ttl"}

    # TPOT: approximate latencyMs / outputTokens (includes TTFT)
    tpot_micro = 0
    if has_tpot and output_tokens > 0 and latency_ms > 0:
        tpot_micro = round(latency_ms * 1000 / output_tokens)  # micro-ms for precision
        update_expr = update_expr.replace("SET ", "SET tpot_count = if_not_exists(tpot_count, :zero) + :one, ")
        update_expr += ", tpot_sum = if_not_exists(tpot_sum, :zero) + :tpot"
        expr_values[":tpot"] = tpot_micro
        expr_values[":zero"] = 0

    usage_stats_table.update_item(
        Key={"PK": pk, "SK": sk},
        UpdateExpression=update_expr,
        ExpressionAttributeValues=expr_values,
        ExpressionAttributeNames=expr_names,
    )

    # Conditional max/min latency_ms + tpot update
    if latency_ms > 0:
        conditionals = [
            ("SET max_latency_ms = :val", "attribute_not_exists(max_latency_ms) OR max_latency_ms < :val", latency_ms),
            ("SET min_latency_ms = :val", "attribute_not_exists(min_latency_ms) OR min_latency_ms > :val", latency_ms),
        ]
        if tpot_micro > 0:
            conditionals += [
                ("SET tpot_max = :val", "attribute_not_exists(tpot_max) OR tpot_max < :val", tpot_micro),
                ("SET tpot_min = :val", "attribute_not_exists(tpot_min) OR tpot_min > :val", tpot_micro),
            ]
        for expr, cond, val in conditionals:
            try:
                usage_stats_table.update_item(
                    Key={"PK": pk, "SK": sk},
                    UpdateExpression=expr,
                    ConditionExpression=cond,
                    ExpressionAttributeValues={":val": val},
                )
            except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
                pass


def get_pricing(model_id, timestamp_str):
    """Get pricing for model at given timestamp. Cached per invocation."""
    cache_key = f"{model_id}#{timestamp_str}"
    if cache_key in _pricing_cache:
        return _pricing_cache[cache_key]

    result = {"input_price_micro": 0, "output_price_micro": 0, "cache_read_price_micro": 0, "cache_write_price_micro": 0}

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
            result["cache_read_price_micro"] = int(float(item.get("cache_read_per_1k", 0)) * 1_000_000)
            result["cache_write_price_micro"] = int(float(item.get("cache_write_per_1k", 0)) * 1_000_000)
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
