"""DynamoDB data access for WebUI."""

import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key

USAGE_TABLE = os.environ.get("USAGE_STATS_TABLE", "BedrockLoggingAnalytics-usage-stats")
PRICING_TABLE = os.environ.get("MODEL_PRICING_TABLE", "BedrockLoggingAnalytics-model-pricing")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-west-2")

_ddb = boto3.resource("dynamodb", region_name=AWS_REGION)
_usage = _ddb.Table(USAGE_TABLE)
_pricing = _ddb.Table(PRICING_TABLE)


def _resolve_granularity(account_region: str, days: int):
    """Pick granularity. Fallback to HOURLY if DAILY has no data (rollup hasn't run yet)."""
    now = datetime.now(timezone.utc)
    if days <= 1:
        start = (now - timedelta(hours=24)).strftime("%Y-%m-%dT%H")
        end = now.strftime("%Y-%m-%dT%H")
        return "HOURLY", start, end

    # Try DAILY first
    start = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    end = now.strftime("%Y-%m-%d")
    resp = _usage.query(
        KeyConditionExpression=Key("PK").eq(account_region) & Key("SK").between(f"DAILY#{start}", f"DAILY#{end}\xff"),
        Limit=1,
    )
    if resp.get("Items"):
        return "DAILY", start, end

    # Fallback to HOURLY
    start = (now - timedelta(days=days)).strftime("%Y-%m-%dT%H")
    end = now.strftime("%Y-%m-%dT%H")
    return "HOURLY", start, end


def get_accounts() -> list[dict]:
    """Get registered account#region list."""
    try:
        resp = _usage.query(
            KeyConditionExpression=Key("PK").eq("META") & Key("SK").begins_with("ACCOUNT#"),
        )
    except Exception as e:
        print(f"[ERROR] Failed to query DynamoDB table '{USAGE_TABLE}' in {AWS_REGION}: {e}")
        return []
    results = []
    for item in resp.get("Items", []):
        acct_region = item["SK"].replace("ACCOUNT#", "")
        parts = acct_region.split("#", 1)
        results.append({"account_id": parts[0], "region": parts[1] if len(parts) > 1 else "", "key": acct_region})
    return results


def query_usage(account_region: str, granularity: str, start: str, end: str, dimension_prefix: str = "") -> list[dict]:
    """Query usage stats for a given account#region and time range.

    Args:
        account_region: e.g. "222829864634#us-west-2"
        granularity: "HOURLY", "DAILY", or "MONTHLY"
        start: start period, e.g. "2026-03-23T00" for HOURLY
        end: end period (inclusive bound)
        dimension_prefix: filter SK by dimension, e.g. "MODEL#", "CALLER#", "TOTAL"
    """
    sk_start = f"{granularity}#{start}"
    sk_end = f"{granularity}#{end}\xff"

    resp = _usage.query(
        KeyConditionExpression=Key("PK").eq(account_region) & Key("SK").between(sk_start, sk_end),
    )
    items = resp.get("Items", [])

    # Client-side filter by dimension
    if dimension_prefix:
        items = [i for i in items if _extract_dimension(i["SK"]).startswith(dimension_prefix)]

    return [_format_item(i, granularity) for i in items]


def get_summary(account_region: str, days: int = 7) -> dict:
    """Get summary stats for dashboard cards."""
    g, start, end = _resolve_granularity(account_region, days)
    items = query_usage(account_region, g, start, end, "TOTAL")

    total = {"invocations": 0, "input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0, "latency_sum_ms": 0}
    for item in items:
        for k in ("invocations", "input_tokens", "output_tokens"):
            total[k] += item.get(k, 0)
        total["cost_usd"] += item.get("cost_usd", 0.0)
        total["latency_sum_ms"] += item.get("latency_sum_ms", 0)

    total["avg_latency_ms"] = round(total["latency_sum_ms"] / total["invocations"]) if total["invocations"] else 0
    return total


def get_by_model(account_region: str, days: int = 7) -> list[dict]:
    """Get usage grouped by model."""
    g, start, end = _resolve_granularity(account_region, days)

    items = query_usage(account_region, g, start, end, "MODEL#")

    # Aggregate across time periods by model
    models = {}
    for item in items:
        model = item["dimension"].replace("MODEL#", "")
        if model not in models:
            models[model] = {"model": model, "invocations": 0, "input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0}
        for k in ("invocations", "input_tokens", "output_tokens"):
            models[model][k] += item.get(k, 0)
        models[model]["cost_usd"] += item.get("cost_usd", 0.0)

    return sorted(models.values(), key=lambda x: x["cost_usd"], reverse=True)


def get_by_caller(account_region: str, days: int = 7) -> list[dict]:
    """Get usage grouped by caller."""
    g, start, end = _resolve_granularity(account_region, days)

    items = query_usage(account_region, g, start, end, "CALLER#")

    callers = {}
    for item in items:
        caller = item["dimension"].replace("CALLER#", "")
        if caller not in callers:
            callers[caller] = {"caller": caller, "invocations": 0, "input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0}
        for k in ("invocations", "input_tokens", "output_tokens"):
            callers[caller][k] += item.get(k, 0)
        callers[caller]["cost_usd"] += item.get("cost_usd", 0.0)

    return sorted(callers.values(), key=lambda x: x["cost_usd"], reverse=True)


def get_trend(account_region: str, days: int = 7) -> list[dict]:
    """Get time-series trend data (TOTAL per period)."""
    g, start, end = _resolve_granularity(account_region, days)

    items = query_usage(account_region, g, start, end, "TOTAL")
    return sorted(items, key=lambda x: x["period"])


def _extract_dimension(sk: str) -> str:
    """Extract dimension from SK like HOURLY#2026-03-23T05#MODEL#xxx → MODEL#xxx"""
    parts = sk.split("#", 2)
    return parts[2] if len(parts) >= 3 else ""


def get_all_pricing() -> list[dict]:
    """Get current effective price for all models."""
    # Scan all MODEL# items, then pick latest SK per model
    resp = _pricing.scan()
    items = resp.get("Items", [])
    while resp.get("LastEvaluatedKey"):
        resp = _pricing.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp.get("Items", []))

    # Group by PK, pick latest SK (= current effective price)
    models: dict[str, dict] = {}
    for item in items:
        pk = item["PK"]
        if not pk.startswith("MODEL#"):
            continue
        if pk not in models or item["SK"] > models[pk]["SK"]:
            models[pk] = item

    result = []
    for pk, item in sorted(models.items()):
        model_id = pk.replace("MODEL#", "")
        result.append({
            "model_id": model_id,
            "input_per_1k": float(item.get("input_per_1k", 0)),
            "output_per_1k": float(item.get("output_per_1k", 0)),
            "effective_date": item["SK"],
            "source": item.get("source", ""),
        })
    return result


def get_pricing_sync_info() -> dict | None:
    """Get last pricing sync metadata."""
    try:
        resp = _usage.get_item(Key={"PK": "META", "SK": "PRICING_SYNC#latest"})
        return resp.get("Item")
    except Exception:
        return None


def _format_item(item: dict, granularity: str) -> dict:
    """Convert DynamoDB item to clean dict."""
    sk = item["SK"]
    parts = sk.split("#", 2)
    period = parts[1] if len(parts) >= 2 else ""
    dimension = parts[2] if len(parts) >= 3 else ""

    cost_micro = int(item.get("cost_micro_usd", 0))
    invocations = int(item.get("invocations", 0))
    latency_sum = int(item.get("latency_sum_ms", 0))

    return {
        "period": period,
        "dimension": dimension,
        "invocations": invocations,
        "input_tokens": int(item.get("input_tokens", 0)),
        "output_tokens": int(item.get("output_tokens", 0)),
        "cost_usd": cost_micro / 1_000_000,
        "cost_micro_usd": cost_micro,
        "latency_sum_ms": latency_sum,
        "avg_latency_ms": round(latency_sum / invocations) if invocations else 0,
        "max_latency_ms": int(item.get("max_latency_ms", 0)),
    }
