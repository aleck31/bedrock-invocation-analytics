#!/usr/bin/env python3
"""Fetch Bedrock on-demand token pricing from AWS Pricing API and save to pricing.json."""
import boto3
import json
import re
import sys

REGION = sys.argv[1] if len(sys.argv) > 1 else "us-west-2"
PROFILE = sys.argv[2] if len(sys.argv) > 2 else None

# Region code mapping for Pricing API usagetype prefix
REGION_PREFIX = {
    "us-east-1": "USE1", "us-west-2": "USW2", "eu-west-1": "EUW1",
    "ap-southeast-1": "APS1", "ap-northeast-1": "APN1",
}
prefix = REGION_PREFIX.get(REGION, REGION.upper().replace("-", ""))

session = boto3.Session(profile_name=PROFILE) if PROFILE else boto3.Session()
pricing = session.client("pricing", region_name="us-east-1")

# Get all usagetype values for Bedrock
paginator = pricing.get_paginator("get_attribute_values")
usage_types = []
for page in paginator.paginate(ServiceCode="AmazonBedrock", AttributeName="usagetype"):
    for v in page["AttributeValues"]:
        val = v["Value"]
        if val.startswith(prefix) and val.endswith("tokens") and "batch" not in val.lower():
            usage_types.append(val)

# Fetch prices
prices = {}
for ut in usage_types:
    resp = pricing.get_products(
        ServiceCode="AmazonBedrock",
        Filters=[{"Type": "TERM_MATCH", "Field": "usagetype", "Value": ut}],
        MaxResults=1,
    )
    if not resp["PriceList"]:
        continue
    product = json.loads(resp["PriceList"][0])
    terms = product.get("terms", {}).get("OnDemand", {})
    for term in terms.values():
        for dim in term.get("priceDimensions", {}).values():
            usd = float(dim["pricePerUnit"].get("USD", 0))
            if usd > 0:
                # Parse: PREFIX-ModelName-input/output-tokens
                name_part = ut[len(prefix) + 1:]  # Remove "USW2-"
                if "-input-token" in name_part:
                    model_key = name_part.replace("-input-tokens", "").replace("-input-token-count", "")
                    direction = "input"
                elif "-output-token" in name_part:
                    model_key = name_part.replace("-output-tokens", "").replace("-output-token-count", "")
                    direction = "output"
                elif "-text-input-token" in name_part:
                    model_key = name_part.replace("-text-input-tokens", "")
                    direction = "input"
                elif "-text-output-token" in name_part:
                    model_key = name_part.replace("-text-output-tokens", "")
                    direction = "output"
                else:
                    continue

                if model_key not in prices:
                    prices[model_key] = {"input_per_1k": 0, "output_per_1k": 0}
                prices[model_key][f"{direction}_per_1k"] = usd

# Sort and output
result = {
    "_metadata": {
        "region": REGION,
        "source": "AWS Pricing API",
        "note": "Prices in USD per 1K tokens. Add missing models manually."
    },
    "models": dict(sorted(prices.items()))
}

output_path = "pricing.json"
with open(output_path, "w") as f:
    json.dump(result, f, indent=2)

print(f"Saved {len(prices)} models to {output_path}")
for k, v in sorted(prices.items()):
    print(f"  {k}: input=${v['input_per_1k']}/1K, output=${v['output_per_1k']}/1K")
