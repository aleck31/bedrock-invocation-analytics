#!/usr/bin/env python3
"""CDK app entry point. Reads config.yaml to instantiate Hub and Spoke stacks."""

import yaml
import aws_cdk as cdk
from hub_stack import HubStack
from spoke_stack import SpokeStack

with open("../config.yaml") as f:
    config = yaml.safe_load(f)

accounts = config.get("accounts", [])
primary = next((a for a in accounts if a.get("primary")), None)
if not primary:
    raise ValueError("No primary account defined in config.yaml")

spokes = [a for a in accounts if not a.get("primary")]
app = cdk.App()
target = app.node.try_get_context("target")  # "hub", "spoke:<profile>", "all"

# Hub stack
if not target or target == "hub" or target == "all":
    HubStack(app, "BedrockInvocationAnalytics",
        env=cdk.Environment(region=primary["region"]),
    )

# Spoke stacks
if target and (target.startswith("spoke:") or target == "all"):
    hub_account = app.node.try_get_context("hub_account") or ""
    hub_role_arn = f"arn:aws:iam::{hub_account}:role/BedrockAnalytics-SpokeWriteRole"
    usage_table = app.node.try_get_context("usage_table") or "BedrockInvocationAnalytics-usage-stats"
    pricing_table = app.node.try_get_context("pricing_table") or "BedrockInvocationAnalytics-model-pricing"

    target_profile = target.split(":")[1] if target.startswith("spoke:") else None
    for s in spokes:
        if target_profile and s["profile"] != target_profile:
            continue
        stack_id = f"BedrockAnalytics-Spoke-{s['profile']}-{s['region']}"
        SpokeStack(app, stack_id,
            env=cdk.Environment(region=s["region"]),
            hub_account=hub_account,
            hub_role_arn=hub_role_arn,
            usage_stats_table=usage_table,
            model_pricing_table=pricing_table,
        )

app.synth()
