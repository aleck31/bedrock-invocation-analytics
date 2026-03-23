"""CDK Stack for Bedrock Invocation Logging Analytics."""

from aws_cdk import (
    Stack,
    CfnParameter,
    CfnOutput,
    Duration,
    RemovalPolicy,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as _lambda,
    aws_s3 as s3,
)
from constructs import Construct


class BedrockLoggingAnalyticsStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # ── Parameters ──
        existing_bucket_name = CfnParameter(
            self, "ExistingBucketName",
            type="String",
            description="S3 bucket name where Bedrock invocation logs are stored.",
        )
        log_prefix = CfnParameter(
            self, "LogPrefix",
            type="String",
            default="bedrock/invocation-logs/",
            description="S3 key prefix for invocation logs.",
        )

        bucket_name = existing_bucket_name.value_as_string
        bucket = s3.Bucket.from_bucket_name(self, "LogsBucket", bucket_name)
        # NOTE: S3 EventBridge notifications must be enabled on the bucket.
        # For imported buckets, CDK cannot set this automatically.
        # Run: aws s3api put-bucket-notification-configuration --bucket <name> \
        #   --notification-configuration '{"EventBridgeConfiguration": {}}'

        # ── DynamoDB Tables ──
        usage_stats_table = dynamodb.Table(
            self, "UsageStatsTable",
            table_name=f"{id}-usage-stats",
            partition_key=dynamodb.Attribute(name="PK", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="SK", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
            time_to_live_attribute="ttl",
        )

        model_pricing_table = dynamodb.Table(
            self, "ModelPricingTable",
            table_name=f"{id}-model-pricing",
            partition_key=dynamodb.Attribute(name="PK", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="SK", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # ── Lambda: Process each new log file ──
        process_log_fn = _lambda.Function(
            self, "ProcessLogFunction",
            function_name=f"{id}-process-log",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="process_log.handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.seconds(60),
            memory_size=256,
            environment={
                "USAGE_STATS_TABLE": usage_stats_table.table_name,
                "MODEL_PRICING_TABLE": model_pricing_table.table_name,
            },
        )

        bucket.grant_read(process_log_fn)
        usage_stats_table.grant_read_write_data(process_log_fn)
        model_pricing_table.grant_read_data(process_log_fn)

        # ── EventBridge: S3 Object Created → Process Log ──
        events.Rule(
            self, "NewLogFileTrigger",
            rule_name=f"{id}-new-log-trigger",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [bucket_name]},
                    "object": {"key": [{"suffix": ".json.gz"}]},
                },
            ),
            targets=[targets.LambdaFunction(process_log_fn)],
        )

        # ── Lambda: Aggregate hourly→daily→monthly ──
        aggregate_stats_fn = _lambda.Function(
            self, "AggregateStatsFunction",
            function_name=f"{id}-aggregate-stats",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="aggregate_stats.handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.seconds(300),
            memory_size=256,
            environment={
                "USAGE_STATS_TABLE": usage_stats_table.table_name,
            },
        )
        usage_stats_table.grant_read_write_data(aggregate_stats_fn)

        # Daily: summarize yesterday's HOURLY → DAILY (00:15 UTC)
        events.Rule(
            self, "DailyAggregateSchedule",
            rule_name=f"{id}-daily-aggregate",
            schedule=events.Schedule.cron(minute="15", hour="0"),
            targets=[targets.LambdaFunction(aggregate_stats_fn, event=events.RuleTargetInput.from_object({"type": "daily"}))],
        )
        # Monthly: summarize last month's DAILY → MONTHLY (1st at 01:00 UTC)
        events.Rule(
            self, "MonthlyAggregateSchedule",
            rule_name=f"{id}-monthly-aggregate",
            schedule=events.Schedule.cron(minute="0", hour="1", day="1"),
            targets=[targets.LambdaFunction(aggregate_stats_fn, event=events.RuleTargetInput.from_object({"type": "monthly"}))],
        )

        # ── Outputs ──
        CfnOutput(self, "UsageStatsTableName", value=usage_stats_table.table_name)
        CfnOutput(self, "ModelPricingTableName", value=model_pricing_table.table_name)
        CfnOutput(self, "ProcessLogFunctionName", value=process_log_fn.function_name)
        CfnOutput(self, "AggregateStatsFunctionName", value=aggregate_stats_fn.function_name)
