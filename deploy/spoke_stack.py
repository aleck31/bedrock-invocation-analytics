"""CDK Spoke Stack: S3 + Bedrock logging + ETL Lambda (writes to Hub DynamoDB)."""

from aws_cdk import (
    Stack, CfnCondition, CfnParameter, CfnOutput, CustomResource,
    Duration, Fn, RemovalPolicy,
    aws_events as events, aws_events_targets as targets,
    aws_iam as iam, aws_lambda as _lambda, aws_s3 as s3, aws_sqs as sqs,
)
from constructs import Construct

# Reuse the same Custom Resource code from hub
from hub_stack import BEDROCK_LOGGING_CR_CODE


class SpokeStack(Stack):
    def __init__(self, scope: Construct, id: str, hub_account: str, hub_role_arn: str,
                 usage_stats_table: str, model_pricing_table: str,
                 hub_firehose_name: str = "", hub_region: str = "",
                 **kwargs):
        super().__init__(scope, id, **kwargs)

        # ── Parameters ──
        existing_bucket_name = CfnParameter(self, "ExistingBucketName",
            type="String", default="",
            description="Leave empty to create a new bucket, or specify an existing bucket name.",
        )
        log_prefix = CfnParameter(self, "LogPrefix",
            type="String", default="bedrock/invocation-logs/",
            description="S3 key prefix for invocation logs.",
        )

        # ── S3 Bucket (conditional) ──
        create_new = CfnCondition(self, "CreateNewBucket",
            expression=Fn.condition_equals(existing_bucket_name.value_as_string, ""),
        )
        new_bucket = s3.CfnBucket(self, "LogsBucket",
            bucket_name=f"bedrock-logs-{self.account}-{self.region}",
            bucket_encryption=s3.CfnBucket.BucketEncryptionProperty(
                server_side_encryption_configuration=[s3.CfnBucket.ServerSideEncryptionRuleProperty(
                    server_side_encryption_by_default=s3.CfnBucket.ServerSideEncryptionByDefaultProperty(sse_algorithm="AES256"),
                )],
            ),
            public_access_block_configuration=s3.CfnBucket.PublicAccessBlockConfigurationProperty(
                block_public_acls=True, block_public_policy=True,
                ignore_public_acls=True, restrict_public_buckets=True,
            ),
            notification_configuration=s3.CfnBucket.NotificationConfigurationProperty(
                event_bridge_configuration=s3.CfnBucket.EventBridgeConfigurationProperty(event_bridge_enabled=True),
            ),
        )
        new_bucket.cfn_options.condition = create_new
        new_bucket.apply_removal_policy(RemovalPolicy.RETAIN)

        bucket_policy = s3.CfnBucketPolicy(self, "LogsBucketPolicy",
            bucket=new_bucket.ref,
            policy_document={"Version": "2012-10-17", "Statement": [{
                "Sid": "AllowBedrockLogging", "Effect": "Allow",
                "Principal": {"Service": "bedrock.amazonaws.com"},
                "Action": "s3:PutObject",
                "Resource": Fn.join("", ["arn:aws:s3:::", new_bucket.ref, "/", log_prefix.value_as_string, "*"]),
                "Condition": {"StringEquals": {"aws:SourceAccount": self.account}},
            }]},
        )
        bucket_policy.cfn_options.condition = create_new

        bucket_name_resolved = Fn.condition_if(
            create_new.logical_id, new_bucket.ref, existing_bucket_name.value_as_string,
        ).to_string()
        bucket = s3.Bucket.from_bucket_name(self, "ResolvedBucket", bucket_name_resolved)

        # ── Bedrock Logging Custom Resource ──
        logging_role = iam.Role(self, "BedrockLoggingRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")],
            inline_policies={"bedrock": iam.PolicyDocument(statements=[
                iam.PolicyStatement(actions=[
                    "bedrock:PutModelInvocationLoggingConfiguration",
                    "bedrock:GetModelInvocationLoggingConfiguration",
                    "bedrock:DeleteModelInvocationLoggingConfiguration",
                ], resources=["*"]),
            ])},
        )
        logging_fn = _lambda.Function(self, "BedrockLoggingFunction",
            function_name=f"{id}-bedrock-invocation-setup",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="index.handler", timeout=Duration.seconds(30),
            role=logging_role,
            code=_lambda.Code.from_inline(BEDROCK_LOGGING_CR_CODE),
        )
        CustomResource(self, "BedrockLogging",
            service_token=logging_fn.function_arn,
            properties={"BucketName": bucket_name_resolved, "KeyPrefix": log_prefix.value_as_string},
        )

        # ── DLQ ──
        dlq = sqs.Queue(self, "ProcessLogDLQ",
            queue_name=f"{id}-process-log-dlq",
            retention_period=Duration.days(14),
        )

        # ── Lambda: Process Log (writes to Hub DynamoDB via assume role) ──
        process_log_fn = _lambda.Function(self, "ProcessLogFunction",
            function_name=f"{id}-process-log",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="process_log.handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.seconds(60), memory_size=256,
            dead_letter_queue=dlq,
            environment={
                "USAGE_STATS_TABLE": usage_stats_table,
                "MODEL_PRICING_TABLE": model_pricing_table,
                "HUB_ROLE_ARN": hub_role_arn,
            },
        )
        bucket.grant_read(process_log_fn)
        # Allow Lambda to assume hub's SpokeWriteRole
        process_log_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["sts:AssumeRole"],
            resources=[hub_role_arn],
        ))

        # ── EventBridge: S3 Object Created → Process Log ──
        events.Rule(self, "NewLogFileTrigger",
            rule_name=f"{id}-new-log-trigger",
            event_pattern=events.EventPattern(
                source=["aws.s3"], detail_type=["Object Created"],
                detail={"bucket": {"name": [bucket_name_resolved]}, "object": {"key": [{"suffix": ".json.gz"}]}},
            ),
            targets=[targets.LambdaFunction(process_log_fn)],
        )

        # ── V3: Parse Log Lambda → Hub Firehose (deployed but trigger disabled) ──
        if hub_firehose_name:
            parse_log_fn = _lambda.Function(self, "ParseLogFunction",
                function_name=f"{id}-parse-log",
                runtime=_lambda.Runtime.PYTHON_3_13,
                handler="parse_log.handler",
                code=_lambda.Code.from_asset("lambda"),
                timeout=Duration.seconds(60), memory_size=256,
                dead_letter_queue=dlq,
                environment={
                    "FIREHOSE_STREAM": hub_firehose_name,
                    "HUB_ROLE_ARN": hub_role_arn,
                    "HUB_REGION": hub_region or self.region,
                    "SCHEMA_VERSION": "1",
                },
            )
            bucket.grant_read(parse_log_fn)
            # Assume hub SpokeWriteRole (role already grants firehose:PutRecord* on hub side)
            parse_log_fn.add_to_role_policy(iam.PolicyStatement(
                actions=["sts:AssumeRole"], resources=[hub_role_arn],
            ))

            # V3 trigger — disabled until Step 5 cutover
            events.Rule(self, "NewLogFileTriggerV3",
                rule_name=f"{id}-new-log-trigger-v3",
                enabled=False,
                event_pattern=events.EventPattern(
                    source=["aws.s3"], detail_type=["Object Created"],
                    detail={"bucket": {"name": [bucket_name_resolved]}, "object": {"key": [{"suffix": ".json.gz"}]}},
                ),
                targets=[targets.LambdaFunction(parse_log_fn)],
            )
            CfnOutput(self, "ParseLogFunctionName", value=parse_log_fn.function_name)

        # ── Outputs ──
        CfnOutput(self, "BucketName", value=bucket_name_resolved)
        CfnOutput(self, "ProcessLogFunctionName", value=process_log_fn.function_name)
