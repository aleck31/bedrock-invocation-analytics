"""CDK Hub Stack for Bedrock Invocation Analytics."""

from aws_cdk import (
    Stack,
    CfnCondition,
    CfnParameter,
    CfnOutput,
    CustomResource,
    Duration,
    Fn,
    RemovalPolicy,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_s3 as s3,
)
from constructs import Construct

BEDROCK_LOGGING_CR_CODE = """
import boto3, json, urllib3
http = urllib3.PoolManager()
def send(event, ctx, status, data={}):
    try:
        body = json.dumps({'Status': status, 'Reason': str(data.get('Error','')),
            'PhysicalResourceId': ctx.log_stream_name, 'StackId': event['StackId'],
            'RequestId': event['RequestId'], 'LogicalResourceId': event['LogicalResourceId'], 'Data': data})
        resp = http.request('PUT', event['ResponseURL'], headers={'content-type':'','content-length':str(len(body))}, body=body)
        print(f'cfn response status: {resp.status}')
    except Exception as e:
        print(f'Failed to send cfn response: {e}')
def handler(event, context):
    try:
        client = boto3.client('bedrock')
        rt = event['RequestType']
        props = event['ResourceProperties']
        if rt in ('Create', 'Update'):
            client.put_model_invocation_logging_configuration(loggingConfig={
                's3Config': {'bucketName': props['BucketName'], 'keyPrefix': props['KeyPrefix']},
                'textDataDeliveryEnabled': True, 'imageDataDeliveryEnabled': True, 'embeddingDataDeliveryEnabled': True,
            })
        elif rt == 'Delete':
            try: client.delete_model_invocation_logging_configuration()
            except: pass
        send(event, context, 'SUCCESS')
    except Exception as e:
        print(e)
        send(event, context, 'FAILED', {'Error': str(e)})
"""


class HubStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # ── Parameters ──
        existing_bucket_name = CfnParameter(
            self, "ExistingBucketName",
            type="String",
            default="",
            description="Leave empty to create a new bucket, or specify an existing bucket name.",
        )
        log_prefix = CfnParameter(
            self, "LogPrefix",
            type="String",
            default="bedrock/invocation-logs/",
            description="S3 key prefix for invocation logs.",
        )
        log_retention_days = CfnParameter(
            self, "LogRetentionDays",
            type="Number",
            default=365,
            description="Days before logs expire (only for newly created bucket).",
        )

        # ── Condition: create new bucket or use existing ──
        create_new = CfnCondition(self, "CreateNewBucket",
            expression=Fn.condition_equals(existing_bucket_name.value_as_string, ""),
        )

        # ── S3 Bucket (conditional) ──
        new_bucket = s3.CfnBucket(
            self, "LogsBucket",
            bucket_name=f"bedrock-logs-{self.account}-{self.region}",
            bucket_encryption=s3.CfnBucket.BucketEncryptionProperty(
                server_side_encryption_configuration=[
                    s3.CfnBucket.ServerSideEncryptionRuleProperty(
                        server_side_encryption_by_default=s3.CfnBucket.ServerSideEncryptionByDefaultProperty(
                            sse_algorithm="AES256",
                        ),
                    ),
                ],
            ),
            public_access_block_configuration=s3.CfnBucket.PublicAccessBlockConfigurationProperty(
                block_public_acls=True, block_public_policy=True,
                ignore_public_acls=True, restrict_public_buckets=True,
            ),
            notification_configuration=s3.CfnBucket.NotificationConfigurationProperty(
                event_bridge_configuration=s3.CfnBucket.EventBridgeConfigurationProperty(event_bridge_enabled=True),
            ),
            lifecycle_configuration=s3.CfnBucket.LifecycleConfigurationProperty(
                rules=[s3.CfnBucket.RuleProperty(
                    id="TransitionAndExpire", status="Enabled",
                    prefix=log_prefix.value_as_string,
                    transitions=[s3.CfnBucket.TransitionProperty(
                        storage_class="STANDARD_IA", transition_in_days=90,
                    )],
                    expiration_in_days=log_retention_days.value_as_number,
                )],
            ),
        )
        new_bucket.cfn_options.condition = create_new
        new_bucket.apply_removal_policy(RemovalPolicy.RETAIN)

        # Bucket policy for Bedrock logging
        bucket_policy = s3.CfnBucketPolicy(
            self, "LogsBucketPolicy",
            bucket=new_bucket.ref,
            policy_document={
                "Version": "2012-10-17",
                "Statement": [{
                    "Sid": "AllowBedrockLogging",
                    "Effect": "Allow",
                    "Principal": {"Service": "bedrock.amazonaws.com"},
                    "Action": "s3:PutObject",
                    "Resource": Fn.join("", ["arn:aws:s3:::", new_bucket.ref, "/", log_prefix.value_as_string, "*"]),
                    "Condition": {"StringEquals": {"aws:SourceAccount": self.account}},
                }],
            },
        )
        bucket_policy.cfn_options.condition = create_new

        # Resolve bucket name
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

        # ── DynamoDB Tables ──
        usage_stats_table = dynamodb.Table(self, "UsageStatsTable",
            table_name=f"{id}-usage-stats",
            partition_key=dynamodb.Attribute(name="PK", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="SK", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
            time_to_live_attribute="ttl",
        )
        model_pricing_table = dynamodb.Table(self, "ModelPricingTable",
            table_name=f"{id}-model-pricing",
            partition_key=dynamodb.Attribute(name="PK", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="SK", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # ── Lambda: Process each new log file ──
        process_log_fn = _lambda.Function(self, "ProcessLogFunction",
            function_name=f"{id}-process-log",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="process_log.handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.seconds(60), memory_size=256,
            environment={
                "USAGE_STATS_TABLE": usage_stats_table.table_name,
                "MODEL_PRICING_TABLE": model_pricing_table.table_name,
            },
        )
        bucket.grant_read(process_log_fn)
        usage_stats_table.grant_read_write_data(process_log_fn)
        model_pricing_table.grant_read_data(process_log_fn)

        # ── EventBridge: S3 Object Created → Process Log ──
        events.Rule(self, "NewLogFileTrigger",
            rule_name=f"{id}-new-log-trigger",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [bucket_name_resolved]},
                    "object": {"key": [{"suffix": ".json.gz"}]},
                },
            ),
            targets=[targets.LambdaFunction(process_log_fn)],
        )

        # ── Lambda: Aggregate hourly→daily→monthly ──
        aggregate_stats_fn = _lambda.Function(self, "AggregateStatsFunction",
            function_name=f"{id}-aggregate-stats",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="aggregate_stats.handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.seconds(300), memory_size=256,
            environment={"USAGE_STATS_TABLE": usage_stats_table.table_name},
        )
        usage_stats_table.grant_read_write_data(aggregate_stats_fn)

        events.Rule(self, "DailyAggregateSchedule",
            rule_name=f"{id}-daily-aggregate",
            schedule=events.Schedule.cron(minute="15", hour="0"),
            targets=[targets.LambdaFunction(aggregate_stats_fn, event=events.RuleTargetInput.from_object({"type": "daily"}))],
        )
        events.Rule(self, "MonthlyAggregateSchedule",
            rule_name=f"{id}-monthly-aggregate",
            schedule=events.Schedule.cron(minute="0", hour="1", day="1"),
            targets=[targets.LambdaFunction(aggregate_stats_fn, event=events.RuleTargetInput.from_object({"type": "monthly"}))],
        )

        # ── Lambda: Sync pricing from LiteLLM (weekly) ──
        sync_pricing_fn = _lambda.Function(self, "SyncPricingFunction",
            function_name=f"{id}-sync-pricing",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="sync_pricing.handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.seconds(120), memory_size=256,
            environment={
                "MODEL_PRICING_TABLE": model_pricing_table.table_name,
                "USAGE_STATS_TABLE": usage_stats_table.table_name,
            },
        )
        model_pricing_table.grant_read_write_data(sync_pricing_fn)
        usage_stats_table.grant_write_data(sync_pricing_fn)

        events.Rule(self, "WeeklyPricingSyncSchedule",
            rule_name=f"{id}-weekly-pricing-sync",
            schedule=events.Schedule.cron(minute="0", hour="21", week_day="SUN"),
            targets=[targets.LambdaFunction(sync_pricing_fn)],
        )

        # ── Class name update ──
        self.usage_stats_table = usage_stats_table
        self.model_pricing_table = model_pricing_table

        # ── IAM Role for Spoke Lambdas (cross-account) ──
        spoke_accounts_str = self.node.try_get_context("spoke_accounts") or ""
        spoke_accounts = [a for a in spoke_accounts_str.split(",") if a]
        if spoke_accounts:
            spoke_write_role = iam.Role(self, "SpokeWriteRole",
                role_name="BedrockAnalytics-SpokeWriteRole",
                assumed_by=iam.CompositePrincipal(*[
                    iam.ArnPrincipal(f"arn:aws:iam::{acct}:root")
                    for acct in spoke_accounts
                ]),
            )
            # Usage stats: read + write
            usage_stats_table.grant_read_write_data(spoke_write_role)
            # Pricing: read only
            model_pricing_table.grant_read_data(spoke_write_role)

            CfnOutput(self, "SpokeWriteRoleArn", value=spoke_write_role.role_arn)

        # ── WebUI deployment (TODO: evaluate lightweight hosting options) ──

        # ── Outputs ──
        CfnOutput(self, "BucketName", value=bucket_name_resolved)
        CfnOutput(self, "UsageStatsTableName", value=usage_stats_table.table_name)
        CfnOutput(self, "ModelPricingTableName", value=model_pricing_table.table_name)
        CfnOutput(self, "ProcessLogFunctionName", value=process_log_fn.function_name)
        CfnOutput(self, "AggregateStatsFunctionName", value=aggregate_stats_fn.function_name)
