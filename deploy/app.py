#!/usr/bin/env python3
import aws_cdk as cdk
from stack import BedrockLoggingAnalyticsStack

app = cdk.App()
BedrockLoggingAnalyticsStack(app, "BedrockLoggingAnalytics")
app.synth()
