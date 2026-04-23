# Bedrock Invocation Log Format Reference

Captured from actual logs (April 2026). Content fields truncated for readability.

## Converse

```json
{
  "timestamp": "2026-04-19T13:28:51Z",
  "accountId": "123456789012",
  "region": "us-west-2",
  "requestId": "7487ab04-734e-470b-8dfc-46d8d8506381",
  "operation": "Converse",
  "modelId": "global.anthropic.claude-opus-4-5-20251101-v1:0",
  "input": {
    "inputContentType": "application/json",
    "inputBodyJson": {
      "messages": "[...truncated...]"
    },
    "inputTokenCount": 255,
    "cacheReadInputTokenCount": 0,
    "cacheWriteInputTokenCount": 0
  },
  "output": {
    "outputContentType": "application/json",
    "outputBodyJson": {
      "output": "[...truncated...]",
      "stopReason": "end_turn",
      "metrics": {
        "latencyMs": 3114
      },
      "usage": {
        "inputTokens": 255,
        "cacheReadInputTokens": 0,
        "cacheWriteInputTokens": 0,
        "outputTokens": 113,
        "totalTokens": 368
      }
    },
    "outputTokenCount": 113
  },
  "identity": {
    "arn": "arn:aws:iam::123456789012:user/your-iam-username"
  },
  "inferenceRegion": "us-east-2",
  "schemaType": "ModelInvocationLog",
  "schemaVersion": "1.0"
}
```

## ConverseStream

```json
{
  "timestamp": "2026-04-19T23:57:01Z",
  "accountId": "123456789012",
  "region": "us-west-2",
  "requestId": "5346af27-8f56-4fb8-a6de-058be7a50818",
  "operation": "ConverseStream",
  "modelId": "global.anthropic.claude-opus-4-6-v1",
  "input": {
    "inputContentType": "application/json",
    "inputBodyJson": {
      "messages": "[...truncated...]"
    },
    "inputTokenCount": 3,
    "cacheReadInputTokenCount": 0,
    "cacheWriteInputTokenCount": 24479
  },
  "output": {
    "outputContentType": "application/json",
    "outputBodyJson": {
      "output": "[...truncated...]",
      "stopReason": "end_turn",
      "metrics": {
        "latencyMs": 7460
      },
      "usage": {
        "inputTokens": 3,
        "cacheReadInputTokens": 0,
        "cacheWriteInputTokens": 24479,
        "outputTokens": 52,
        "totalTokens": 24534
      }
    },
    "outputTokenCount": 52
  },
  "identity": {
    "arn": "arn:aws:iam::123456789012:user/your-br-api-key"
  },
  "inferenceRegion": "us-east-2",
  "schemaType": "ModelInvocationLog",
  "schemaVersion": "1.0"
}
```

## Key Fields for ETL

| Field | Location | Notes |
|-------|----------|-------|
| `modelId` | top-level | May be ARN format, needs normalization |
| `inputTokenCount` | `input.` | Non-cache input tokens |
| `outputTokenCount` | `output.` | Output tokens |
| `cacheReadInputTokenCount` | `input.` | ✅ Primary source for cache read tokens (all API types) |
| `cacheWriteInputTokenCount` | `input.` | ✅ Primary source for cache write tokens (all API types) |
| `cacheReadInputTokens` | `output.outputBodyJson.usage.` | Fallback (Converse API only, not streaming) |
| `cacheWriteInputTokens` | `output.outputBodyJson.usage.` | Fallback (Converse API only, not streaming) |
| `latencyMs` | `output.outputBodyJson.metrics.` | E2E latency (not available for streaming) |
| `identity.arn` | top-level | Caller IAM ARN |

### Notes

- `outputBodyJson` is a **dict** for Converse/InvokeModel, but a **list** for streaming APIs
- Cache token fields in `input.` top-level are the most reliable source (present in all API types)
- `latencyMs` is missing for streaming responses (outputBodyJson is a list of events)

## InvokeModelWithResponseStream (with cache tokens)

```json
{
  "timestamp": "2026-04-20T00:17:33Z",
  "accountId": "565521294060",
  "region": "us-west-2",
  "requestId": "f3ae32b1-ae22-48f1-ba9c-d8075737f52a",
  "operation": "InvokeModelWithResponseStream",
  "modelId": "arn:aws:bedrock:us-west-2:565521294060:inference-profile/global.anthropic.claude-opus-4-7",
  "input": {
    "inputContentType": "application/json",
    "inputBodyS3Path": "s3://bedrock-logs-565521294060-us-west-2/bedrock/invocation-logs/AWSLogs/565521294060/BedrockModelInvocationLogs/us-west-2/2026/04/20/00/data/f3ae32b1-ae22-48f1-ba9c-d8075737f52a_input.json.gz",
    "inputTokenCount": 6,
    "cacheReadInputTokenCount": 0,
    "cacheWriteInputTokenCount": 225913
  },
  "output": {
    "outputContentType": "application/json",
    "outputBodyJson": [
      "[...truncated stream events...]"
    ],
    "outputTokenCount": 167
  },
  "identity": {
    "arn": "arn:aws:iam::565521294060:user/your-br-api-key"
  },
  "inferenceRegion": "us-east-1",
  "schemaType": "ModelInvocationLog",
  "schemaVersion": "1.0"
}
```
