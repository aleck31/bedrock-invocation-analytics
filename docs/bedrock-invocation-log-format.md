# Bedrock Invocation Log Format Reference

Captured from actual logs in `us-west-2` (Apr-May 2026), validated via Athena audit.
Content fields are truncated for readability. Account IDs and identities are scrubbed.

## Storage layout

Bedrock writes one gzipped NDJSON file per batch to:
```
s3://{bucket}/{prefix}AWSLogs/{accountId}/BedrockModelInvocationLogs/{region}/YYYY/MM/DD/HH/*.json.gz
```

Each line is one invocation record. **One exception**: when a request or response body exceeds 25 KB, Bedrock writes it separately to a `/data/` subdirectory:
```
.../YYYY/MM/DD/HH/data/{requestId}_input.json.gz
.../YYYY/MM/DD/HH/data/{requestId}_output.json.gz
```
The main record then carries `inputBodyS3Path` / `outputBodyS3Path` pointing to that file. **ETL pipelines must skip `/data/` and `permission-check` keys** — they aren't invocation records.

## Operation types observed

| operation | body shape | share in sample |
|---|---|---|
| `Converse` | dict | 0.5% |
| `ConverseStream` | **dict** (not list) | 88% |
| `InvokeModel` (Anthropic native JSON) | dict | 9% |
| `InvokeModelWithResponseStream` (Anthropic native JSON) | **list** | 8% |
| `InvokeModel` (embeddings, image models, etc.) | dict | rare |

Key insight: **body shape is tied to the API, not to whether it streams**. `ConverseStream` aggregates its stream into a single dict response in the log. Only `InvokeModelWithResponseStream` stores the raw stream as an array of event chunks.

## Common top-level fields (every operation)

```json
{
  "schemaType": "ModelInvocationLog",
  "schemaVersion": "1.0",
  "timestamp": "2026-04-19T13:28:51Z",
  "accountId": "123456789012",
  "region": "us-west-2",
  "requestId": "7487ab04-734e-470b-8dfc-46d8d8506381",
  "operation": "Converse",
  "modelId": "global.anthropic.claude-opus-4-7-v1",
  "identity": { "arn": "arn:aws:iam::123456789012:user/username" },
  "inferenceRegion": "us-east-2",
  "errorCode": null,

  "input": {
    "inputContentType": "application/json",
    "inputBodyJson": { "...": "..." },
    "inputBodyS3Path": "s3://.../data/{requestId}_input.json.gz",  // only when > 25 KB

    "inputTokenCount": 255,                // uncached input (already excludes cached portion)
    "cacheReadInputTokenCount": 0,         // tokens served from prompt cache
    "cacheWriteInputTokenCount": 0         // total tokens written to cache (5m + 1h combined)
  },

  "output": {
    "outputContentType": "application/json",
    "outputBodyJson": /* dict OR list, depends on operation */,
    "outputTokenCount": 113
  }
}
```

**`modelId`** may appear in short form (`global.anthropic.claude-opus-4-7-v1`) or as a full inference-profile ARN (`arn:aws:bedrock:us-west-2:...:inference-profile/global.anthropic.claude-opus-4-7`). ETL should strip the ARN prefix.

**`errorCode`** is null on success. Our sample had zero error records, but the field exists in the schema — failed invocations should be filtered when aggregating cost.

## Operation-specific body shapes

### Converse and ConverseStream (dict)

```json
"outputBodyJson": {
  "output": "[...truncated...]",
  "stopReason": "end_turn",
  "metrics": { "latencyMs": 3114 },
  "usage": {
    "inputTokens": 255,
    "cacheReadInputTokens": 0,
    "cacheWriteInputTokens": 24479,
    "outputTokens": 113,
    "totalTokens": 368
  }
}
```

- `metrics.latencyMs`: end-to-end latency, present on both sync and stream variants (99%+ coverage)
- `usage.cacheReadInputTokens` / `cacheWriteInputTokens`: mirrors top-level `cacheReadInputTokenCount` / `cacheWriteInputTokenCount`
- **No** `cache_creation.ephemeral_*` subfield — Converse API does not expose 5min vs 1h cache TTL split; if this matters, fall back to treating the total as 5min

### InvokeModel — Anthropic native JSON (dict)

```json
"outputBodyJson": {
  "id": "msg_bdrk_...",
  "type": "message",
  "role": "assistant",
  "model": "claude-sonnet-4-6",
  "content": "[...truncated...]",
  "stop_reason": "end_turn",
  "usage": {
    "input_tokens": 9,
    "cache_creation_input_tokens": 2403,
    "cache_read_input_tokens": 0,
    "cache_creation": {
      "ephemeral_5m_input_tokens": 0,
      "ephemeral_1h_input_tokens": 2403     // ← 1h cache split only exposed here
    },
    "output_tokens": 23
  }
}
```

- Field names use **`snake_case`** (Anthropic native), not the camelCase of Converse
- **`usage.cache_creation.ephemeral_{5m,1h}_input_tokens` is the only place** where 5min vs 1h prompt cache tokens are split out — essential for correct cost (1h cache costs ~1.6x the 5min rate)
- `metrics.latencyMs` is **not** on this shape (different body structure from Converse)

### InvokeModelWithResponseStream — Anthropic native JSON (**list**)

```json
"outputBodyJson": [
  {
    "type": "message_start",
    "message": {
      "id": "msg_bdrk_...",
      "model": "claude-opus-4-7",
      "usage": {
        "input_tokens": 6,
        "cache_creation_input_tokens": 553,
        "cache_read_input_tokens": 178945,
        "cache_creation": {
          "ephemeral_5m_input_tokens": 553,
          "ephemeral_1h_input_tokens": 0
        },
        "output_tokens": 2
      }
    }
  },
  { "type": "content_block_start", "index": 0, ... },
  { "type": "content_block_delta", ... },
  { "type": "content_block_delta", ... },
  // ... many deltas ...
  { "type": "message_stop", ... }
]
```

- `outputBodyJson` is an **array of stream events**, not a single object
- Usage data lives in the **first `message_start` chunk**: `outputBodyJson[0].message.usage.*`
- Has the same `cache_creation.ephemeral_{5m,1h}` split as non-streaming InvokeModel
- **No `latencyMs` anywhere** — stream events don't carry end-to-end latency; use CloudWatch metric `AWS/Bedrock` / `TimeToFirstToken` if needed

## Field coverage matrix

✅ = always present (or ~100% sample)  ·  ○ = present when applicable  ·  ❌ = never present  ·  🔶 = API doesn't expose this split

| Field | Converse | ConverseStream | InvokeModel | InvokeModelWithResponseStream |
|---|---|---|---|---|
| `input.inputTokenCount` | ✅ | ✅ | ✅ | ✅ |
| `input.cacheReadInputTokenCount` | ✅ | ✅ | ✅ | ✅ |
| `input.cacheWriteInputTokenCount` | ✅ | ✅ | ✅ | ✅ |
| `output.outputTokenCount` | ✅ | ✅ | ✅ | ✅ |
| `output.outputBodyJson` type | **dict** | **dict** | dict | **list** |
| `usage.inputTokens` (camelCase) | ✅ | ✅ | ❌ | ❌ |
| `usage.input_tokens` (snake_case) | ❌ | ❌ | ✅ | ✅ (at `[0].message.usage`) |
| `cache_creation.ephemeral_5m_input_tokens` | 🔶 | 🔶 | ✅ | ✅ (at `[0].message.usage`) |
| `cache_creation.ephemeral_1h_input_tokens` | 🔶 | 🔶 | ✅ | ✅ (at `[0].message.usage`) |
| `metrics.latencyMs` | ✅ | ✅ | ❌ | ❌ |

## Token semantics (important)

These are **three non-overlapping** token counts that sum to the total input:

```
total input = inputTokenCount          # uncached fresh input
            + cacheReadInputTokenCount # served from cache
            + cacheWriteInputTokenCount  # written to cache (this invocation pays to create it)
```

`inputTokenCount` is **already** the uncached portion — don't subtract cache read/write from it. Verified against Anthropic's `usage.input_tokens` / `cache_read_input_tokens` / `cache_creation_input_tokens` on 2026-05-07.

For Anthropic models, `cache_creation` further splits `cacheWriteInputTokenCount` into 5min-TTL vs 1h-TTL portions:
```
cacheWriteInputTokenCount = cache_creation.ephemeral_5m_input_tokens
                          + cache_creation.ephemeral_1h_input_tokens
```

## Key fields for ETL

| Field | Location | Notes |
|-------|----------|-------|
| `modelId` | top-level | May be ARN; strip prefix to short form |
| `requestId` | top-level | Natural dedup key; use as Firehose unique key into Iceberg |
| `accountId`, `region` | top-level | Also parseable from S3 key path (use S3 path as fallback) |
| `timestamp` | top-level | ISO-8601; parse to hour for aggregation |
| `identity.arn` | top-level | Caller IAM ARN; parse to username/role label |
| `operation` | top-level | Distinguish API type for body parsing |
| `errorCode` | top-level | Non-null ⇒ failed invocation, skip when aggregating cost |
| `inputTokenCount` | `input.` | Uncached input tokens |
| `cacheReadInputTokenCount` | `input.` | Tokens served from cache |
| `cacheWriteInputTokenCount` | `input.` | Total cache write (5m + 1h) |
| `outputTokenCount` | `output.` | Output tokens |
| 5m / 1h cache split | `output.outputBodyJson.usage.cache_creation.ephemeral_*` (InvokeModel), `outputBodyJson[0].message.usage.cache_creation.ephemeral_*` (WithResponseStream) | **Anthropic only.** Required to price 1h cache correctly. |
| `latencyMs` | `output.outputBodyJson.metrics.latencyMs` | Converse/ConverseStream only. Missing on both InvokeModel variants. |

## Parse strategy

**Branch on body shape, not on operation**, for forward compatibility:

```python
body = record["output"].get("outputBodyJson")
if isinstance(body, dict):
    usage = body.get("usage", {})
    metrics = body.get("metrics", {})
elif isinstance(body, list):
    # Walk chunks until we find a recognizable usage carrier
    for chunk in body:
        if not isinstance(chunk, dict):
            continue
        if chunk.get("type") == "message_start":           # Anthropic
            usage = chunk.get("message", {}).get("usage", {})
            break
        if "metadata" in chunk and "usage" in chunk["metadata"]:  # future-proof
            usage = chunk["metadata"]["usage"]
            break
```

This way, a future API that streams with a new chunk shape only needs a new `elif`.

## References

- AWS docs: [Monitor model invocation with Amazon Bedrock](https://docs.aws.amazon.com/bedrock/latest/userguide/model-invocation-logging.html)
