# [IaC] Bedrock 调用日志分析方案

[English](README.md) | 中文

一键部署 Amazon Bedrock 调用日志记录，通过 Amazon Athena 分析 Token 消耗与费用。

## 架构

```
Bedrock API 调用 → 调用日志 → S3 (JSON.gz) → Athena (SQL 查询)
```

**部署资源：**
- AWS Lambda (自定义资源) — 配置 Bedrock 调用日志
- IAM Role — Lambda 执行角色，具备 Bedrock 日志配置权限
- Athena WorkGroup — 专用工作组，含查询结果位置和 10GB 扫描限制
- Athena Named Queries — 7 个预置分析查询模板
- S3 Bucket (可选) — 含 AES256 加密、公开访问阻止和生命周期策略

## 部署

> **注意：** 请将下方 `aleck31/bedrock-logging-analytics/main` 替换为你的实际 GitHub 仓库路径。

| 区域 | 部署 |
|------|------|
| us-west-2 (俄勒冈) | [![Launch Stack](https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png)](https://us-west-2.console.aws.amazon.com/cloudformation/home?region=us-west-2#/stacks/create/review?stackName=bedrock-logging-analytics&templateURL=https://raw.githubusercontent.com/aleck31/bedrock-logging-analytics/main/cf-deploy-template.yaml) |
| us-east-1 (弗吉尼亚北部) | [![Launch Stack](https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png)](https://us-east-1.console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/create/review?stackName=bedrock-logging-analytics&templateURL=https://raw.githubusercontent.com/aleck31/bedrock-logging-analytics/main/cf-deploy-template.yaml) |

## 参数说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| UseExistingBucket | No | `Yes` 使用已有 S3 存储桶，`No` 创建新桶 |
| ExistingBucketName | — | 选择 Yes 时必填，该桶需允许 Bedrock 执行 `s3:PutObject` |
| LogPrefix | `bedrock/invocation-logs/` | S3 日志前缀 |
| LogRetentionDays | 365 | 日志保留天数（仅新建桶生效） |
| AthenaDbName | `bedrock_analytics` | Athena 数据库名称 |

## 部署后配置

1. 打开 **Athena 控制台**，选择工作组 `bedrock-logging-analytics-workgroup`
2. 进入 **已保存的查询** 标签页
3. 先运行 `bedrock-logging-analytics-create-database`
4. 再运行 `bedrock-logging-analytics-create-table` 创建分区表
5. 开始使用预置的分析查询

## 预置查询

| 查询名称 | 说明 |
|----------|------|
| token-usage-by-model | 按模型统计 Token 消耗和平均延迟（近 7 天） |
| estimated-cost-by-model | 按模型估算费用（近 30 天） |
| usage-by-caller | 按 IAM 用户/角色统计用量，用于费用分摊 |
| hourly-trend | 按小时统计调用和 Token 趋势（近 24 小时） |
| daily-trend | 按天统计调用和 Token 趋势（近 30 天） |
| high-latency-calls | 延迟超过 5 秒的调用（近 7 天） |

## CLI 部署

```bash
# 创建新桶
aws cloudformation create-stack \
  --stack-name bedrock-logging-analytics \
  --template-body file://cf-deploy-template.yaml \
  --parameters ParameterKey=UseExistingBucket,ParameterValue=No \
  --capabilities CAPABILITY_IAM \
  --region us-west-2

# 使用已有桶
aws cloudformation create-stack \
  --stack-name bedrock-logging-analytics \
  --template-body file://cf-deploy-template.yaml \
  --parameters ParameterKey=UseExistingBucket,ParameterValue=Yes \
               ParameterKey=ExistingBucketName,ParameterValue=你的桶名 \
  --capabilities CAPABILITY_IAM \
  --region us-west-2
```

## 清理

```bash
aws cloudformation delete-stack --stack-name bedrock-logging-analytics --region us-west-2
```

> 删除堆栈会关闭 Bedrock 调用日志。如果创建了新的 S3 桶，该桶会被保留（DeletionPolicy: Retain），需手动删除。

## 费用

- **S3 存储** — 标准存储费率（约 $0.023/GB/月）
- **Athena** — $5/TB 扫描量（分区投影可大幅减少扫描量）
- **Lambda** — 仅在堆栈创建/更新/删除时调用（可忽略不计）
