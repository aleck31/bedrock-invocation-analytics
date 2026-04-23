# Bedrock 调用日志分析

[English](../README.md) | 中文

Amazon Bedrock 实时分析 — 监控多账户的 Token 用量、成本和性能。

## 功能
- 概览卡片：调用次数、输入/输出 Token、预估成本、平均延迟、平均 TPOT
- 按模型和调用者的 Token 用量与成本（图表 / 饼图 / 表格视图）
- 饼图：输入 Token、输出 Token、成本占比
- 性能分析：按模型延迟（min/avg/max）+ 延迟趋势（支持按模型筛选）
- TPOT 分析：按模型 TPOT（min/avg/max）+ CloudWatch TTFT 趋势（avg/p99）
- 使用趋势（支持按模型筛选）
- 自动刷新（5s / 15s / 30s / 1min / 5min）
- 定价设置：查看/编辑模型定价及历史，同步状态
- 登录认证（通过 config.yaml 配置）
- 多账户、多区域支持（侧栏选择器）
- 响应式布局（桌面和移动端）

**截图**

![WebUI](webui_screenshot.png)

## 项目结构

```
├── deploy/
│   ├── cdk.json              # CDK 配置
│   ├── app.py                # CDK 应用入口（hub/spoke 路由）
│   ├── hub_stack.py          # 主账号 Stack
│   ├── spoke_stack.py        # Spoke 账号 Stack
│   └── lambda/
│       ├── process_log.py    # ETL：S3 事件 → 解析 → DDB 聚合
│       ├── aggregate_stats.py # 汇总：HOURLY → DAILY → MONTHLY
│       └── sync_pricing.py   # 每周从 LiteLLM 同步定价
├── webui/
│   ├── main.py               # 入口（ui.run）
│   ├── dashboard.py          # 仪表盘页面
│   ├── pricing.py            # 定价设置页面
│   └── data.py               # DynamoDB 数据访问层
├── scripts/
│   └── seed_pricing.py       # 从 LiteLLM 导入定价
├── config.example.yaml  # 多账号部署配置
├── deploy.sh                 # CDK 部署脚本（hub/spoke/all）
├── start-webui.sh            # WebUI 启动脚本（读取 .env.deploy）
└── pyproject.toml            # 依赖管理（uv）
```

## 架构

```
┌─────────────────────────────────────────────────────────────────┐
│ 主账号                                                           │
│                                                                 │
│  S3 桶 ──→ EventBridge ──→ Lambda: process_log ──┐              │
│  Bedrock 日志                                    │               │
│                                                  ▼              │
│  DynamoDB: usage-stats  ◄──────────────────── 写入               │
│  DynamoDB: model-pricing ◄── Lambda: sync_pricing（每周）        │
│  Lambda: aggregate_stats（每日/每月汇总）                          │
│  IAM Role: SpokeWriteRole                                       │
│  WebUI ──→ 读取 DynamoDB                                         │
└─────────────────────────────────────────────────────────────────┘
       ▲ assume role
       │
┌──────┴──────────────────────────────────────────────────────────┐
│ 其他账号                                                         │
│                                                                 │
│  S3 桶 ──→ EventBridge ──→ Lambda: process_log                  │
│  Bedrock 日志                     │                              │
│                                   ▼                             │
│                     assume SpokeWriteRole → 主账号 DynamoDB      │
│  SQS DLQ（处理失败的死信队列）                                      │
└─────────────────────────────────────────────────────────────────┘
```

**工作原理：**
1. 每个账号将 Bedrock 调用日志写入各自的 S3 桶（Bedrock 要求同 Region）
2. S3 事件触发 Lambda ETL，解析 Token、延迟、调用者信息，并基于定价数据计算成本
3. 其他账号的 Lambda 通过跨账号 IAM Role 写入主账号的 DynamoDB
4. 统计数据按模型、调用者、汇总维度聚合，支持小时/天/月粒度
5. 每周自动从 [LiteLLM](https://github.com/BerriAI/litellm) 同步模型定价（覆盖 286+ Bedrock 模型）
6. WebUI 从 DynamoDB 读取数据，亚秒级加载所有账号数据

## 前置条件

- [AWS CDK CLI](https://docs.aws.amazon.com/cdk/v2/guide/getting-started.html)（`npm install -g aws-cdk`）
- [uv](https://docs.astral.sh/uv/)（Python 包管理器）
- 已配置 AWS 凭证（`aws configure` 或 `~/.aws/credentials`）

## 部署

在 `config.yaml` 中配置账号信息：

```yaml
log_prefix: bedrock/invocation-logs/

accounts:
  - profile: me
    region: us-west-2
    bucket: my-existing-bucket
    primary: true           # 主账号：完整部署（DynamoDB、定价同步、汇总、WebUI）

  - profile: lab
    region: us-west-2
    bucket: ""              # 空 = 创建新桶
```

标记 `primary: true` 的为主账号，其他账号部署轻量级 Spoke Stack（S3 + Bedrock 日志 + ETL Lambda），数据写入主账号的 DynamoDB。

```bash
# 安装依赖
uv sync

# 部署主账号（自动初始化 CDK）
./deploy.sh hub

# 部署其他账号
./deploy.sh spoke              # 所有 spoke
./deploy.sh spoke lab          # 指定 spoke

# 全部部署（更新代码后推荐使用）
./deploy.sh all
```

> **注意：** 代码更新后，建议使用 `./deploy.sh all` 确保 hub 和 spoke 的 Lambda 同步更新。

> 使用已有存储桶时，需启用 S3 EventBridge 通知：
> ```bash
> aws s3api put-bucket-notification-configuration --bucket 你的桶名 \
>   --notification-configuration '{"EventBridgeConfiguration": {}}'
> ```

### 部署资源

**主账号：**

| 资源 | 用途 |
|------|------|
| Custom Resource | 配置 Bedrock 调用日志 |
| DynamoDB 表 × 2 | 用量统计聚合 + 模型定价 |
| Lambda × 4 | 日志处理 + 统计汇总 + 定价同步 + Bedrock 日志配置 |
| EventBridge × 4 | S3 触发器 + 每日/每月汇总 + 每周定价同步 |
| IAM Role | SpokeWriteRole（跨账号访问） |
| S3 存储桶（可选） | 原始日志 |

**其他账号：**

| 资源 | 用途 |
|------|------|
| Custom Resource | 配置 Bedrock 调用日志 |
| Lambda × 1 | 日志处理（通过 assume role 写入主账号 DynamoDB） |
| EventBridge × 1 | S3 触发器 |
| SQS DLQ | 处理失败的死信队列 |
| S3 存储桶（可选） | 原始日志 |

## 初始化定价数据

定价数据来源于 [LiteLLM](https://github.com/BerriAI/litellm)（覆盖 286+ Bedrock 模型）：

```bash
AWS_DEFAULT_REGION=us-west-2 python3 scripts/seed_pricing.py \
  BedrockInvocationAnalytics-model-pricing YOUR_PROFILE
```

## 启动 WebUI

```bash
./start-webui.sh
```

浏览器打开 http://localhost:8060

## 清理

```bash
./deploy.sh destroy --profile YOUR_PROFILE
```

> DynamoDB 表和 S3 存储桶在删除 Stack 后会保留（RemovalPolicy: RETAIN）。

## 成本

| 服务 | 定价 | 说明 |
|------|------|------|
| DynamoDB | 按请求付费 | 约 $1.25/百万次写入，读取可忽略 |
| Lambda | $0.20/百万次请求 | 每个日志文件约 60ms |
| S3 | 约 $0.023/GB/月 | 90 天后自动转为低频存储 |

**月度估算**（100 万次 Bedrock 调用）：
- Lambda：约 $0.20
- DynamoDB：约 $4（每次调用 3 次写入）
- S3：约 $1（累计日志存储）
- **合计：约 $5/月**
