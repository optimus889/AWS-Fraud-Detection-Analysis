**[English](README.md) | [简体中文](README_zh_CN.md) | [繁體中文](README_zh_TW.md)**

# AWS 欺诈检测分析

基于 AWS 构建的实时欺诈检测流水线，集成离线 XGBoost 模型训练、实时交易流推理、自动告警与交互式可视化。

---

## 作者

**范明宇、李政洋、王韦辰** — 东北大学信息学研究生  
项目仓库：[AWS-Fraud-Detection-Analysis](https://github.com/optimus889/AWS-Fraud-Detection-Analysis)

---

## 项目概述

本项目使用 AWS 托管服务实现端到端欺诈检测系统。使用合成交易数据集在 SageMaker 上训练 XGBoost 分类器；实时交易通过 Kinesis 流入，由 Lambda 调用已部署的 SageMaker 端点进行评分，预测结果存储至 S3，可通过 Athena 查询并在 QuickSight 中可视化。CloudWatch 监控整条流水线，当欺诈率超过阈值时触发 SNS 告警。

---

## 数据集

原始数据集（`PS_20174392719_1491204439457_log.csv`，约 470 MB）超出 GitHub 文件大小限制，**未包含在本仓库中**，存储于 S3，运行流水线前须先下载。

**从 S3 下载：**

```bash
aws s3 cp s3://finalproject-fraud-detection/raw/PS_20174392719_1491204439457_log.csv \
    dataset/PS_20174392719_1491204439457_log.csv
```

> 请确保 AWS CLI 已配置（`aws configure`），且凭证具有 `AmazonS3FullAccess` 或至少对 `finalproject-fraud-detection` 存储桶的读取权限。

**原始来源：**[PaySim 合成金融数据集 — Kaggle](https://www.kaggle.com/datasets/ealaxi/paysim1)

---

## 架构

### 离线流水线（模型训练）

```
PS_20174392719_1491204439457_log.csv
    └── S3 (raw/)
        └── SageMaker XGBoost 训练
            └── SageMaker 端点
```

### 在线流水线（实时推理）

```
本地 Ubuntu 机器
    └── src/stream_transactions.py
        └── Kinesis (fraud-stream)
            └── Lambda (fraud_detection_lambda.py)
                └── SageMaker 端点
                    └── S3 (predictions/realtime/)
                        └── Athena
                            └── QuickSight
```

### 监控

```
CloudWatch
    ├── 日志     — Lambda 执行日志、SageMaker 推理延迟
    ├── 指标     — 欺诈率、Kinesis 流吞吐量、端点调用次数
    └── 告警     → SNS 主题 (fraud-alert-topic)
```

---

## 使用的 AWS 服务

| 服务 | 职责 |
|---|---|
| **Amazon Kinesis** | 实时交易数据接入（`fraud-stream`）；容量模式：**按需**；最大容量：**10,240 KiB** |
| **AWS Lambda** | 对每个 Kinesis 分片记录触发批量推理 |
| **Amazon SageMaker** | XGBoost 模型训练（`ml.m5.large`）及托管实时端点 |
| **Amazon S3** | 存储原始数据、处理后数据分片、模型产物及预测结果 |
| **Amazon Athena** | 对 S3 预测结果进行 SQL 查询 |
| **Amazon QuickSight** | 交互式欺诈分析仪表盘 |
| **Amazon CloudWatch** | 流水线日志、指标与告警监控 |
| **Amazon SNS** | 欺诈率超阈值时向 `fraud-alert-topic` 发送告警通知 |

---

## 模型详情

| 项目 | 值 |
|---|---|
| **算法** | SageMaker 内置 XGBoost 1.7-1 |
| **目标函数** | `binary:logistic` |
| **评估指标** | AUC |
| **训练轮数** | 120 |
| **训练实例** | `ml.m5.large` |
| **推理阈值** | 0.5 |
| **测试准确率** | 0.9980 |
| **测试精确率** | 0.9718 |
| **测试召回率** | 0.9789 |
| **测试 F1 分数** | 0.9753 |
| **测试 ROC-AUC** | 0.9992 |

### 特征向量（11 个特征，顺序敏感）

| # | 特征 | 类型 |
|---|---|---|
| 1 | `step` | int |
| 2 | `amount` | float |
| 3 | `oldbalanceOrg` | float |
| 4 | `newbalanceOrig` | float |
| 5 | `oldbalanceDest` | float |
| 6 | `newbalanceDest` | float |
| 7 | `type_CASH_IN` | int（独热编码） |
| 8 | `type_CASH_OUT` | int（独热编码） |
| 9 | `type_DEBIT` | int（独热编码） |
| 10 | `type_PAYMENT` | int（独热编码） |
| 11 | `type_TRANSFER` | int（独热编码） |

---

## IAM 角色与策略要求

运行项目前，请在 AWS 控制台中配置以下 IAM 角色。

### 1. 本地机器 — IAM 用户

进入 **IAM → 用户 → 你的用户 → 添加权限 → 直接附加策略**：

| 托管策略 | 用途 |
|---|---|
| `AmazonS3FullAccess` | 上传原始数据并从 S3 读取模型产物 |
| `AmazonKinesisFullAccess` | 向 `fraud-stream` 写入记录 |
| `AmazonSageMakerFullAccess` | 提交训练任务并调用端点 |

### 2. SageMaker 执行角色

进入 **IAM → 角色 → 创建角色 → AWS 服务 → SageMaker**：

| 托管策略 | 用途 |
|---|---|
| `AmazonSageMakerFullAccess` | 训练、模型及端点管理 |
| `AmazonS3FullAccess` | 读取原始/处理后数据，写入模型产物 |
| `CloudWatchLogsFullAccess` | 将训练日志写入 CloudWatch |

### 3. Lambda 执行角色

进入 **IAM → 角色 → 创建角色 → AWS 服务 → Lambda**：

| 托管策略 | 用途 |
|---|---|
| `AmazonKinesisFullAccess` | 从 `fraud-stream` 读取记录 |
| `AmazonSageMakerFullAccess` | 调用 SageMaker 推理端点 |
| `AmazonS3FullAccess` | 将预测结果写入 `predictions/` |
| `CloudWatchLogsFullAccess` | 写入 Lambda 执行日志 |

### 4. CloudWatch 与 SNS

进入 **IAM → 角色 → 你的 CloudWatch 角色 → 添加权限**：

| 托管策略 | 用途 |
|---|---|
| `CloudWatchFullAccess` | 创建告警、指标和仪表盘 |
| `AmazonSNSFullAccess` | 向 `fraud-alert-topic` 发布欺诈率告警 |

---

## S3 存储桶结构

**存储桶：** `finalproject-fraud-detection`

```
finalproject-fraud-detection/
├── raw/                        # 原始数据集 CSV
├── processed/
│   ├── train/                  # 训练集（70%）
│   ├── validation/             # 验证集（15%）
│   └── test/                   # 测试集（15%）
├── predictions/
│   ├── realtime/               # Lambda 逐笔推理输出（JSON）
│   └── offline-endpoint-check/ # 端点验证结果（CSV）
├── model/
│   ├── training-output/        # SageMaker 训练任务输出
│   └── xgboost/                # 复制的模型产物（model.tar.gz）
└── Athena-results/             # Athena 查询输出文件
```

---

## 仓库结构

```
AWS-Fraud-Detection-Analysis/
├── architecture/                         # AWS 架构图
├── dashboard/                            # QuickSight 导出可视化图表
├── dataset/                              # 离线生成的交易数据集（见数据集章节）
├── lambda/
│   └── fraud_detection_lambda.py         # Lambda 处理器：解码 → 特征构建 → 批量推理 → S3 写入
├── model/                                # 离线训练的模型产物
├── sql/                                  # Athena SQL 查询脚本
├── src/
│   └── stream_transactions.py            # 均衡池构建器与 Kinesis 流模拟器
├── Training Model and Deploy.ipynb       # SageMaker 训练、评估与部署笔记本
├── README.md
└── requirements.txt
```

---

## 快速开始

### 前置条件

- Ubuntu 24.04
- Python 3.10+
- AWS CLI 已配置（`aws configure`），使用 IAM 用户凭证
- 已启用以下 AWS 服务：Kinesis、SageMaker、Lambda、S3、Athena、QuickSight、CloudWatch、SNS

### 安装

```bash
git clone https://github.com/optimus889/AWS-Fraud-Detection-Analysis.git
cd AWS-Fraud-Detection-Analysis
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 第一步 — 从 S3 下载数据集

原始数据集存储于 S3，运行笔记本前须先下载到本地：

```bash
aws s3 cp s3://finalproject-fraud-detection/raw/PS_20174392719_1491204439457_log.csv \
    dataset/PS_20174392719_1491204439457_log.csv
```

### 第二步 — 训练模型（SageMaker 笔记本）

在 SageMaker 笔记本实例中打开并运行 `Training Model and Deploy.ipynb`，该笔记本将：

1. 从 `S3 (raw/)` 下载原始 CSV，并以分块方式读取预处理
2. 对交易类型进行独热编码，对非欺诈记录进行降采样（3% 采样）
3. 分割为训练集/验证集/测试集并上传至 `S3 (processed/)`
4. 在 `ml.m5.large` 上启动 SageMaker XGBoost 训练任务
5. 在测试集上评估模型（ROC-AUC ≈ 0.9992）

### 第三步 — 部署 SageMaker 端点

继续在 `Training Model and Deploy.ipynb` 中部署已训练模型：

```python
predictor = xgb.deploy(
    initial_instance_count=1,
    instance_type="ml.m5.large",
    serializer=CSVSerializer(),
    deserializer=JSONDeserializer()
)
```

记录已部署的端点名称（例如 `sagemaker-xgboost-2026-03-13-23-05-26-528`），供 Lambda 配置使用。

### 第四步 — 配置 Lambda 函数

1. 在 AWS 控制台中，使用 `lambda/fraud_detection_lambda.py` 中的代码创建 Lambda 函数
2. 设置以下环境变量：

| 键 | 值 |
|---|---|
| `ENDPOINT_NAME` | 已部署的 SageMaker 端点名称 |
| `PREDICTION_BUCKET` | 你的存储桶名称 |
| `PREDICTION_PREFIX` | `predictions/realtime` |

3. 配置以下 Lambda 资源设置：

| 设置 | 值 |
|---|---|
| **内存** | 1024 MB |
| **超时** | 5 分钟（300 秒） |

4. 添加指向 `fraud-stream` 的 Kinesis 触发器
5. 附加上述 IAM 章节中定义的 Lambda 执行角色

### 第五步 — 启动实时流

```bash
python3 src/stream_transactions.py
```

该脚本从 S3 读取原始 CSV，构建均衡池（默认 20% 欺诈、80% 正常），并向 `fraud-stream` 推送 1,500 笔交易。Lambda 处理每批数据，调用 SageMaker 端点，并将逐笔交易的 JSON 结果写入 `s3://finalproject-fraud-detection/predictions/realtime/`。

### 第六步 — 使用 Athena 查询结果

对预测存储桶运行 `sql/` 中的 SQL 脚本，结果保存至 `s3://finalproject-fraud-detection/Athena-results/`。

### 第七步 — 使用 QuickSight 可视化

将 QuickSight 连接至 Athena 数据源，并从 `dashboard/` 加载仪表盘配置，交互式探索欺诈指标。

### 第八步 — 使用 CloudWatch 与 SNS 监控

CloudWatch 收集 Lambda 执行日志、Kinesis 流吞吐量及 SageMaker 端点调用指标。配置告警，当欺诈率超过定义阈值时，向 `fraud-alert-topic` 触发 SNS 通知。

---

## 许可证

本项目仅供学术与教育用途。