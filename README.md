# AWS Fraud Detection Analysis

A real-time fraud detection pipeline built on AWS, combining offline model training with live transaction stream inference, automated alerting, and interactive visualization.

---

## Author

**Mingyu Fan, Cheng-yang Lee, Wei-chen Wang** — Graduate Student, Informatics, Northeastern University  
Project Repository: [AWS-Fraud-Detection-Analysis](https://github.com/optimus889/AWS-Fraud-Detection-Analysis)

---

## Project Overview

This project implements an end-to-end fraud detection system using AWS managed services. Transactions are streamed in real time through Kinesis, scored by a SageMaker-hosted XGBoost model via Lambda, and results are stored in S3 for analysis through Athena and QuickSight. CloudWatch monitors the pipeline and sends SNS alerts when fraud rates exceed defined thresholds.

---

## Architecture

### Offline Pipeline (Model Training)

```
Ubuntu Local Machine
    └── offline_transaction.py          # Generate synthetic transaction dataset
        └── .csv → S3 (raw/)            # Upload raw data to S3
            └── SageMaker XGBoost       # Train fraud detection model
                └── SageMaker Endpoint  # Deploy model for real-time inference
```

### Online Pipeline (Real-Time Inference)

```
Ubuntu Local Machine
    └── stream_transactions.py          # Simulate live transaction stream
        └── Kinesis (fraud-stream)      # Ingest streaming data
            └── Lambda (inference)      # Trigger inference on each record
                └── SageMaker Endpoint  # InvokeEndpoint for fraud scoring
                    └── S3 (predictions/) # Store prediction results
                        └── Athena      # Query predictions with SQL
                            └── QuickSight # Visualize fraud analytics dashboard
```

### Monitoring

```
CloudWatch
    ├── Logs     — Lambda execution logs, inference latency
    ├── Metrics  — Fraud rate, stream throughput, endpoint invocations
    └── Alarms   → SNS Alerts (triggered when fraud rate exceeds threshold)
```

---

## AWS Services Used

| Service | Role |
|---|---|
| **Amazon Kinesis** | Real-time transaction data ingestion (`fraud-stream`) |
| **AWS Lambda** | Serverless inference trigger on each Kinesis record |
| **Amazon SageMaker** | XGBoost model training and hosted endpoint |
| **Amazon S3** | Storage for raw data, predictions, processed files, models, and logs |
| **Amazon Athena** | SQL-based querying over S3 prediction results |
| **Amazon QuickSight** | Interactive fraud analytics dashboard |
| **Amazon CloudWatch** | Logs, metrics, and alarms for pipeline monitoring |
| **Amazon SNS** | Alert notifications when fraud rate threshold is exceeded |

---

## IAM Role & Policy Requirements

This project requires the following IAM roles and permission policies to be configured before running.

### 1. Local Ubuntu Machine — IAM User / Role

The machine running `offline_transaction.py` and `stream_transactions.py` must be configured via AWS CLI (`aws configure`) with credentials that have the following permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::finalproject-fraud-detection",
        "arn:aws:s3:::finalproject-fraud-detection/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "kinesis:PutRecord",
        "kinesis:PutRecords",
        "kinesis:DescribeStream"
      ],
      "Resource": "arn:aws:kinesis:*:*:stream/fraud-stream"
    },
    {
      "Effect": "Allow",
      "Action": [
        "sagemaker:CreateTrainingJob",
        "sagemaker:DescribeTrainingJob",
        "sagemaker:CreateModel",
        "sagemaker:CreateEndpointConfig",
        "sagemaker:CreateEndpoint",
        "sagemaker:DescribeEndpoint",
        "sagemaker:InvokeEndpoint"
      ],
      "Resource": "*"
    }
  ]
}
```

### 2. SageMaker Execution Role

The SageMaker training job requires an execution role with the following managed policies attached:

| Managed Policy | Purpose |
|---|---|
| `AmazonSageMakerFullAccess` | Training, model, and endpoint management |
| `AmazonS3FullAccess` | Read raw data from S3, write model artifacts |
| `CloudWatchLogsFullAccess` | Write training logs to CloudWatch |

### 3. Lambda Execution Role

The Lambda inference function requires an execution role with the following permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kinesis:GetRecords",
        "kinesis:GetShardIterator",
        "kinesis:DescribeStream",
        "kinesis:ListStreams"
      ],
      "Resource": "arn:aws:kinesis:*:*:stream/fraud-stream"
    },
    {
      "Effect": "Allow",
      "Action": [
        "sagemaker:InvokeEndpoint"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::finalproject-fraud-detection/predictions/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    }
  ]
}
```

### 4. CloudWatch & SNS

CloudWatch Alarms require permission to publish to SNS. Attach the following to the CloudWatch alarm action role or use the AWS-managed `CloudWatchFullAccess` policy:

```json
{
  "Effect": "Allow",
  "Action": [
    "sns:Publish"
  ],
  "Resource": "arn:aws:sns:*:*:fraud-alert-topic"
}
```

---

## S3 Bucket Structure

**Bucket:** `finalproject-fraud-detection`

```
finalproject-fraud-detection/
├── raw/                  # Raw generated transaction CSV files
├── processed/            # Cleaned / feature-engineered data
├── predictions/          # Lambda inference output (fraud scores)
├── model/                # Trained SageMaker XGBoost model artifacts
└── Athena-results/       # Athena query output files
```

---

## Repository Structure

```
AWS-Fraud-Detection-Analysis/
├── architecture/         # AWS architecture diagrams
├── dashboard/            # QuickSight exported visualizations
├── dataset/              # Offline-generated transaction datasets
├── lambda/               # Lambda function source code
├── model/                # Offline-trained model artifacts
├── sql/                  # Athena SQL query scripts
├── src/
│   ├── offline_transaction.py    # Synthetic dataset generation & S3 upload
│   └── stream_transactions.py   # Real-time Kinesis stream simulator
├── README.md
└── requirements.txt
```

---

## Getting Started

### Prerequisites

- Ubuntu 24.04
- Python 3.10+
- AWS CLI configured with appropriate IAM permissions
- AWS services enabled: Kinesis, SageMaker, Lambda, S3, Athena, QuickSight, CloudWatch, SNS

### Installation

```bash
git clone https://github.com/optimus889/AWS-Fraud-Detection-Analysis.git
cd AWS-Fraud-Detection-Analysis
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Step 1 — Generate Dataset & Train Model (Offline)

```bash
python3 src/offline_transaction.py
```

This script generates a synthetic transaction dataset, uploads it to `s3://finalproject-fraud-detection/raw/`, and triggers SageMaker XGBoost training. The trained model artifact is saved to `s3://finalproject-fraud-detection/model/`.

### Step 2 — Deploy SageMaker Endpoint

Deploy the trained model as a real-time SageMaker inference endpoint via the AWS Console or SageMaker SDK.

### Step 3 — Start Real-Time Stream

```bash
python3 src/stream_transactions.py
```

This script streams simulated transactions to the Kinesis `fraud-stream`. Lambda automatically invokes the SageMaker endpoint for each record and writes predictions to `s3://finalproject-fraud-detection/predictions/`.

### Step 4 — Query Results with Athena

Run the SQL scripts in `sql/` against the predictions bucket to analyze fraud patterns. Athena query results are saved to `s3://finalproject-fraud-detection/Athena-results/`.

### Step 5 — Visualize with QuickSight

Connect QuickSight to the Athena data source and load the dashboard configurations from `dashboard/` to explore fraud metrics interactively.

### Step 6 — Monitor with CloudWatch & SNS

CloudWatch monitors Lambda invocations, stream throughput, and fraud rate metrics. An alarm triggers an SNS notification when the fraud rate exceeds the configured threshold.

---

## License

This project is for academic and educational purposes.