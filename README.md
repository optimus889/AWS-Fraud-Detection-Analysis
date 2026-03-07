# AWS Cloud Fraud Detection Analytics

This project demonstrates a cloud-based fraud detection analytics pipeline built on AWS.

The system combines machine learning and cloud analytics to identify suspicious financial transactions and visualize fraud patterns.

## Architecture

This project includes both batch analytics and real-time fraud detection pipelines on AWS.

### Batch Analytics Pipeline
Amazon S3 → AWS Glue → Amazon Athena → Amazon QuickSight

### Real-Time Detection Pipeline
API Gateway → Amazon Kinesis → AWS Lambda → Amazon SageMaker Endpoint

### Monitoring
Amazon CloudWatch monitors logs, API requests, Lambda execution, and model performance.

## AWS Services Used

- Amazon S3 – Data lake storage
- Amazon SageMaker – Machine learning model training
- Amazon Athena – Serverless SQL analytics
- Amazon QuickSight – Business intelligence dashboard

## Dataset

The dataset contains transaction records with the following fields:

- transaction_id
- timestamp
- amount
- merchant
- location
- payment_method
- fraud label

Fraud label values:
- 0 = Normal transaction
- 1 = Fraudulent transaction

## Machine Learning Model

Model used:

Random Forest Classifier

Features used:

- amount
- merchant
- location
- payment_method
- hour
- day_of_week
- is_weekend
- is_night

The model is trained in Amazon SageMaker and used to predict whether a transaction is suspicious.

## Athena Analytics

Example queries include:

- Fraud vs Normal distribution
- Fraud by location
- Fraud by merchant
- Fraud by payment method
- Fraud by transaction amount range

Athena allows SQL queries directly on data stored in Amazon S3.

## QuickSight Dashboard

The QuickSight dashboard provides interactive visualization including:

- Fraud vs Normal transactions
- Fraud by Location
- Fraud by Merchant
- Fraud by Payment Method

These visualizations help identify fraud patterns quickly.

## Project Workflow

1. Upload dataset to Amazon S3
2. Train fraud detection model in SageMaker
3. Analyze fraud patterns using Athena SQL
4. Build fraud analytics dashboard using QuickSight

## Architecture Demo

## AWS Architecture

The following diagram illustrates the cloud architecture used for the fraud detection analytics system built on AWS.

![AWS Architecture](architecture/architecture.png)

## Dashboard Demo

The QuickSight dashboard visualizes fraud analytics including:

- Fraud vs Normal transactions
- Fraud by Location
- Fraud by Merchant
- Fraud by Payment Method

![QuickSight Dashboard](dashboard/quicksight_dashboard.png)

Full dashboard report:

[Download PDF Version](dashboard/Analysis.pdf)

## Author
Mingyu Fan, Cheng-yang Lee, Wei-Chen Wang







