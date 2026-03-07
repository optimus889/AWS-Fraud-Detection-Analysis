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

## Real-Time Fraud Detection

The project supports real-time fraud detection using Amazon API Gateway, Amazon Kinesis, AWS Lambda, and a deployed Amazon SageMaker endpoint.

1. A transaction request is submitted through API Gateway
2. The event is streamed through Amazon Kinesis
3. AWS Lambda processes the transaction data
4. Lambda invokes the SageMaker endpoint for fraud prediction
5. Prediction results are stored for further analytics and visualization

This design enables near real-time fraud scoring for suspicious transactions.

## Data Processing Pipeline

AWS Glue is used to clean, transform, and catalog transaction data stored in Amazon S3.
The Glue Data Catalog makes the dataset queryable in Amazon Athena, enabling efficient SQL-based fraud analysis.

## Monitoring and Observability

Amazon CloudWatch is used to collect logs and monitor service performance across API Gateway, Lambda, Kinesis, and SageMaker.
It helps track inference latency, execution errors, and overall pipeline health.

## AWS Services Used

- Amazon S3 – Data lake storage for transaction records and prediction outputs
- AWS Glue – ETL processing and data cataloging
- Amazon SageMaker – Machine learning model training and real-time inference
- Amazon Athena – Serverless SQL analytics on cloud data
- Amazon QuickSight – Interactive fraud analytics dashboard
- AWS Lambda – Serverless real-time fraud event processing
- Amazon API Gateway – REST API for transaction submission
- Amazon Kinesis – Streaming ingestion of transaction events
- Amazon CloudWatch – Monitoring, logging, and performance tracking

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









