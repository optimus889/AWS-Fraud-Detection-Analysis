# AWS Cloud Fraud Detection Analytics

This project demonstrates a cloud-based fraud detection analytics pipeline built on AWS.

The system combines machine learning and cloud analytics to identify suspicious financial transactions and visualize fraud patterns.

## Architecture

This project includes both batch analytics and real-time fraud detection pipelines on AWS.

### Batch Analytics Pipeline
Amazon S3 → AWS Glue → Amazon Athena → Amazon QuickSight

### Real-Time Detection Pipeline
Client/Application → Amazon API Gateway → Amazon Kinesis → AWS Lambda → Amazon SageMaker Endpoint → Prediction Results → Amazon S3

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

### Features used:

- amount
- merchant
- location
- payment_method
- hour
- day_of_week
- is_weekend
- is_night

### Model workflow

1. Transaction data is stored in Amazon S3
2. Data is prepared using AWS Glue
3. The dataset is used to train a model in Amazon SageMaker
4. The trained model is deployed as a SageMaker endpoint
5. Lambda invokes the endpoint for real-time fraud prediction

## Athena Analytics

Example queries include:

- Fraud vs Normal distribution
- Fraud by location
- Fraud by merchant
- Fraud by payment method
- Fraud by transaction amount range
- Fraud activity by time of day

Athena allows SQL queries directly on data stored in Amazon S3.

## QuickSight Dashboard

The QuickSight dashboard provides interactive visualization including:

- Fraud vs Normal transactions
- Fraud by Location
- Fraud by Merchant
- Fraud by Payment Method
- Fraud by Transaction Amount
- Fraud Trend Over Time

These visualizations help identify fraud patterns quickly.

## Data Processing Pipeline

AWS Glue performs ETL operations on the transaction dataset stored in Amazon S3.

Functions include:

- data cleaning
- schema definition
- metadata cataloging
- preparing structured datasets for Athena queries

The Glue Data Catalog enables Athena to efficiently query structured datasets.

## Real-Time Fraud Detection

The system supports near real-time fraud prediction using serverless architecture.

### Real-Time Flow
1. A transaction request is submitted through API Gateway
2. The event is streamed into Amazon Kinesis
3. AWS Lambda processes the event
4. Lambda invokes the SageMaker model endpoint
5. The model returns a fraud prediction
6. Prediction results are stored in Amazon S3
This architecture allows scalable real-time fraud detection.

## Monitoring and Observability

Amazon CloudWatch is used to monitor system performance and operational health.

CloudWatch tracks:
- API Gateway request metrics
- Lambda execution logs
- Kinesis streaming metrics
- SageMaker inference latency

CloudWatch enables centralized monitoring for the entire fraud detection pipeline.

## Project Workflow

1. Upload transaction dataset to Amazon S3
2. Clean and catalog data using AWS Glue
3. Train fraud detection model using Amazon SageMaker
4. Deploy model as SageMaker endpoint
5. Process streaming transactions via API Gateway and Kinesis
6. Detect fraud using Lambda + SageMaker inference
7. Store results in Amazon S3
8. Analyze data using Amazon Athena
9. Visualize insights using Amazon QuickSight
10. Monitor the system using Amazon CloudWatch

## Architecture Demo

## Architecture Diagram

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











