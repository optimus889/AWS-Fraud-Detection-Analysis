# AWS Cloud Fraud Detection Analytics

This project demonstrates a cloud-based fraud detection analytics pipeline built on AWS.

The system combines machine learning and cloud analytics to identify suspicious financial transactions and visualize fraud patterns.

## Architecture

S3 → SageMaker → Athena → QuickSight

1. Amazon S3 stores the transaction dataset
2. Amazon SageMaker trains the fraud detection model
3. Amazon Athena performs SQL analytics on cloud data
4. Amazon QuickSight visualizes fraud insights through dashboards

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

## Example Architecture

See the architecture diagram in the `architecture` folder.

## Example Dashboard

See dashboard screenshot in the `dashboard` folder.

## Author
Mingyu Fan, Cheng-yang Lee, Wei-Chen Wang

