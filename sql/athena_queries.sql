-- Create database
CREATE DATABASE IF NOT EXISTS fraud_analytics;

--Create Table
CREATE EXTERNAL TABLE IF NOT EXISTS predictions (
  transaction_id string,
  timestamp string,
  amount double,
  merchant string,
  location string,
  payment_method string,
  actual_fraud int,
  prediction_score double,
  predicted_fraud int
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 's3://finalproject-fraud-detection/predictions/';

-- Test 10 queries
SELECT *
FROM predictions
LIMIT 10;

-- Overall Fraud Detection Summary
SELECT
  COUNT(*) AS total_predictions,
  SUM(predicted_fraud) AS total_fraud_flags,
  AVG(prediction_score) AS avg_prediction_score
FROM fraud_analytics.predictions;

-- Fraud Detection by Merchant
SELECT
  merchant,
  COUNT(*) AS total_transactions,
  SUM(predicted_fraud) AS fraud_transactions,
  AVG(prediction_score) AS avg_score
FROM fraud_analytics.predictions
GROUP BY merchant
ORDER BY fraud_transactions DESC, avg_score DESC;

-- Fraud Detection by Location
SELECT
  location,
  COUNT(*) AS total_transactions,
  SUM(predicted_fraud) AS fraud_transactions,
  AVG(prediction_score) AS avg_score
FROM fraud_analytics.predictions
GROUP BY location
ORDER BY fraud_transactions DESC, avg_score DESC;

-- Fraud Detection by Payment Method
SELECT
  payment_method,
  COUNT(*) AS total_transactions,
  SUM(predicted_fraud) AS fraud_transactions,
  AVG(prediction_score) AS avg_score
FROM fraud_analytics.predictions
GROUP BY payment_method
ORDER BY fraud_transactions DESC, avg_score DESC;