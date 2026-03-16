-- Create database
CREATE DATABASE IF NOT EXISTS fraud_analytics;

--Create Table
CREATE EXTERNAL TABLE IF NOT EXISTS fraud_analytics.realtime_predictions (
    transaction_id      string,
    `timestamp`         string,
    location            string,
    feature_version     string,
    `type`              string,
    step                int,
    amount              double,
    oldbalanceorg       double,
    newbalanceorig      double,
    oldbalancedest      double,
    newbalancedest      double,
    actual_isfraud      int,
    predicted_score     double,
    predicted_label     int,
    threshold           double,
    endpoint_name       string,
    endpoint_payload    string,
    processed_at        string
)
PARTITIONED BY (
    year  string,
    month string,
    day   string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://finalproject-fraud-detection/predictions/realtime/';

-- Refresh table partitions after new S3 data arrives
MSCK REPAIR TABLE fraud_analytics.realtime_predictions;

-- Preview realtime prediction records
SELECT *
FROM fraud_analytics.realtime_predictions
LIMIT 20;

-- Count the total number of records in the realtime_predictions table
SELECT COUNT(*) AS total_records
FROM fraud_analytics.realtime_predictions;

-- Count the number of transactions for each actual fraud label
-- actual_isfraud = 0 means legitimate transaction
-- actual_isfraud = 1 means fraudulent transaction
SELECT
    actual_isfraud,
    COUNT(*) AS cnt
FROM fraud_analytics.realtime_predictions
GROUP BY actual_isfraud
ORDER BY actual_isfraud;

-- Count the number of transactions for each predicted fraud label
-- predicted_label = 0 means predicted as legitimate
-- predicted_label = 1 means predicted as fraud
SELECT
    predicted_label,
    COUNT(*) AS cnt
FROM fraud_analytics.realtime_predictions
GROUP BY predicted_label
ORDER BY predicted_label;

-- Confusion matrix summary for model evaluation
SELECT
    SUM(CASE WHEN actual_isfraud = 1 AND predicted_label = 1 THEN 1 ELSE 0 END) AS true_positive,
    SUM(CASE WHEN actual_isfraud = 0 AND predicted_label = 0 THEN 1 ELSE 0 END) AS true_negative,
    SUM(CASE WHEN actual_isfraud = 0 AND predicted_label = 1 THEN 1 ELSE 0 END) AS false_positive,
    SUM(CASE WHEN actual_isfraud = 1 AND predicted_label = 0 THEN 1 ELSE 0 END) AS false_negative
FROM fraud_analytics.realtime_predictions;

-- Classification performance metrics for fraud prediction model
WITH metrics AS (
    SELECT
        SUM(CASE WHEN actual_isfraud = 1 AND predicted_label = 1 THEN 1 ELSE 0 END) AS tp,
        SUM(CASE WHEN actual_isfraud = 0 AND predicted_label = 0 THEN 1 ELSE 0 END) AS tn,
        SUM(CASE WHEN actual_isfraud = 0 AND predicted_label = 1 THEN 1 ELSE 0 END) AS fp,
        SUM(CASE WHEN actual_isfraud = 1 AND predicted_label = 0 THEN 1 ELSE 0 END) AS fn
    FROM fraud_analytics.realtime_predictions
)
SELECT
    tp,
    tn,
    fp,
    fn,
    ROUND(1.0 * (tp + tn) / (tp + tn + fp + fn), 4) AS accuracy,
    ROUND(1.0 * tp / NULLIF(tp + fp, 0), 4) AS precision,
    ROUND(1.0 * tp / NULLIF(tp + fn, 0), 4) AS recall,
    ROUND(2.0 * tp / NULLIF(2 * tp + fp + fn, 0), 4) AS f1_score
FROM metrics;

-- Overall KPI summary for fraud monitoring dashboard
SELECT
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN predicted_label = 1 THEN 1 ELSE 0 END) AS predicted_fraud_count,
    SUM(CASE WHEN actual_isfraud = 1 THEN 1 ELSE 0 END) AS actual_fraud_count,
    ROUND(100.0 * SUM(CASE WHEN predicted_label = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS predicted_fraud_rate_pct,
    ROUND(100.0 * SUM(CASE WHEN actual_isfraud = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS actual_fraud_rate_pct,
    ROUND(AVG(predicted_score), 4) AS avg_predicted_score,
    ROUND(AVG(amount), 2) AS avg_transaction_amount,
    ROUND(SUM(amount), 2) AS total_transaction_amount
FROM fraud_analytics.realtime_predictions;

-- Daily fraud trend for QuickSight time-series visualization
SELECT
    DATE(from_iso8601_timestamp(timestamp)) AS event_date,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN predicted_label = 1 THEN 1 ELSE 0 END) AS predicted_fraud_count,
    SUM(CASE WHEN actual_isfraud = 1 THEN 1 ELSE 0 END) AS actual_fraud_count,
    ROUND(100.0 * SUM(CASE WHEN predicted_label = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS predicted_fraud_rate_pct,
    ROUND(AVG(predicted_score), 4) AS avg_predicted_score,
    ROUND(AVG(amount), 2) AS avg_transaction_amount
FROM fraud_analytics.realtime_predictions
GROUP BY DATE(from_iso8601_timestamp(timestamp))
ORDER BY event_date;

-- Hourly fraud pattern based on event timestamp
SELECT
    HOUR(from_iso8601_timestamp(timestamp)) AS event_hour,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN predicted_label = 1 THEN 1 ELSE 0 END) AS predicted_fraud_count,
    SUM(CASE WHEN actual_isfraud = 1 THEN 1 ELSE 0 END) AS actual_fraud_count,
    ROUND(100.0 * SUM(CASE WHEN predicted_label = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS predicted_fraud_rate_pct
FROM fraud_analytics.realtime_predictions
GROUP BY HOUR(from_iso8601_timestamp(timestamp))
ORDER BY event_hour;

-- Fraud analysis by transaction type
SELECT
    type,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN predicted_label = 1 THEN 1 ELSE 0 END) AS predicted_fraud_count,
    SUM(CASE WHEN actual_isfraud = 1 THEN 1 ELSE 0 END) AS actual_fraud_count,
    ROUND(100.0 * SUM(CASE WHEN predicted_label = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS predicted_fraud_rate_pct,
    ROUND(100.0 * SUM(CASE WHEN actual_isfraud = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS actual_fraud_rate_pct,
    ROUND(AVG(predicted_score), 4) AS avg_predicted_score,
    ROUND(AVG(amount), 2) AS avg_transaction_amount
FROM fraud_analytics.realtime_predictions
GROUP BY type
ORDER BY predicted_fraud_count DESC;

-- Fraud distribution by location
SELECT
    location,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN predicted_label = 1 THEN 1 ELSE 0 END) AS predicted_fraud_count,
    SUM(CASE WHEN actual_isfraud = 1 THEN 1 ELSE 0 END) AS actual_fraud_count,
    ROUND(100.0 * SUM(CASE WHEN predicted_label = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS predicted_fraud_rate_pct,
    ROUND(AVG(predicted_score), 4) AS avg_predicted_score,
    ROUND(SUM(amount), 2) AS total_amount
FROM fraud_analytics.realtime_predictions
GROUP BY location
ORDER BY predicted_fraud_count DESC;

-- Fraud analysis by transaction amount band
SELECT
    CASE
        WHEN amount < 100 THEN 'Below 100'
        WHEN amount >= 100 AND amount < 1000 THEN '100 - 999.99'
        WHEN amount >= 1000 AND amount < 5000 THEN '1000 - 4999.99'
        WHEN amount >= 5000 AND amount < 10000 THEN '5000 - 9999.99'
        ELSE '10000 and above'
    END AS amount_band,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN predicted_label = 1 THEN 1 ELSE 0 END) AS predicted_fraud_count,
    SUM(CASE WHEN actual_isfraud = 1 THEN 1 ELSE 0 END) AS actual_fraud_count,
    ROUND(100.0 * SUM(CASE WHEN predicted_label = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS predicted_fraud_rate_pct,
    ROUND(AVG(predicted_score), 4) AS avg_predicted_score
FROM fraud_analytics.realtime_predictions
GROUP BY
    CASE
        WHEN amount < 100 THEN 'Below 100'
        WHEN amount >= 100 AND amount < 1000 THEN '100 - 999.99'
        WHEN amount >= 1000 AND amount < 5000 THEN '1000 - 4999.99'
        WHEN amount >= 5000 AND amount < 10000 THEN '5000 - 9999.99'
        ELSE '10000 and above'
    END
ORDER BY amount_band;

-- Prediction score distribution for model risk analysis
SELECT
    CASE
        WHEN predicted_score < 0.2 THEN '0.0 - 0.19'
        WHEN predicted_score < 0.4 THEN '0.2 - 0.39'
        WHEN predicted_score < 0.6 THEN '0.4 - 0.59'
        WHEN predicted_score < 0.8 THEN '0.6 - 0.79'
        ELSE '0.8 - 1.0'
    END AS score_band,
    COUNT(*) AS transaction_count,
    SUM(CASE WHEN predicted_label = 1 THEN 1 ELSE 0 END) AS predicted_fraud_count
FROM fraud_analytics.realtime_predictions
GROUP BY
    CASE
        WHEN predicted_score < 0.2 THEN '0.0 - 0.19'
        WHEN predicted_score < 0.4 THEN '0.2 - 0.39'
        WHEN predicted_score < 0.6 THEN '0.4 - 0.59'
        WHEN predicted_score < 0.8 THEN '0.6 - 0.79'
        ELSE '0.8 - 1.0'
    END
ORDER BY score_band;

-- Recent high-risk transactions for detailed dashboard table
SELECT
    transaction_id,
    processed_at,
    timestamp,
    location,
    type,
    step,
    amount,
    actual_isfraud,
    predicted_score,
    predicted_label,
    threshold,
    endpoint_name
FROM fraud_analytics.realtime_predictions
WHERE predicted_label = 1
ORDER BY from_iso8601_timestamp(timestamp) DESC
LIMIT 100;

