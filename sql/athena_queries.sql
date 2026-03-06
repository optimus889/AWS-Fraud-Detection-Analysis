-- Fraud vs Normal transactions
SELECT fraud, COUNT(*) AS transaction_count
FROM fraud_demo_db.fraud_transactions
GROUP BY fraud
ORDER BY fraud;

-- Fraud by location
SELECT location, COUNT(*) AS fraud_count
FROM fraud_demo_db.fraud_transactions
WHERE fraud = 1
GROUP BY location
ORDER BY fraud_count DESC;

-- Fraud by merchant
SELECT merchant, COUNT(*) AS fraud_count
FROM fraud_demo_db.fraud_transactions
WHERE fraud = 1
GROUP BY merchant
ORDER BY fraud_count DESC;

-- Fraud by payment method
SELECT payment_method, COUNT(*) AS fraud_count
FROM fraud_demo_db.fraud_transactions
WHERE fraud = 1
GROUP BY payment_method
ORDER BY fraud_count DESC;

-- Fraud by transaction amount range
SELECT
CASE
WHEN amount < 100 THEN 'Low'
WHEN amount BETWEEN 100 AND 500 THEN 'Medium'
ELSE 'High'
END AS amount_range,
COUNT(*) AS fraud_count
FROM fraud_demo_db.fraud_transactions
WHERE fraud = 1
GROUP BY
CASE
WHEN amount < 100 THEN 'Low'
WHEN amount BETWEEN 100 AND 500 THEN 'Medium'
ELSE 'High'
END
ORDER BY fraud_count DESC;
