import pandas as pd
import random
from datetime import datetime, timedelta

# merchants
merchants = [
    "Amazon", "Walmart", "Apple Store", "Best Buy",
    "Target", "Starbucks", "eBay", "Uber", "Netflix"
]

# locations
locations = [
    "New York", "Boston", "San Francisco", "Chicago",
    "Los Angeles", "Seattle", "Dallas", "Miami"
]

# payment methods
payment_methods = [
    "Credit Card", "Debit Card", "Online Payment", "Mobile Pay"
]

start_date = datetime(2024,1,1)
end_date = datetime(2026,3,30)

data = []

for i in range(1,1001):

    random_days = random.randint(0,(end_date-start_date).days)
    random_time = start_date + timedelta(days=random_days)

    amount = round(random.uniform(5,2000),2)

    fraud = 1 if random.random() < 0.1 else 0  # 10% fraud

    data.append({
        "transaction_id": i,
        "timestamp": random_time,
        "amount": amount,
        "merchant": random.choice(merchants),
        "location": random.choice(locations),
        "payment_method": random.choice(payment_methods),
        "fraud": fraud
    })

df = pd.DataFrame(data)

df.to_csv("fraud_transactions.csv", index=False)

print("Dataset generated: fraud_transactions.csv")

