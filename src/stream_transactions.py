import json
import random
import time
import boto3
from datetime import datetime

# AWS Kinesis client
kinesis = boto3.client("kinesis", region_name="us-east-1")

# merchants
merchants = [
    "Amazon","Walmart","Apple Store","Best Buy",
    "Target","Starbucks","eBay","Uber","Netflix"
]

# locations
locations = [
    "New York","Boston","San Francisco","Chicago",
    "Los Angeles","Seattle","Dallas","Miami"
]

# payment methods
payment_methods = [
    "Credit Card","Debit Card","Online Payment","Mobile Pay"
]

def generate_transaction(tx_id):

    amount = round(random.uniform(5,2000),2)

    fraud = 1 if random.random() < 0.1 else 0

    transaction = {
        "transaction_id": tx_id,
        "timestamp": datetime.utcnow().isoformat(),
        "amount": amount,
        "merchant": random.choice(merchants),
        "location": random.choice(locations),
        "payment_method": random.choice(payment_methods),
        "fraud": fraud
    }

    return transaction


def send_to_kinesis(data):

    kinesis.put_record(
        StreamName="fraud-stream",
        Data=json.dumps(data),
        PartitionKey=str(data["transaction_id"])
    )


def main():

    tx_id = 1

    print("Starting real-time transaction stream...")

    while True:

        transaction = generate_transaction(tx_id)

        print(transaction)

        send_to_kinesis(transaction)

        tx_id += 1

        time.sleep(2)


if __name__ == "__main__":
    main()
