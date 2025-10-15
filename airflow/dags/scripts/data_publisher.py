import os
import json, time, random
from datetime import datetime, timezone
from google.cloud import pubsub_v1

def publish():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/config/pub-credential.json"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path("gcpl-469311", "datalabs-streamtest")
    
    transaction_ids = 1
    while True:
        transaction = {
            "transaction_id": transaction_ids,
            "customer_id": random.randint(1, 100),
            "product_id": random.randint(1, 50),
            "quantity": random.randint(1, 5),
            "price": round(random.uniform(5.0, 200.0), 2),
            "timestamp" : datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        }

        data = json.dumps(transaction).encode("utf-8")
        publisher.publish(topic_path, data)
        print(f"Published: {transaction}")

        transaction_ids += 1
        time.sleep(random.uniform(0.5, 2.0))