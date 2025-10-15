import os
import json
import time
import threading
from google.cloud import pubsub_v1
from datetime import datetime, timezone

WINDOW_DURATION = 60

message_buffer = []
lock = threading.Lock()

def callback(message):
    try:
        data = json.loads(message.data.decode("utf-8"))
        with lock:
            message_buffer.append(data)
        message.ack()
    except Exception as e:
        print(f"Error decoding message: {e}")
        message.nack()


def aggregate_job():
    while True:
        time.sleep(WINDOW_DURATION)
        with lock:
            count = len(message_buffer)
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{timestamp}] Total transaksi dalam 1 menit terakhir: {count}")
            message_buffer.clear()


def main():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/config/sub-credential.json"
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path("gcpl-469311", "datalabs-streamtest-sub")

    threading.Thread(target=aggregate_job, daemon=True).start()

    print(f"Listening on {subscription_path} ...")
    streaming_pull = subscriber.subscribe(subscription_path, callback=callback)

    try:
        streaming_pull.result()
    except KeyboardInterrupt:
        streaming_pull.cancel()
        print("Stopped.")