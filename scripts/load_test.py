"""Measures producer throughput and end-to-end ingestion latency against the running stack."""
import json
import statistics
import time

import requests
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP = "localhost:9092"
ES_URL = "http://localhost:9200"
TOPIC = f"load-test-{int(time.time())}"
N_MESSAGES = 2000


def run() -> None:
    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    # Warm up topic auto-creation so the consumer doesn't stall on metadata discovery.
    producer.send(TOPIC, {"seq": -1, "sent_at": time.time()}).get(timeout=10)
    time.sleep(2)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BOOTSTRAP],
        auto_offset_reset="earliest",
        consumer_timeout_ms=30000,
    )
    consumer.poll(timeout_ms=2000)  # force partition assignment before producing

    send_times = {}
    start = time.time()
    for i in range(N_MESSAGES):
        ts = time.time()
        send_times[i] = ts
        producer.send(TOPIC, {"seq": i, "sent_at": ts})
    producer.flush()
    produce_elapsed = time.time() - start

    latencies = []
    consumed = 0
    for msg in consumer:
        payload = json.loads(msg.value)
        if payload["seq"] == -1:
            continue
        latencies.append(time.time() - payload["sent_at"])
        consumed += 1
        if consumed >= N_MESSAGES:
            break

    throughput = N_MESSAGES / produce_elapsed if produce_elapsed > 0 else float("inf")

    print(f"Messages produced:        {N_MESSAGES}")
    print(f"Produce wall time (s):    {produce_elapsed:.2f}")
    print(f"Producer throughput:      {throughput:.1f} msgs/sec")
    print(f"Messages consumed:        {consumed}")
    if latencies:
        print(f"End-to-end latency p50:  {statistics.median(latencies)*1000:.1f} ms")
        print(f"End-to-end latency p95:  {sorted(latencies)[int(len(latencies)*0.95)-1]*1000:.1f} ms")
        print(f"End-to-end latency max:  {max(latencies)*1000:.1f} ms")

    time.sleep(3)  # allow Logstash to flush to Elasticsearch
    try:
        resp = requests.get(f"{ES_URL}/ecommerce-*/_count", timeout=5)
        print(f"Elasticsearch document count (ecommerce-*): {resp.json().get('count')}")
    except requests.RequestException as exc:
        print(f"Could not query Elasticsearch: {exc}")


if __name__ == "__main__":
    run()
