import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("activity-producer")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
USER_ACTIVITY_TOPIC = os.environ.get("USER_ACTIVITY_TOPIC", "user-activity")
TRANSACTION_TOPIC = os.environ.get("TRANSACTION_TOPIC", "transactions")
EVENTS_PER_SECOND = float(os.environ.get("EVENTS_PER_SECOND", "5"))
TRANSACTION_RATE = float(os.environ.get("TRANSACTION_RATE", "0.2"))

fake = Faker()
_running = True
_stats = {"activity_sent": 0, "transactions_sent": 0, "errors": 0}


def _shutdown(signum, frame):
    global _running
    logger.info("Shutdown signal received (%s); draining producer...", signum)
    _running = False


signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=50,
        max_in_flight_requests_per_connection=1,
    )


def generate_user_activity() -> dict:
    return {
        "user_id": fake.uuid4(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "action": random.choice(["view", "click", "add_to_cart", "remove_from_cart"]),
        "product_id": fake.uuid4(),
        "category": fake.word(),
        "ip_address": fake.ipv4(),
    }


def generate_transaction() -> dict:
    return {
        "transaction_id": fake.uuid4(),
        "user_id": fake.uuid4(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "products": [
            {
                "product_id": fake.uuid4(),
                "quantity": random.randint(1, 5),
                "price": round(random.uniform(10, 1000), 2),
            }
            for _ in range(random.randint(1, 5))
        ],
        "total_amount": round(random.uniform(10, 5000), 2),
        "payment_method": random.choice(["credit_card", "debit_card", "paypal"]),
    }


def send(producer: KafkaProducer, topic: str, payload: dict, counter_key: str) -> None:
    try:
        producer.send(topic, payload).add_errback(
            lambda exc: logger.error("Delivery failed for %s: %s", topic, exc)
        )
        _stats[counter_key] += 1
    except KafkaError as exc:
        _stats["errors"] += 1
        logger.error("Failed to enqueue message on %s: %s", topic, exc)


def main() -> int:
    logger.info(
        "Starting producer: brokers=%s rate=%s/s tx_rate=%s",
        KAFKA_BOOTSTRAP_SERVERS, EVENTS_PER_SECOND, TRANSACTION_RATE,
    )
    producer = build_producer()
    interval = 1.0 / EVENTS_PER_SECOND if EVENTS_PER_SECOND > 0 else 0.2
    last_report = time.time()

    while _running:
        send(producer, USER_ACTIVITY_TOPIC, generate_user_activity(), "activity_sent")

        if random.random() < TRANSACTION_RATE:
            send(producer, TRANSACTION_TOPIC, generate_transaction(), "transactions_sent")

        if time.time() - last_report >= 10:
            logger.info("Stats: %s", _stats)
            last_report = time.time()

        time.sleep(interval)

    producer.flush(timeout=10)
    producer.close()
    logger.info("Producer stopped cleanly. Final stats: %s", _stats)
    return 0


if __name__ == "__main__":
    sys.exit(main())
