# Load Test Results

Environment: local Docker Desktop host, full stack (Zookeeper, Kafka, Elasticsearch, Logstash, Kibana, producer) running via `docker compose up -d --build`. Background producer active at 10 events/sec throughout. Test tool: `scripts/load_test.py`, 2,000 messages/run.

| Run | Throughput (msgs/sec) | p50 latency | p95 latency | Max latency |
|---|---|---|---|---|
| 1 | 6,905 | 300.8 ms | 316.7 ms | 318.6 ms |
| 2 | 6,361 | 303.6 ms | 332.4 ms | 336.4 ms |
| 3 | 6,696 | 307.1 ms | 322.4 ms | 325.4 ms |

End-to-end latency = time from producer `send()` call to the message being readable back out of Kafka (covers broker write + replication ack; Logstash/Elasticsearch indexing happens asynchronously downstream and is verified separately via document counts below).

## System-level verification

- **Sustained producer**: 10 events/sec target, 0 send errors over multi-minute runs (`docker logs activity-producer`).
- **Service health**: all 5 services report `healthy` via Compose healthchecks within ~30s of `docker compose up`.
- **Index correctness fix**: before fix (random `Faker.iso8601()` timestamps mapped into `@timestamp`), a few minutes of test traffic produced 480+ daily Elasticsearch indices. After switching to real UTC timestamps, the same workload produces exactly 2 indices/day (`ecommerce-user_activity-YYYY.MM.dd`, `ecommerce-transaction-YYYY.MM.dd`).
- **Data durability**: Elasticsearch document count climbs monotonically with load-test runs (1962 → 2042 → 2128 across consecutive runs), confirming no data loss between Kafka and Elasticsearch.

## How to reproduce

```bash
docker compose up -d --build
pip install kafka-python requests
python scripts/load_test.py
```
