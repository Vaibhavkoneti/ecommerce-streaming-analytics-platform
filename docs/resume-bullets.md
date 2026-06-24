# Resume Material

## Project title
**Real-Time User Activity Analysis Pipeline** — Kafka, ELK Stack, Docker

## Bullet points (pick 2-4 depending on space)

- Designed and containerized a real-time e-commerce clickstream analytics pipeline (Kafka → Logstash → Elasticsearch → Kibana) orchestrated with Docker Compose, achieving sustained 10 events/sec ingestion with 0 errors and ~6,500 msgs/sec burst throughput at ~300ms p50 end-to-end latency.
- Identified and fixed a production data-correctness defect where randomized event timestamps were being mapped into Elasticsearch's `@timestamp` field, causing 480+ redundant daily indices; corrected to real UTC timestamps, reducing index count to 2/day and cutting cluster shard overhead.
- Hardened the deployment for production readiness: added Docker healthchecks and `service_healthy` startup ordering across 5 services, graceful shutdown with in-flight message flushing, retry/backoff on the Kafka producer, and persistent named volumes for stateful services.
- Built a CI pipeline (GitHub Actions) that lints application code, validates Docker Compose configuration, and builds container images on every push/PR.
- Authored a repeatable load-testing tool to benchmark producer throughput and end-to-end ingestion latency against the live stack, used to validate performance before and after pipeline changes.

## Resume image
Use `docs/architecture.svg` (also embedded in the README) as the project visual — it's a clean, dark-themed architecture diagram showing the producer → Kafka → Logstash → Elasticsearch → Kibana flow plus the orchestration/CI layer. Export to PNG if your resume builder doesn't accept SVG:

```bash
# requires Inkscape or rsvg-convert installed
rsvg-convert -w 1200 docs/architecture.svg -o docs/architecture.png
```

Alternatively, take a screenshot of a populated Kibana dashboard (`http://localhost:5601`) once the stack is running — that's a strong "real product" visual to pair with the architecture diagram.
