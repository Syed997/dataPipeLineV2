
# Kafka ↔ Flask Producer ↔ Spark Structured Streaming (Docker)

This project spins up:
- **Kafka (KRaft mode)** with auto-topic-creation enabled
- **Flask producer** that POST-starts background senders (3-6 msgs/sec per topic)
- **Spark Structured Streaming** consumer that:
  - subscribes via `subscribePattern` so **new topics are consumed automatically**
  - maintains a rolling per-topic count for the **last 5 seconds**
  - exposes a **GET API**: `/count?topic=demo-alpha`

## Prereqs
- Docker + Docker Compose
- Ubuntu works great

## Quick start

```bash
docker compose up -d --build
```

Start producing (defaults to topics `demo-alpha`, `demo-beta`, `demo-gamma`):
```bash
curl -X POST http://localhost:5000/start
```

Optionally, start with your own topics (must start with `demo-` to match the regex in Spark):
```bash
curl -X POST http://localhost:5000/start   -H "Content-Type: application/json"   -d '{"topics": ["demo-foo","demo-bar","demo-baz"]}'
```

Check producer status:
```bash
curl http://localhost:5000/status
```

Query rolling counts (last 5 seconds) per topic:
```bash
curl "http://localhost:5001/count?topic=demo-alpha"
```

Stop producers:
```bash
curl -X POST http://localhost:5000/stop
```

## Notes
- Kafka is configured with `KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true` so topics are created on demand.
- Spark uses `subscribePattern=demo-.*` to automatically consume new topics with that prefix.
- Adjust the rolling window in `spark/app.py` via `WINDOW_SECONDS` if needed.
- Spark UI available at http://localhost:4040 while the job is running.



print(f"   - Health: http://localhost:{API_PORT}/health")
print(f"   - Count: http://localhost:{API_PORT}/count?topic=TOPIC_NAME")
print(f"   - Topics: http://localhost:{API_PORT}/topics")
print(f"   - Batches: http://localhost:{API_PORT}/batches/recent")



curl -X POST http://localhost:5000/start \
  -H "Content-Type: application/json" \
  -d '{
    "topics": [
      "demo-app1-traces",
      "demo-app2-metrics",
      "demo-app1-logs",
      "demo-app1-metrics",
      "demo-app2-traces",
      "demo-app2-logs"
    ]
  }'


  curl -X POST http://localhost:5000/start \
  -H "Content-Type: application/json" \
  -d '{
    "topics": [
      "app-flsk_traces",
      "app-flsk_metrics",
      "app-flsk_logs"
    ]
  }'