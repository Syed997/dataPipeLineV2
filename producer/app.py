
import os
import json
import time
import random
import threading
from datetime import datetime
from flask import Flask, request, jsonify
from kafka import KafkaProducer

BOOTSTRAP = os.environ.get("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_PREFIX = os.environ.get("TOPIC_PREFIX", "demo-")

app = Flask(__name__)

# Default topic list; can be overridden by POST /start body
DEFAULT_TOPICS = [f"{TOPIC_PREFIX}{name}" for name in ["alpha","beta","gamma"]]

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
)

threads = {}
stop_flags = {}

def produce_loop(topic: str):
    # Loop forever until stop flag is set; random rate 3-6 msgs/sec
    while not stop_flags.get(topic, False):
        continue
        rate = random.randint(3, 6)  # messages per second for this topic
        now = datetime.utcnow().isoformat() + "Z"
        for i in range(rate):
            msg = {
                "topic": topic,
                "ts": now,
                "seq_in_second": i,
                "payload": {"value": random.random()},
            }
            try:
                producer.send(topic, msg)
            except Exception as e:
                # Kafka will create topic on demand if broker is configured to allow it
                print(f"[producer] send failed for {topic}: {e}")
        # Flush each second to avoid buffering too long
        producer.flush()
        time.sleep(1.0)

@app.post("/start")
def start_producing():
    data = request.get_json(silent=True) or {}
    topics = data.get("topics") or DEFAULT_TOPICS

    started = []
    for t in topics:
        if t in threads and threads[t].is_alive():
            continue
        stop_flags[t] = False
        th = threading.Thread(target=produce_loop, args=(t,), daemon=True, name=f"producer-{t}")
        th.start()
        threads[t] = th
        started.append(t)
    return jsonify({"status": "ok", "started_topics": started}), 200

@app.post("/stop")
def stop_producing():
    data = request.get_json(silent=True) or {}
    topics = data.get("topics")
    if not topics:
        topics = list(threads.keys())
    for t in topics:
        stop_flags[t] = True
    return jsonify({"status": "ok", "stopped_topics": topics}), 200

@app.get("/status")
def status():
    alive = [t for t, th in threads.items() if th.is_alive()]
    return jsonify({"alive_topics": alive, "default_topics": DEFAULT_TOPICS})

if __name__ == "__main__":
    # Run with gunicorn in production; for simplicity use Flask dev server here
    app.run(host="0.0.0.0", port=5000)