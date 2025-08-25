import requests
import random
import logging
import time
# Setup Python logging (OTel log injection will capture these if OTEL_PYTHON_LOG_CORRELATION=true)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

apps = [
    ("http://127.0.0.1:9001/", "http://127.0.0.1:9001/error"),
    ("http://127.0.0.1:9002/", "http://127.0.0.1:9002/error"),
]

def send_request():
    for app in apps:
        url = app[0]
        try:
            resp = requests.get(url, timeout=2)
            logging.info(f"Sent to {url} - Status: {resp.status_code}")
        except Exception as e:
            logging.error(f"Request to {url} failed: {e}")

if __name__ == "__main__":
    while(True):
        send_request()
        # Sleep for a short duration to avoid overwhelming the server
        time.sleep(2)

