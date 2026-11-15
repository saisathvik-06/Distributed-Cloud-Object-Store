"""Worker settings, read once from environment variables."""
import os

WORKER_ID = os.environ.get("WORKER_ID", "worker1")
CONTROLLER_URL = os.environ.get("CONTROLLER_URL", "http://controller:8000").rstrip("/")
# Address the controller should use to reach this worker (service name, never an IP).
ADVERTISE_URL = os.environ.get("ADVERTISE_URL", f"http://{WORKER_ID}:8000").rstrip("/")
DATA_DIR = os.environ.get("DATA_DIR", "/data")
# Largest chunk body this worker accepts.
MAX_CHUNK_BYTES = int(os.environ.get("MAX_CHUNK_BYTES", str(1024 * 1024)))
