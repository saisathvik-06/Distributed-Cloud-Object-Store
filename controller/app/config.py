"""Controller settings, read once from environment variables."""
import os


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def _float(name: str, default: float) -> float:
    return float(os.environ.get(name, str(default)))


CHUNK_SIZE = _int("CHUNK_SIZE_BYTES", 1024 * 1024)          # "1 MB" = 1,048,576 bytes
REPLICATION_FACTOR = _int("REPLICATION_FACTOR", 2)
HEARTBEAT_INTERVAL_S = _float("HEARTBEAT_INTERVAL_S", 3)     # handed to workers at registration
WORKER_TIMEOUT_S = _float("WORKER_TIMEOUT_S", 10)            # silence before a worker is DEAD
REPAIR_GRACE_S = _float("REPAIR_GRACE_S", 15)                # wait after death before re-replicating
REPAIR_INTERVAL_S = _float("REPAIR_INTERVAL_S", 5)           # how often the repair loop runs
STARTUP_GRACE_S = _float("STARTUP_GRACE_S", 20)              # no repairs right after the controller starts
WORKER_CONNECT_TIMEOUT_S = _float("WORKER_CONNECT_TIMEOUT_S", 2)
WORKER_TRANSFER_TIMEOUT_S = _float("WORKER_TRANSFER_TIMEOUT_S", 15)
MAX_OBJECT_SIZE = _int("MAX_OBJECT_SIZE_BYTES", 0)          # 0: no assignment-specific size limit
ORPHAN_MIN_AGE_S = _float("ORPHAN_MIN_AGE_S", 60)            # reconciliation never deletes younger files
DATA_DIR = os.environ.get("DATA_DIR", "/data")
DB_PATH = os.path.join(DATA_DIR, "metadata.db")

MAX_METADATA_KEYS = _int("MAX_METADATA_KEYS", 0)           # 0: unlimited
MAX_METADATA_BYTES = _int("MAX_METADATA_BYTES", 0)
SCRUB_INTERVAL_S = _float("SCRUB_INTERVAL_S", 30)
RECONCILE_INTERVAL_S = _float("RECONCILE_INTERVAL_S", 30)
DELETE_RETRY_INTERVAL_S = _float("DELETE_RETRY_INTERVAL_S", 5)
