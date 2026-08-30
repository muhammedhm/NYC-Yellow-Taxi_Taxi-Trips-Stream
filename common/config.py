"""
Shared configuration for the streaming pipeline (producer + consumer).

Reads a .env file at the project root if present, falling back to these
defaults, which match docker-compose.yml exactly.
"""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

_env_path = PROJECT_ROOT / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())

# Kafka -- host-side bootstrap servers (matches the 3 externally-mapped ports
# in docker-compose.yml: 9092/9094/9096)
KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9094,localhost:9096"
)
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "taxi-trips-stream")

# ClickHouse
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "taxi")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "taxi")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "taxi_stream")

# Producer
PRODUCER_METRICS_PORT = int(os.environ.get("PRODUCER_METRICS_PORT", "8001"))
DEFAULT_EVENTS_PER_SECOND = int(os.environ.get("DEFAULT_EVENTS_PER_SECOND", "25"))

# Consumer
CONSUMER_METRICS_PORT = int(os.environ.get("CONSUMER_METRICS_PORT", "8000"))
CONSUMER_GROUP_ID = os.environ.get("CONSUMER_GROUP_ID", "taxi-stream-processor")
