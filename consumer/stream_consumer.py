"""
Task 3: Stream Ingestion, Processing & Modeling.

Faust consumer for `taxi-trips-stream`. Two things happen per event:
  1. It's appended to an in-memory buffer, flushed to ClickHouse's
     raw_trip_events table every RAW_FLUSH_INTERVAL seconds (Task 3:
     "write the raw streaming events into an analytical table").
  2. It's folded into the SlidingWindowAggregator (windowing.py). Every
     AGG_FLUSH_INTERVAL seconds, a snapshot of all zones' trailing
     5-minute windows is upserted into ClickHouse's trip_window_agg
     table (Task 3: "calculate a sliding window aggregation ... while
     upserting aggregations").

Run:
  faust -A consumer.stream_consumer worker -l info
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import faust
from prometheus_client import Counter, Gauge, Histogram, start_http_server

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "common"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config  # noqa: E402
from logging_config import get_logger, log_event  # noqa: E402
from windowing import SlidingWindowAggregator  # noqa: E402
from clickhouse_writer import ClickHouseWriter  # noqa: E402

logger = get_logger("consumer")

RAW_FLUSH_INTERVAL = 5.0     # seconds between raw-event batch inserts
AGG_FLUSH_INTERVAL = 60.0    # seconds between sliding-window snapshots (the window "step")
WINDOW_SIZE = timedelta(minutes=5)
LATE_THRESHOLD = timedelta(minutes=10)

# ---- Task 4: Prometheus metrics -----------------------------------------
MESSAGES_CONSUMED = Counter("consumer_messages_consumed_total", "Total trip events consumed from Kafka")
LATE_EVENTS_TOTAL = Counter("consumer_late_events_total", "Total events flagged as late arrivals (watermark check)")
RAW_FLUSH_ERRORS = Counter("consumer_raw_flush_errors_total", "Failed raw-event batch inserts to ClickHouse")
AGG_FLUSH_ERRORS = Counter("consumer_agg_flush_errors_total", "Failed window-aggregate upserts to ClickHouse")
RAW_BUFFER_SIZE = Gauge("consumer_raw_buffer_size", "Events currently buffered, awaiting the next ClickHouse flush")
ACTIVE_ZONES = Gauge("consumer_active_zones", "Distinct pickup zones with events in the current window snapshot")
END_TO_END_LATENCY = Histogram(
    "consumer_end_to_end_latency_seconds",
    "Time from producer_sent_at to consumer processing -- the whole pipeline's real-world lag",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
)

# Faust broker URL takes multiple hosts separated by `;`
_hosts = config.KAFKA_BOOTSTRAP_SERVERS.split(",")
BROKER_URL = "kafka://" + ";".join(_hosts)

app = faust.App(
    config.CONSUMER_GROUP_ID,
    broker=BROKER_URL,
    value_serializer="json",
    topic_partitions=6,
)


class TripEvent(faust.Record, serializer="json"):
    trip_seq: int
    vendor_id: int
    pickup_datetime: str
    dropoff_datetime: str
    passenger_count: float
    trip_distance: float
    pu_location_id: int
    do_location_id: int
    rate_code_id: float
    payment_type: int
    fare_amount: float
    tip_amount: float
    total_amount: float
    producer_sent_at: str


trip_topic = app.topic(config.KAFKA_TOPIC, value_type=TripEvent, partitions=6)

# Process-local state (see windowing.py docstring for the documented
# limitation: this doesn't survive a consumer restart).
aggregator = SlidingWindowAggregator(window_size=WINDOW_SIZE, late_threshold=LATE_THRESHOLD)
raw_buffer: list = []
ch_writer: ClickHouseWriter = None  # set in on_start


@app.on_worker_init.connect
def init_clickhouse_writer(**kwargs):
    global ch_writer
    ch_writer = ClickHouseWriter()
    start_http_server(config.CONSUMER_METRICS_PORT)
    log_event(logger, "metrics_server_started", port=config.CONSUMER_METRICS_PORT)


@app.agent(trip_topic)
async def process_trips(stream):
    async for event in stream:
        arrival_time = datetime.now(timezone.utc).replace(tzinfo=None)
        pickup_dt = datetime.fromisoformat(event.pickup_datetime)

        MESSAGES_CONSUMED.inc()

        sent_at = datetime.fromisoformat(event.producer_sent_at)
        latency_seconds = (datetime.now(timezone.utc) - sent_at).total_seconds()
        END_TO_END_LATENCY.observe(max(latency_seconds, 0))

        is_late = aggregator.add_event(
            pu_location_id=event.pu_location_id,
            pickup_datetime=pickup_dt,
            fare_amount=event.fare_amount,
            total_amount=event.total_amount,
            trip_distance=event.trip_distance,
            arrival_time=arrival_time,
        )
        if is_late:
            LATE_EVENTS_TOTAL.inc()

        raw_buffer.append({
            "trip_seq": event.trip_seq,
            "vendor_id": event.vendor_id,
            "pickup_datetime": pickup_dt,
            "dropoff_datetime": datetime.fromisoformat(event.dropoff_datetime),
            "passenger_count": event.passenger_count,
            "trip_distance": event.trip_distance,
            "pu_location_id": event.pu_location_id,
            "do_location_id": event.do_location_id,
            "rate_code_id": int(event.rate_code_id),
            "payment_type": event.payment_type,
            "fare_amount": event.fare_amount,
            "tip_amount": event.tip_amount,
            "total_amount": event.total_amount,
            "producer_sent_at": sent_at.replace(tzinfo=None),
            "is_late_arrival": 1 if is_late else 0,
        })
        RAW_BUFFER_SIZE.set(len(raw_buffer))


@app.timer(interval=RAW_FLUSH_INTERVAL)
async def flush_raw_events():
    global raw_buffer
    if not raw_buffer:
        return
    batch, raw_buffer = raw_buffer, []
    try:
        await ch_writer.insert_raw_events(batch)
        RAW_BUFFER_SIZE.set(len(raw_buffer))
    except Exception as e:
        RAW_FLUSH_ERRORS.inc()
        log_event(logger, "raw_flush_failed", error=str(e), batch_size=len(batch))
        # Put the batch back so it's retried on the next tick rather than lost.
        raw_buffer = batch + raw_buffer
        RAW_BUFFER_SIZE.set(len(raw_buffer))


@app.timer(interval=AGG_FLUSH_INTERVAL)
async def flush_window_aggregates():
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = aggregator.snapshot(now=now)
    ACTIVE_ZONES.set(len(rows))
    if not rows:
        return
    try:
        await ch_writer.upsert_window_aggregates(rows)
        log_event(logger, "window_snapshot_taken", zones=len(rows),
                  total_trips_in_windows=sum(r["trip_count"] for r in rows),
                  late_events_total=aggregator.late_event_count)
    except Exception as e:
        AGG_FLUSH_ERRORS.inc()
        log_event(logger, "aggregate_flush_failed", error=str(e))


if __name__ == "__main__":
    app.main()
