"""
Task 2: Stream Producer.

Reads the NYC taxi Parquet file(s) already downloaded for Assignment 1
(data/raw/yellow_tripdata_*.parquet) and replays them into the
`taxi-trips-stream` Kafka topic as if they were arriving live, at a
configurable rate between 10-50 events/second (per the assessment).

Design decisions / assumptions:
  - Each trip becomes one JSON message. Message key = pickup LocationID
    (as a string), so trips from the same zone land on the same
    partition -- useful later if the consumer wants any per-zone local
    state, and demonstrates non-uniform key distribution across the
    6 partitions.
  - Two timestamps travel with every message: `pickup_datetime` (the
    trip's real/event time, used for windowing in Task 3) and
    `producer_sent_at` (wall-clock time the producer actually sent it,
    used to measure end-to-end pipeline latency in Task 4).
  - Rows are replayed in a *mildly shuffled* order (a small sliding
    buffer, not strict pickup-time order) to deliberately simulate the
    out-of-order arrival real systems see, so Task 3's windowing logic
    has something real to handle rather than a suspiciously tidy
    already-sorted stream.
  - Rate limiting: a fixed target rate (10-50 msgs/sec, CLI-configurable)
    using simple sleep-based pacing; actual achieved rate is logged
    every 5 seconds so any drift is visible.

Usage:
  python producer/stream_producer.py --rate 25 --file data/raw/yellow_tripdata_2023-01.parquet
  python producer/stream_producer.py --rate 25 --loop     # replay forever, for long demo runs
"""

import argparse
import json
import os
import random
import sys
import time
from collections import deque
from datetime import datetime, timezone

import duckdb
from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, start_http_server

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "common"))
import config  # noqa: E402
from logging_config import get_logger, log_event  # noqa: E402

logger = get_logger("producer")

SHUFFLE_BUFFER_SIZE = 50  # how far out of order messages can be, deliberately

MESSAGES_SENT = Counter("producer_messages_sent_total", "Total trip events sent to Kafka")
SEND_ERRORS = Counter("producer_send_errors_total", "Total Kafka delivery failures")
CURRENT_RATE = Gauge("producer_current_rate_per_sec", "Actual achieved send rate, msgs/sec")


def load_trip_rows(parquet_path: str):
    """Yield trip rows as dicts, ready to serialize -- pulls only the columns
    the stream actually needs (a subset of Assignment 1's fact columns)."""
    con = duckdb.connect()
    query = f"""
        SELECT
            VendorID AS vendor_id,
            tpep_pickup_datetime AS pickup_datetime,
            tpep_dropoff_datetime AS dropoff_datetime,
            COALESCE(passenger_count, 1) AS passenger_count,
            trip_distance,
            PULocationID AS pu_location_id,
            DOLocationID AS do_location_id,
            RatecodeID AS rate_code_id,
            payment_type,
            fare_amount,
            tip_amount,
            total_amount
        FROM read_parquet('{parquet_path}')
        WHERE tpep_pickup_datetime IS NOT NULL
          AND trip_distance > 0
          AND fare_amount >= 0
        ORDER BY tpep_pickup_datetime
    """
    df = con.execute(query).df()
    for row in df.itertuples(index=False):
        yield row._asdict()


def shuffled_stream(rows, buffer_size: int = SHUFFLE_BUFFER_SIZE):
    """Reservoir-style shuffle: keeps a small buffer, always emits a random
    element from it, so output is close to but not exactly time-ordered."""
    buf = deque()
    for row in rows:
        buf.append(row)
        if len(buf) >= buffer_size:
            idx = random.randrange(len(buf))
            buf[idx], buf[-1] = buf[-1], buf[idx]
            yield buf.pop()
    while buf:
        idx = random.randrange(len(buf))
        buf[idx], buf[-1] = buf[-1], buf[idx]
        yield buf.pop()


def make_payload(row: dict, trip_seq: int) -> dict:
    payload = dict(row)
    payload["trip_seq"] = trip_seq
    payload["pickup_datetime"] = row["pickup_datetime"].isoformat()
    payload["dropoff_datetime"] = row["dropoff_datetime"].isoformat()
    payload["producer_sent_at"] = datetime.now(timezone.utc).isoformat()
    return payload


def delivery_report(err, msg):
    if err is not None:
        SEND_ERRORS.inc()
        log_event(logger, "delivery_failed", error=str(err))


def run(parquet_paths: list, rate_per_sec: int, loop: bool):
    if not (10 <= rate_per_sec <= 50):
        log_event(logger, "rate_out_of_recommended_range", rate=rate_per_sec,
                   note="Assessment asks for 10-50 events/sec; proceeding anyway.")

    start_http_server(config.PRODUCER_METRICS_PORT)
    log_event(logger, "metrics_server_started", port=config.PRODUCER_METRICS_PORT)

    producer = Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "linger.ms": 10,
        "acks": "all",
    })

    interval = 1.0 / rate_per_sec
    trip_seq = 0
    sent_in_window = 0
    window_start = time.time()

    log_event(logger, "producer_start", rate_per_sec=rate_per_sec,
              files=parquet_paths, topic=config.KAFKA_TOPIC)

    def one_pass():
        nonlocal trip_seq, sent_in_window, window_start
        for path in parquet_paths:
            rows = load_trip_rows(path)
            window_start = time.time()  # reset here: exclude Parquet load time from the rate calc
            sent_in_window = 0
            for row in shuffled_stream(rows):
                payload = make_payload(row, trip_seq)
                trip_seq += 1
                key = str(row["pu_location_id"]).encode("utf-8")
                producer.produce(
                    config.KAFKA_TOPIC,
                    key=key,
                    value=json.dumps(payload, default=str).encode("utf-8"),
                    callback=delivery_report,
                )
                producer.poll(0)
                MESSAGES_SENT.inc()
                sent_in_window += 1

                if time.time() - window_start >= 5:
                    rate = sent_in_window / (time.time() - window_start)
                    CURRENT_RATE.set(rate)
                    log_event(logger, "throughput_sample", target_rate=rate_per_sec,
                              actual_rate=round(rate, 2), total_sent=trip_seq)
                    sent_in_window = 0
                    window_start = time.time()

                time.sleep(interval)

    try:
        one_pass()
        while loop:
            log_event(logger, "replay_loop_restart", total_sent_so_far=trip_seq)
            one_pass()
    except KeyboardInterrupt:
        log_event(logger, "producer_interrupted_by_user")
    finally:
        producer.flush(10)
        log_event(logger, "producer_stopped", total_messages_sent=trip_seq)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", action="append", dest="files",
                         help="Parquet file to stream; repeat for multiple months")
    parser.add_argument("--rate", type=int, default=config.DEFAULT_EVENTS_PER_SECOND,
                         help="Target events/sec (10-50 recommended)")
    parser.add_argument("--loop", action="store_true", help="Replay files forever")
    args = parser.parse_args()

    files = args.files or [
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                     "data", "raw", "yellow_tripdata_2023-01.parquet")
    ]
    run(files, args.rate, args.loop)
