"""
ClickHouse writer used by the Faust consumer.

Wraps clickhouse-connect (a synchronous HTTP client) and runs each insert
in a thread executor so it never blocks Faust's asyncio event loop --
important because Faust agents and timers all share one event loop, and
a blocking network call inside either would stall every other partition
being processed by this worker.
"""

import asyncio
from typing import List, Dict

import clickhouse_connect

import config
from logging_config import get_logger, log_event

logger = get_logger("clickhouse_writer")

RAW_COLUMNS = [
    "trip_seq", "vendor_id", "pickup_datetime", "dropoff_datetime",
    "passenger_count", "trip_distance", "pu_location_id", "do_location_id",
    "rate_code_id", "payment_type", "fare_amount", "tip_amount",
    "total_amount", "producer_sent_at", "is_late_arrival",
]

AGG_COLUMNS = [
    "window_start", "window_end", "pu_location_id", "trip_count",
    "total_revenue", "avg_fare", "late_events_in_window",
]


class ClickHouseWriter:
    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_PORT,
            username=config.CLICKHOUSE_USER,
            password=config.CLICKHOUSE_PASSWORD,
            database=config.CLICKHOUSE_DATABASE,
        )
        log_event(logger, "clickhouse_connected", host=config.CLICKHOUSE_HOST,
                  database=config.CLICKHOUSE_DATABASE)

    def _insert_sync(self, table: str, columns: List[str], rows: List[list]):
        self.client.insert(table, rows, column_names=columns)

    async def insert_raw_events(self, rows: List[Dict]):
        if not rows:
            return
        data = [[r[c] for c in RAW_COLUMNS] for r in rows]
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._insert_sync, "raw_trip_events", RAW_COLUMNS, data)
        log_event(logger, "raw_events_flushed", rows=len(rows))

    async def upsert_window_aggregates(self, rows: List[Dict]):
        if not rows:
            return
        data = [[r[c] for c in AGG_COLUMNS] for r in rows]
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._insert_sync, "trip_window_agg", AGG_COLUMNS, data)
        log_event(logger, "window_aggregates_upserted", windows=len(rows))

    def close(self):
        self.client.close()
