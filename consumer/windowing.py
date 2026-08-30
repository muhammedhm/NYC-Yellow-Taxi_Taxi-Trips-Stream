"""
Sliding window aggregation, deliberately implemented as plain Python
(not Faust's built-in `.hopping()` Table windowing) so the logic is
transparent and unit-testable on its own -- see test_windowing.py.

Two independent mechanisms live here:

1. Sliding window (processing-time based): for each pickup zone, keeps a
   deque of recently-arrived events. Every `window_step` seconds, prunes
   anything older than `window_size` and emits one aggregate snapshot per
   zone covering "the trailing `window_size` as of right now". Using
   arrival/processing time (not event time) for the window boundary means
   the aggregation itself is naturally insensitive to the producer's
   deliberate out-of-order shuffling -- an event landing a few seconds
   "late" just gets folded into whichever window is open when it arrives.

2. Watermark / lateness detection (event-time based): tracks the maximum
   `pickup_datetime` seen so far as a watermark. Any event whose
   `pickup_datetime` trails the watermark by more than `late_threshold`
   is flagged `is_late=True` -- still aggregated (we don't want to drop
   revenue), but counted separately so "how out-of-order is my stream"
   is directly observable rather than silently absorbed.

Known limitation (documented, not fixed here): state lives in a plain
Python dict inside one consumer process. If the consumer crashes, all
in-flight window state is lost -- a production version would back this
with a Faust Table (which persists to a Kafka changelog topic and
recovers automatically) instead of a bare dict. Kept simple here so the
windowing math itself is easy to read, test, and explain live.
"""

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List


@dataclass
class ZoneBuffer:
    events: deque = field(default_factory=deque)  # (arrival_time, fare, total, distance)


class SlidingWindowAggregator:
    def __init__(self, window_size: timedelta = timedelta(minutes=5),
                 late_threshold: timedelta = timedelta(minutes=10)):
        self.window_size = window_size
        self.late_threshold = late_threshold
        self._buffers: Dict[int, ZoneBuffer] = {}
        self._watermark: datetime | None = None
        self.late_event_count = 0

    def add_event(self, pu_location_id: int, pickup_datetime: datetime,
                  fare_amount: float, total_amount: float, trip_distance: float,
                  arrival_time: datetime) -> bool:
        """Returns True if this event was flagged as a late arrival."""
        if self._watermark is None or pickup_datetime > self._watermark:
            self._watermark = pickup_datetime

        is_late = (self._watermark - pickup_datetime) > self.late_threshold
        if is_late:
            self.late_event_count += 1

        buf = self._buffers.setdefault(pu_location_id, ZoneBuffer())
        buf.events.append((arrival_time, fare_amount, total_amount, trip_distance))
        return is_late

    def _prune(self, buf: ZoneBuffer, now: datetime):
        cutoff = now - self.window_size
        while buf.events and buf.events[0][0] < cutoff:
            buf.events.popleft()

    def snapshot(self, now: datetime) -> List[dict]:
        """Prune every zone's buffer to the trailing window_size and return
        one aggregate row per zone that has at least one event in-window."""
        rows = []
        window_start = now - self.window_size
        late_since_last = self.late_event_count
        for pu_location_id, buf in self._buffers.items():
            self._prune(buf, now)
            if not buf.events:
                continue
            count = len(buf.events)
            total_revenue = sum(e[2] for e in buf.events)
            avg_fare = sum(e[1] for e in buf.events) / count
            rows.append({
                "window_start": window_start,
                "window_end": now,
                "pu_location_id": pu_location_id,
                "trip_count": count,
                "total_revenue": round(total_revenue, 2),
                "avg_fare": round(avg_fare, 2),
                "late_events_in_window": late_since_last,
            })
        return rows
