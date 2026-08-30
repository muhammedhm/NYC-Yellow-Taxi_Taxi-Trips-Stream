"""
Standalone test of the sliding window logic -- no Kafka, no Faust, no
ClickHouse needed. Run: python consumer/test_windowing.py
"""

import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from windowing import SlidingWindowAggregator


def test_basic_aggregation():
    agg = SlidingWindowAggregator(window_size=timedelta(minutes=5))
    t0 = datetime(2023, 1, 1, 12, 0, 0)

    # 3 events in zone 100, arriving at t0, t0+1min, t0+2min
    agg.add_event(100, t0, fare_amount=10.0, total_amount=12.0, trip_distance=2.0, arrival_time=t0)
    agg.add_event(100, t0, fare_amount=20.0, total_amount=24.0, trip_distance=3.0, arrival_time=t0 + timedelta(minutes=1))
    agg.add_event(100, t0, fare_amount=30.0, total_amount=36.0, trip_distance=4.0, arrival_time=t0 + timedelta(minutes=2))

    snap = agg.snapshot(now=t0 + timedelta(minutes=2))
    assert len(snap) == 1, f"expected 1 zone, got {len(snap)}"
    row = snap[0]
    assert row["pu_location_id"] == 100
    assert row["trip_count"] == 3, f"expected 3 trips, got {row['trip_count']}"
    assert row["total_revenue"] == 72.0, f"expected 72.0, got {row['total_revenue']}"
    assert abs(row["avg_fare"] - 20.0) < 0.001
    print("test_basic_aggregation: PASS")


def test_window_pruning_drops_old_events():
    agg = SlidingWindowAggregator(window_size=timedelta(minutes=5))
    t0 = datetime(2023, 1, 1, 12, 0, 0)

    agg.add_event(100, t0, fare_amount=10.0, total_amount=10.0, trip_distance=1.0, arrival_time=t0)
    # 10 minutes later -- the first event should have aged out of a 5-min window
    later = t0 + timedelta(minutes=10)
    agg.add_event(100, later, fare_amount=50.0, total_amount=50.0, trip_distance=5.0, arrival_time=later)

    snap = agg.snapshot(now=later)
    assert len(snap) == 1
    assert snap[0]["trip_count"] == 1, f"expected only the recent event, got {snap[0]['trip_count']}"
    assert snap[0]["total_revenue"] == 50.0
    print("test_window_pruning_drops_old_events: PASS")


def test_out_of_order_events_flagged_late():
    agg = SlidingWindowAggregator(late_threshold=timedelta(minutes=10))
    t0 = datetime(2023, 1, 1, 12, 0, 0)

    # Establish watermark at t0
    is_late_1 = agg.add_event(200, t0, fare_amount=15.0, total_amount=15.0, trip_distance=2.0, arrival_time=t0)
    assert is_late_1 is False

    # Watermark advances to t0+20min
    agg.add_event(200, t0 + timedelta(minutes=20), fare_amount=25.0, total_amount=25.0,
                  trip_distance=3.0, arrival_time=t0 + timedelta(minutes=20))

    # A trip whose pickup_datetime is t0+1min arrives late (after the watermark
    # has already moved to t0+20min) -- 19 minutes behind the watermark,
    # past the 10-minute late_threshold -- must be flagged.
    is_late_3 = agg.add_event(200, t0 + timedelta(minutes=1), fare_amount=5.0, total_amount=5.0,
                              trip_distance=1.0, arrival_time=t0 + timedelta(minutes=21))
    assert is_late_3 is True, "event 19 minutes behind the watermark should be flagged late"
    assert agg.late_event_count == 1
    print("test_out_of_order_events_flagged_late: PASS")


def test_multiple_zones_isolated():
    agg = SlidingWindowAggregator(window_size=timedelta(minutes=5))
    t0 = datetime(2023, 1, 1, 12, 0, 0)

    agg.add_event(100, t0, fare_amount=10.0, total_amount=10.0, trip_distance=1.0, arrival_time=t0)
    agg.add_event(200, t0, fare_amount=99.0, total_amount=99.0, trip_distance=9.0, arrival_time=t0)

    snap = agg.snapshot(now=t0)
    zones = {r["pu_location_id"]: r for r in snap}
    assert set(zones.keys()) == {100, 200}
    assert zones[100]["total_revenue"] == 10.0
    assert zones[200]["total_revenue"] == 99.0
    print("test_multiple_zones_isolated: PASS")


if __name__ == "__main__":
    test_basic_aggregation()
    test_window_pruning_drops_old_events()
    test_out_of_order_events_flagged_late()
    test_multiple_zones_isolated()
    print("\nAll windowing tests passed.")
