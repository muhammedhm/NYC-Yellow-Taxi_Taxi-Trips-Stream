# NYC Taxi Real-Time Streaming Pipeline — Assignment 2

A local, Docker-based real-time pipeline that simulates live taxi trip
events, streams them through Kafka, processes them with a sliding-window
aggregation, stores both raw events and aggregates in ClickHouse, and
exposes full observability through Prometheus + Grafana.

---

## 1. Architecture at a glance

```
 [Parquet file]                                        [ClickHouse]
      |                                                   ^      ^
      v                                                   |      |
 stream_producer.py --(JSON, 10-50 msg/s)--> Kafka topic      raw_trip_events
   (confluent-kafka)      taxi-trips-stream        |         trip_window_agg
      |                    (6 partitions,           |              ^
      | /metrics           3 brokers, KRaft)        v              |
      v                          |            stream_consumer.py --+
 [Prometheus] <-----------------+---------------(Faust, confluent-kafka
      |          /metrics       |                 under the hood)
      |                         v                       |
      v                   [Kafka UI]                     | /metrics
 [Grafana]                (inspect topics,               v
  dashboard                 partitions, lag)        [Prometheus]
```

Everything runs via a single `docker compose up -d` except the Python
producer and consumer processes themselves, which run on the host (so you
can see their logs directly and restart them independently while iterating).

---

## 2. Tech stack chosen, and why (one tool per category, decided up front)

| Component | Choice | Why |
|---|---|---|
| Message broker | **Apache Kafka, KRaft mode, 3 brokers** | No Zookeeper needed. Originally used `bitnami/kafka`, but Bitnami discontinued free images mid-build (see Incident #1 below) — switched to the official `apache/kafka` image, which is free and actively maintained by the Kafka project itself. |
| Kafka client library | **confluent-kafka** (producer) | Official Confluent client, C-library-backed (librdkafka), the de facto standard for raw produce/consume outside a stream-processing framework. |
| Streaming engine | **Faust** (`faust-streaming` fork) | Kafka Streams-like API in pure Python — keeps the whole pipeline in one language, easier to explain live than standing up a JVM/Scala toolchain for Kafka Streams, or a Spark cluster for PySpark Streaming. |
| OLAP storage | **ClickHouse** | `ReplacingMergeTree` gives near-native upsert semantics for the windowed aggregation table, which Task 3 explicitly asks for. |
| Observability | **Prometheus + Grafana** | As specified in the assessment. `danielqsj/kafka-exporter` fills the gap for broker/consumer-lag metrics, which neither Kafka nor Faust expose to Prometheus natively. |
| Bonus | **Kafka UI** (Provectus) | Not required, but makes partitions/consumer lag/message contents visible with zero extra code — worth having for the interview demo itself. |

**Deliberate scope reduction vs. the assessment's own allowance:** the
3-broker Kafka cluster satisfies "multi-broker" as asked, but if your
machine can't spare the RAM, `docker-compose.yml` has a comment showing
exactly what to change to run a single broker instead (per the
assessment's own "no need for multiple nodes" note).

---

## 3. Setup steps, in order

### Task 1 — Kafka infrastructure

```bash
# 1. Install Docker Desktop, allocate at least 6-8GB RAM / 4 CPUs to it
# 2. From the project root:
docker compose up -d
docker compose ps          # all 8 services should show "Up"

# 3. Create the topic (6 partitions, replication factor 3)
./docker/create_topics.sh
# Windows PowerShell: run the docker exec commands inside the script manually,
# or use Git Bash / WSL to run the .sh file directly.

# 4. Verify:
#    Kafka UI    -> http://localhost:8080   (see the 3-broker cluster + topic)
#    Prometheus  -> http://localhost:9090   (Status > Targets)
#    Grafana     -> http://localhost:3000   (auto-logged in as admin)
```

### Task 2 — Stream producer

```bash
pip install -r requirements.txt
# Copy your NYC taxi Parquet file(s) into data/raw/, e.g.:
#   data/raw/yellow_tripdata_2023-01.parquet

python producer/stream_producer.py --rate 25
# --loop to replay forever; --file to point at a specific file/path
```

### Task 3 — Faust consumer + ClickHouse

```bash
# Apply the ClickHouse schema (creates taxi_stream database + 2 tables)
Get-Content sql/clickhouse_schema.sql | docker exec -i taxi_clickhouse clickhouse-client --user taxi --password taxi --multiquery
# (macOS/Linux: use `<` instead of the Get-Content pipe)

# Sanity-check the windowing math with zero infra involved:
python consumer/test_windowing.py     # all 4 tests should print PASS

# Run the consumer (with the producer already running in another terminal):
faust -A consumer.stream_consumer worker -l info
```

### Task 4 — Monitoring

Already wired up by `docker compose up -d` from Task 1, plus the metrics
endpoints the producer (`:8001/metrics`) and consumer (`:8000/metrics`)
expose automatically once they're running. Open Grafana
(`http://localhost:3000`) — the "NYC Taxi Streaming Pipeline" dashboard is
auto-provisioned, no manual setup needed. It shows: producer/consumer
throughput, consumer lag, end-to-end latency (p50/p95/p99), broker count,
active pickup zones, late-event rate, and pipeline error rates.

### Verify the whole thing end-to-end

```bash
# Row counts growing in real time:
docker exec taxi_clickhouse clickhouse-client --user taxi --password taxi `
  --query "SELECT count(*) FROM taxi_stream.raw_trip_events"

# Latest windowed aggregates (FINAL forces dedup at read time -- see schema notes):
docker exec taxi_clickhouse clickhouse-client --user taxi --password taxi `
  --query "SELECT * FROM taxi_stream.trip_window_agg FINAL ORDER BY window_end DESC LIMIT 10"
```

---

## 4. Key design decisions and assumptions

1. **Producer replays historical data as if live**, at a configurable rate
   (10–50 events/sec per the assessment), with a deliberate small
   out-of-order shuffle (50-event buffer) so the stream isn't suspiciously
   sorted — Task 3's out-of-order handling has something real to catch.
2. **Sliding window is processing-time based**, implemented as plain
   Python (`consumer/windowing.py`) rather than Faust's built-in
   `.hopping()` table windowing — chosen for transparency: the logic is
   independently unit-testable with zero Kafka/Faust involved
   (`test_windowing.py`, 4 tests, all passing), and easy to explain line
   by line in a live demo. A production version could move to Faust's
   native windowing for changelog-topic-backed fault tolerance (see
   limitation #2 below).
3. **Out-of-order detection is separate from windowing**: a watermark
   tracks the max `pickup_datetime` seen; anything trailing it by more
   than 10 minutes is flagged `is_late` (stored per-row, and counted) —
   observable in Grafana as "Late Events Rate" rather than silently
   absorbed into the aggregate.
4. **Raw events are appended (MergeTree), aggregates are upserted
   (ReplacingMergeTree)** — exactly the two different semantics Task 3
   asks for, on the two different tables that need them.
5. **Known limitation, left as-is on purpose:** windowing state
   (`SlidingWindowAggregator`'s per-zone buffers) lives in a plain Python
   dict inside one consumer process. If the consumer crashes, in-flight
   window state is lost — a production version would back this with a
   Faust `Table` (which persists to a Kafka changelog topic and recovers
   automatically on restart) instead. Kept as a bare dict here so the
   windowing math itself stays simple to read and test.
6. **ClickHouse dedup is merge-time, not write-time**: `ReplacingMergeTree`
   only removes duplicate keys during background merges, so a query run
   immediately after an upsert may still show duplicate rows for the same
   window. Use `FINAL` (as in the verification query above) whenever you
   need guaranteed-deduplicated reads.

---

## 5. Incidents encountered during development, and how they were fixed

Documenting these because they were real bugs caught during actual
testing against your data — not hypothetical edge cases.

| # | Symptom | Root cause | Fix |
|---|---|---|---|
| 1 | `manifest for bitnami/kafka:3.7 not found` | Bitnami moved `kafka` behind a paid "Secure Images" subscription and pulled every free tag from Docker Hub | Switched `docker-compose.yml` to the official `apache/kafka:latest` image — different env var names (no `KAFKA_CFG_` prefix), 3-listener setup instead of 2, `kafka-topics.sh` at `/opt/kafka/bin/` instead of on `PATH` |
| 2 | `ModuleNotFoundError: No module named 'duckdb'` in Streamlit (Assignment 1, same root cause applies here) | Streamlit was launched from Anaconda's base environment, not the project's `venv` | Launch via `venv\Scripts\python.exe -m streamlit run ...` to bypass PATH ambiguity entirely |
| 3 | `The '<' operator is reserved for future use` | PowerShell doesn't support bash-style `<` stdin redirection | Use `Get-Content file.sql \| docker exec -i ... --multiquery` instead |
| 4 | `IO Error: No files found that match the pattern ...` | Parquet file was never copied into `taxi-streaming/data/raw/` (separate folder from the Assignment 1 project) | Copy the file over, or pass `--file` pointing at its existing location |
| 5 | `orjson.JSONDecodeError: unexpected character, expected a JSON value` in the Faust consumer | Some rows have `RatecodeID = NULL` → becomes `NaN` in JSON via Python's `json` module (which incorrectly allows the non-standard `NaN` literal); Faust's parser (`orjson`) correctly rejects it as invalid JSON | `COALESCE(RatecodeID, -1)` at the SQL source in the producer, plus a general NaN→null sanitizer in `make_payload()` as defense-in-depth for any other nullable column |

Verified after fix #5: 2000 real events replayed through the full
parse → window → buffer pipeline (ClickHouse network calls mocked, since
this development environment has no live broker/ClickHouse to test
against) — 0 parse errors, 79 zones aggregated correctly, 21 events
correctly flagged as late by the watermark logic.

---

## 6. Task 5 — Production architecture: scaling to 50,000 events/sec on a cloud provider

This local setup (3 local brokers, 1 consumer process, 1 ClickHouse node)
tops out somewhere in the low thousands of events/sec — nowhere near
50k/sec. Getting there isn't "add more of the same," it's several
qualitatively different changes. Using AWS as the concrete example
(the same shape applies on GCP/Azure with their equivalent managed services):

### 6.1 Message broker: self-managed Kafka → Amazon MSK

- Move from 3 self-managed brokers to **Amazon MSK** (managed Kafka),
  sized for throughput: at 50k events/sec and an estimated ~1KB/event
  average payload, that's ~50MB/sec sustained write throughput before
  replication. MSK sizing guidance targets keeping per-broker network
  throughput comfortably under the instance's cap — this points to
  **6+ brokers** on `kafka.m5.2xlarge` or larger, not 3.
- **Increase `taxi-trips-stream` partitions** from 6 to **50-100**.
  Partition count is the hard ceiling on consumer parallelism — you
  cannot have more active consumers in a group than partitions. Going
  from 6 to ~60 partitions means ~60-way parallelism is possible on the
  consumer side.
- **Replication factor 3** (not 3 brokers total — 3 *copies* of each
  partition), spread across 3 Availability Zones for durability.
- Switch producer `acks` from `all` (used locally for correctness/demo
  clarity) to a throughput/durability trade-off appropriate for the
  actual data: `acks=1` if occasional loss on a broker failure is
  acceptable for analytics workloads, keeping `acks=all` only if this
  feeds anything billing- or compliance-adjacent.

### 6.2 Producer: single script → distributed ingestion tier

- One Python process can plausibly push a few thousand events/sec at
  most (bounded by GIL + JSON serialization + network I/O). At 50k/sec,
  run **multiple stateless producer instances** (e.g., an ECS/Fargate
  service, auto-scaled on a target throughput metric), each handling a
  shard of the source data or a partition-key range.
- Batch/compress: enable `compression.type=lz4` or `zstd` and larger
  `linger.ms`/`batch.size` — at this volume, per-message overhead adds
  up fast; local demo used `linger.ms=10` for low-latency clarity, not
  throughput.
- If ingestion is from a real upstream system rather than replayed
  files, prefer a **Kafka Connect source connector** over a hand-rolled
  producer script — gets you offset tracking, retries, and scaling
  for free.

### 6.3 Stream processing: single Faust worker → autoscaled consumer group

- **This is the biggest local-vs-production gap.** One Faust worker
  processing all 6 partitions caps out well before 50k/sec (bounded by
  Python's single-threaded event loop, even with async I/O). Options,
  in increasing order of throughput ceiling:
  - Run **multiple Faust worker processes** in the same consumer group
    (`CONSUMER_GROUP_ID` unchanged) — Kafka automatically spreads the
    ~60 partitions across however many workers are running. Deploy as
    an ECS/EKS service, autoscaled on **consumer lag** (via the same
    kafka-exporter metric already wired into Prometheus locally — this
    is exactly the signal an HPA/KEDA autoscaler would key off).
  - If Faust's Python-level throughput ceiling is still hit even with
    many workers, this is the point to seriously evaluate **migrating
    to Kafka Streams (JVM) or Apache Flink** for the hot path — both
    have materially higher per-core throughput than Python-based
    stream processing. This is a real architectural fork worth naming
    explicitly rather than papering over: Faust's "one language, easy
    to demo" advantage (why it was chosen locally) is exactly what
    costs throughput headroom at this scale.
  - Either way, move the windowing state from the in-process Python
    dict (documented limitation, section 4.5) to something that
    survives a worker restart / rebalance — Faust's own `Table`
    (changelog-topic-backed) if staying on Faust, or Flink's
    RocksDB-backed state if migrating.

### 6.4 Storage: single ClickHouse node → cluster

- **ClickHouse Cloud**, or self-managed ClickHouse in a **sharded +
  replicated cluster** (e.g., 3+ shards × 2 replicas across AZs), fronted
  by a `Distributed` table engine so writes/reads fan out automatically.
- Batch inserts more aggressively than the local 5-second raw-event
  flush — ClickHouse strongly prefers large, infrequent inserts (low
  thousands/sec of *insert operations*, not rows, is the real limit)
  over many small ones; at 50k events/sec, buffer for longer (10-30s) or
  route through a **Buffer table engine** or a Kafka-to-ClickHouse
  connector (ClickHouse has a native `Kafka` table engine that pulls
  directly from a topic, which is worth strongly considering as a
  replacement for the custom Faust→ClickHouse write path entirely).

### 6.5 Observability at scale

- **Amazon Managed Service for Prometheus + Amazon Managed Grafana**
  instead of self-hosted containers — avoids Prometheus itself becoming
  a scaling bottleneck/single point of failure once there are dozens of
  producer/consumer instances to scrape.
- Add **alerting** (not present in the local demo): consumer lag
  exceeding a threshold, end-to-end latency p99 breaching an SLO, and
  ClickHouse insert error rate — all backed by metrics already exposed
  locally, just needing Alertmanager/Grafana alert rules layered on.

### 6.6 Schema and compatibility

- The local demo sends raw JSON with no schema enforcement — fine at
  small scale, risky at 50k/sec with multiple producer/consumer versions
  potentially deployed simultaneously during a rollout. Introduce a
  **schema registry** (Confluent Schema Registry or AWS Glue Schema
  Registry) with **Avro or Protobuf** — smaller wire format (helps
  throughput directly) and enforced compatibility checks on schema
  evolution.

### 6.7 What does NOT need to change

Worth being explicit about this in a design review: the **data model**
(raw events + windowed aggregates, same two ClickHouse tables), the
**windowing semantics** (still a trailing sliding window, still
watermark-based lateness detection), and the **monitoring signals that
matter** (throughput, lag, latency, error rate) are all scale-invariant.
50k events/sec is an infrastructure and deployment-topology problem
layered on top of an architecture that was already designed correctly —
which is the point of building it this way locally first.
