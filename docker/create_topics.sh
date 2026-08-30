#!/usr/bin/env bash
# =====================================================================
# Creates the taxi-trips-stream topic once the Kafka cluster is up.
#
# Usage:  ./docker/create_topics.sh
#
# Partitions=6 lets up to 6 consumer instances process in parallel later
# (Task 3). Replication=3 matches the 3-broker cluster; if you scaled
# down to a single broker (see docker-compose.yml note), change this to
# --replication-factor 1 or topic creation will fail with "replication
# factor larger than available brokers".
# =====================================================================
set -euo pipefail

TOPIC_NAME="taxi-trips-stream"
PARTITIONS=6
REPLICATION_FACTOR=3

echo "Waiting for kafka-1 to be ready..."
until docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list &> /dev/null; do
    sleep 2
done

echo "Creating topic '${TOPIC_NAME}' (partitions=${PARTITIONS}, replication=${REPLICATION_FACTOR})..."
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --if-not-exists \
    --topic "${TOPIC_NAME}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION_FACTOR}"

echo ""
echo "Topic details:"
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --describe \
    --topic "${TOPIC_NAME}"
