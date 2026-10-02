#!/usr/bin/env bash
set -euo pipefail

for topic in streamforge.orders.v1 streamforge.payments.v1 streamforge.dead-letter.v1; do
  kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists \
    --topic "$topic" --partitions 3 --replication-factor 1
done
