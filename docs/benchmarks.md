# Benchmark record

Fill this only with measured results.

| Run date | Hardware | Event rate | Duration | Kafka partitions | Observed throughput | API p95 | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-10-01 | Docker Desktop, local Compose | `EVENTS_PER_SECOND=50` additional producer | 68.6 s | 3 | 25.1 order records/s | Not measured | Order-topic end offsets increased by 1,720 while the normal producer also ran. |

Suggested experiment: run 60 seconds at 50, 500, and 1,000 events/second;
record the producer rate, Spark output rate, consumer lag, and API response
time. Explain the first bottleneck rather than claiming an unexplained maximum.
