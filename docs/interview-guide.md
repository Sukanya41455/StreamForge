# StreamForge interview prompts

- **Why Kafka?** It decouples event producers and consumers, persists ordered
  partition logs, and allows a failed consumer to resume from offsets.
- **Why are duplicates possible?** A producer/consumer retry can happen after
  an acknowledged side effect is uncertain. The design treats delivery as
  at-least-once and makes aggregate writes idempotent.
- **Event time versus processing time?** The event timestamp represents when a
  marketplace action occurred; processing time represents when Spark saw it.
  Watermarks bound how long Spark retains state for late events.
- **What does checkpointing recover?** Spark query progress and state metadata,
  allowing a restarted job to continue rather than blindly reprocess from the
  beginning.
- **What does consumer lag mean?** The difference between the latest Kafka
  offset and a consumer group’s committed offset. It is a throughput/backpressure
  signal, not automatically data loss.
- **Why is the HPA not enough?** CPU-based HPA can scale API replicas, but it
  cannot directly solve Kafka partition limits, Spark state pressure, or a slow
  database sink.
