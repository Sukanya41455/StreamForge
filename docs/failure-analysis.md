# Failure experiments

| Experiment | Command | Observe | Record |
| --- | --- | --- | --- |
| Restart streaming | `docker compose restart streaming` | checkpoint recovery and lag | restart time; aggregate consistency |
| Duplicate events | set `SCENARIO=duplicates` | deduplication by `event_id` | aggregate count before/after |
| Late events | set `SCENARIO=late` | watermark policy | dead-letter/late behavior |
| Increased traffic | set `EVENTS_PER_SECOND=500` | lag and batch duration | first saturated resource |

State whether the system offers at-least-once delivery and how the Postgres
composite-key upsert makes replay effects idempotent. Do not call it exactly
once unless an experiment proves the full end-to-end claim.

## Observed runs

- 2026-10-01: `docker compose restart streaming` completed in 20.53 seconds.
  Immediately afterward, `/analytics/summary` returned 8 orders, 1047.10
  revenue, and a 100% payment-success rate. This records the container restart
  command duration, not an end-to-end recovery or data-loss guarantee.
- 2026-10-01: an additional producer ran with `EVENTS_PER_SECOND=50` for
  68.6 seconds. The three order-topic end offsets changed from
  `3377,3456,3341` to `3931,4079,3884`: 1,720 order records total, or 25.1
  order records/second. The normal Compose producer remained active, and API
  latency was not measured.
