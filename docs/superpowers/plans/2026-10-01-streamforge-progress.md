# StreamForge execution ledger

Plan: `docs/superpowers/plans/2026-10-01-streamforge-implementation.md`

Ruling: Work in the current checkout rather than an isolated worktree — the user explicitly requested uninterrupted local progress and prohibited Git history operations — cost if wrong: concurrent uncommitted edits could overlap.

Task 1 ruling: A late event is ten minutes behind its own scheduled timestamp, not necessarily ten minutes behind the immediately preceding event — comparison now uses the same sequence position under normal and late scenarios — cost if wrong: the test could misrepresent the intended lateness model.

Task 1 complete — `.venv\\Scripts\\python.exe -m pytest tests/test_models.py tests/test_generator.py -v` → 7 passed; `git diff --check` → no whitespace errors. No Git staging or commit performed.

Task 2 ruling: Docker Desktop reports no `dockerDesktopLinuxEngine` pipe after startup, so the required Kafka/Postgres Compose runtime smoke check cannot yet run — continue with isolated unit-tested code and retry the smoke check after Docker becomes ready — cost if wrong: container configuration defects remain unverified until the engine starts.

Task 2 ruling: Confluent's KRaft image rejected `KAFKA_CLUSTER_ID`; logs established that its required environment variable is `CLUSTER_ID` — corrected the Compose setting — cost if wrong: the broker cannot initialize.

Task 2 complete — publisher unit test passed; Kafka, topic initializer, and Postgres are healthy; three topics and three tables were verified; a generated order was published to `streamforge.orders.v1` and consumed from partition 2. No Git staging or commit performed.

Completion ruling: Kafka requires `enableServiceLinks: false`, listeners bound
to `0.0.0.0`, a `localhost:29093` controller voter for the one-node KRaft
quorum, and `publishNotReadyAddresses: true` for the client Service. These
settings are intentionally specific to the local single-broker topology.

Completion evidence: `.venv\Scripts\python.exe -m pytest -v` passed 18 tests
with one Starlette TestClient deprecation warning. Compose was force-recreated
from the rebuilt image; `/analytics/summary` returned persisted non-zero order
and revenue values. The kind cluster had all application Pods Ready, an
available Metrics API, and a CPU-reading HPA. The `kind` command was not on
PATH, so the rebuilt image was loaded into the existing node with
`docker save ... | docker exec ... ctr images import -` before the application
deployments were restarted.
