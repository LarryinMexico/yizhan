# Yizhan Phase 2 (Production Depth) Plan

> **For agentic workers:** This plan is written at task-and-acceptance level. Before starting each work package, expand it into bite-sized steps with the superpowers:writing-plans skill, and re-check every tool version and chart name against its official documentation, because this plan was written two months earlier. Then execute with superpowers:subagent-driven-development or superpowers:executing-plans.

**Goal:** Turn the working v0 into a system whose reliability is measured and proven, running on Kubernetes, with a real application built on top of it.

**When:** Winter break, mid-December 2026 to mid-January 2027. Roughly 120–150 hours.

**Spec:** `docs/superpowers/specs/2026-11-12-webhook-delivery-platform-design.md`
**Contracts (normative):** `docs/design/contracts.md` — every contract change in this phase is made there first, with an ADR.
**Testing standard:** `docs/testing.md` (experiment protocol in section 7).

## Global Constraints

- All phase 1 constraints still apply.
- **`src/yizhan/outbox/relay.py` is written by the owner by hand.** The owner also runs every experiment and writes every experiment report personally.
- The order below is the priority order. If time runs out, stop at the cut line; never skip ahead and leave an earlier package half done.
- Production stays on one small VM. Anything needing several nodes or brokers runs locally in kind or k3d.
- Monthly cost target $0 on credits; the paid fallback must stay under about $12 per month.
- First task of the phase: finish anything in `docs/backlog.md` that was cut from phase 1.

## Order and time budget

| # | Work package | Hours | Depends on |
|---|---|---|---|
| P2.1 | Metrics and dashboards | 14 | — |
| P2.2 | Experiment E1 (prove the dual-write loss), then the transactional outbox | 16 | P2.1 |
| P2.3 | Load test E2 and failure experiments E3–E6 | 20 | P2.2 |
| P2.4 | Kubernetes: local kind, Helm chart, then k3s in production | 28 | P2.1 |
| P2.5 | Release Radar | 30 | P2.2 |
| — | **cut line** — everything above is required for the phase to count as done | | |
| P2.6 | React dashboard | 18 | — |
| P2.7 | Worker autoscaling on consumer lag (KEDA), experiment E7 | 8 | P2.4 |
| P2.8 | Terraform for AWS resources | 8 | P2.4 |
| P2.9 | Tracing with OpenTelemetry | 6 | P2.1 |

Metrics come before the outbox and the experiments because none of them can be judged without numbers. Kubernetes comes after the experiments so that a baseline exists on the simple deployment first.

---

## P2.1 Metrics and dashboards

**Learn first:** counters, gauges, histograms; the pull model; labels and cardinality; percentiles from histograms; RED (rate, errors, duration) for request paths; what an SLO and an error budget are.

**Build:**
- `prometheus-client` in all three processes; `/metrics` on the API and a small metrics HTTP server in the worker and scheduler.
- Metrics (names are part of the contract; add a section to `contracts.md`):

| Metric | Type | Labels |
|---|---|---|
| `yizhan_http_requests_total` | counter | `route`, `method`, `status` |
| `yizhan_http_request_duration_seconds` | histogram | `route`, `method` |
| `yizhan_messages_accepted_total` | counter | — |
| `yizhan_delivery_attempts_total` | counter | `outcome` (`success`, `http_error`, `timeout`, `connect_error`, `tls_error`, `blocked_address`, `request_error`) |
| `yizhan_attempt_duration_seconds` | histogram | `outcome` |
| `yizhan_delivery_end_to_end_seconds` | histogram | — (message `created_at` to first successful attempt) |
| `yizhan_first_attempt_delay_seconds` | histogram | — (message `created_at` to start of attempt 1) |
| `yizhan_deliveries_terminal_total` | counter | `status` |
| `yizhan_worker_records_total` | counter | `outcome` (the `Outcome` enum) |
| `yizhan_scheduler_published_total` | counter | — |
| `yizhan_due_backlog` | gauge | — (pending deliveries past `next_attempt_at`) |
| `yizhan_stuck_deliveries` | gauge | — (the detection query in contracts section 3) |
| `yizhan_outbox_unpublished` | gauge | — (added in P2.2) |

- Never label by application ID, endpoint ID, or URL: unbounded cardinality.
- Kafka consumer lag from a Kafka exporter.
- Prometheus and Grafana as Compose services under a `monitoring` profile; dashboards stored as JSON in `deploy/grafana/`.
- One Grafana dashboard with four rows: API (rate, error rate, p50/p99), deliveries (attempt outcomes, end-to-end p50/p99, terminal statuses), queue (consumer lag, due backlog, stuck), resources (CPU and memory per container).
- SLOs written in `docs/slo.md`:
  1. 99% of first attempts start within 5 seconds of the message being accepted.
  2. API availability 99.5% over 30 days (non-5xx ÷ all requests).
  3. Zero stuck deliveries.
- Alert rules for: stuck deliveries > 0 for 10 minutes; consumer lag growing for 10 minutes; API 5xx rate above 1% for 5 minutes.

**Accept:**
- Integration test: after one successful and one failed delivery, `/metrics` shows the expected counter increments.
- With the stack up and `scripts/smoke.sh` run ten times, every dashboard panel shows data.
- Forcing a stuck delivery (insert a row by hand) fires the alert in a local test.
- ADR-005: which SLOs and why these numbers.

## P2.2 Experiment E1, then the transactional outbox

**Learn first:** the dual-write problem; why "write then publish" and "publish then write" both fail; the outbox pattern; polling relay versus change data capture; why the relay gives at-least-once and why that is fine here.

**Step A — E1, on phase 1 code, before any fix.**
- Add a fault-injection setting `FAULT_CRASH_AFTER_COMMIT_PROBABILITY` (default 0; only honoured when `ENVIRONMENT != production`) that makes the API process exit right after the database commit in `POST /v1/messages`.
- Hypothesis (owner writes it before running): with probability p and N requests, about p × N deliveries end up stuck.
- Run 5,000 messages at p = 0.01 with the API container set to restart automatically. Count with the stuck-delivery query.
- Report `docs/experiments/001-dual-write-loss.md`.
- **Accept:** stuck count is greater than zero and roughly matches the hypothesis.

**Step B — the outbox.**
- Contract change (update `contracts.md` and write ADR-006 first):

```sql
CREATE TABLE outbox (
  id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  topic        text NOT NULL,
  key          text NOT NULL,
  payload      bytea NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
```

- `POST /v1/messages`, replay, and the scheduler insert outbox rows in the **same transaction** as their state change and no longer publish to Kafka themselves.
- Relay algorithm (owner implements `relay.py`): in a loop, in one transaction, `SELECT ... WHERE published_at IS NULL ORDER BY id LIMIT :batch FOR UPDATE SKIP LOCKED`; publish each and await acknowledgements; set `published_at = now()`; commit. On publish failure, roll back. A crash after publishing re-publishes on the next pass; the worker's duplicate rule already makes that harmless.
- A cleanup job deletes published rows older than 24 hours.
- Model writes the failing tests first; owner implements:
  - `test_o1_row_published_once_and_marked`
  - `test_o2_publish_failure_leaves_row_unpublished`
  - `test_o3_two_relays_do_not_double_publish` (200 rows, two relays in parallel → 200 records)
  - `test_o4_order_preserved_per_key` (rows for one key are published in `id` order)
  - `test_o5_api_accepts_messages_while_kafka_is_down` (Kafka container paused: `POST /v1/messages` still returns 202; after unpause, deliveries complete)
  - `test_o6_crash_after_publish_before_mark` (duplicate record reaches the worker and is skipped)
- **Step C — rerun E1** with the same parameters. **Accept:** stuck count is exactly 0; report updated with before and after.

## P2.3 Load test and failure experiments

**Learn first:** closed versus open load models; coordinated omission; why averages hide problems; how to read a latency histogram; what "recovery time" means and how to measure it.

**Build:**
- `loadtest/` using Locust: a user class that sends messages at a target rate to N endpoints; a fast sink receiver (`loadtest/sink.py`) that records message IDs and can inject latency or errors per endpoint.
- `scripts/check_invariants.sh`: runs the three SQL checks in `docs/testing.md` section 7 plus the receiver comparison (every accepted message ID received; duplicate rate).
- Experiments, each a report from the template. Run each on the laptop stack first, then E2 once on the production-size VM.

| ID | Question | Procedure | Pass criteria |
|---|---|---|---|
| E2 | What sustained throughput can one worker handle? | Step the rate up every 3 minutes (50, 100, 200, 400, 800 messages per second) against a fast sink | Report the highest rate where first-attempt delay p99 stays under 5 s and lag does not grow; identify the bottleneck with evidence (CPU, database, or Kafka) |
| E3 | Does killing workers lose or corrupt anything? | At 200 messages per second, `SIGKILL` the worker every 30 s for 5 minutes | Invariant checks all pass; every message received at least once; duplicate rate reported |
| E4 | What happens when Kafka is down? | At 100 messages per second, stop Kafka for 60 s, then start it | API keeps returning 202 throughout (outbox); after restart the backlog drains; time to drain reported; invariants pass |
| E5 | What happens when PostgreSQL restarts? | At 100 messages per second, restart PostgreSQL | API returns 503 while it is down and recovers by itself; no process needs a manual restart; invariants pass |
| E6 | Does one slow endpoint hurt the others? | Ten endpoints, one with 9-second responses, 100 messages per second spread evenly | Report first-attempt delay p99 for the nine healthy endpoints. This is **expected to be bad**; the result is the motivation for phase 3 fairness work, so a "fail" here is the desired finding |

**Accept:** five reports merged, each with commands, commit hash, numbers, and a conclusion. Any invariant failure found is fixed (reproduced first, per the testing standard) and the fix is described in the report.

## P2.4 Kubernetes

**Learn first:** Pod, Deployment, StatefulSet, Service, Ingress, ConfigMap, Secret, PersistentVolumeClaim; liveness versus readiness probes; requests and limits; rolling updates; what an operator is; how Helm templating and values work.

**Decide (ADR-007) before building:** Kafka on Kubernetes through the Strimzi operator or as a hand-written StatefulSet. Consider memory (an operator costs a few hundred MB on a 4 GB node), what is standard in industry, and what is learned. Decide also whether production Prometheus and Grafana run in the cluster or use Grafana Cloud's free tier to save memory.

**Build, in this order:**
1. Images published to GitHub Container Registry from CI (multi-architecture or ARM, matching the VM), tagged with the commit hash.
2. `deploy/helm/yizhan/`: Deployments for api, worker, scheduler, relay; a migration Job as a pre-upgrade hook; StatefulSets or operator resources for PostgreSQL, Kafka, Redis; Services; Ingress with TLS through cert-manager; probes wired to `/healthz` and `/readyz`; resource requests and limits on everything; values files `values-local.yaml` and `values-prod.yaml`.
3. Local: a kind (or k3d) cluster with three nodes, created by `make cluster-up`; the chart installs cleanly; `scripts/smoke.sh` passes against it.
4. Production: move the VM to 4 GB if it is not already; install k3s (single node, bundled Traefik); restore the database from a `pg_dump` of the Compose deployment; switch DNS; keep the Compose deployment stopped but intact for one week as a rollback path.
5. Deploy workflow changes to `helm upgrade --install` run on the host over SSH (the Kubernetes API port is not exposed publicly).

**Accept:**
- `helm lint` and `helm template | kubeconform` pass in CI.
- On kind: deleting the worker pod during a 100-message run loses nothing (invariant script passes).
- On kind: a rolling update of the API under load produces zero failed requests (readiness probe and graceful shutdown working); evidence in `docs/experiments/007-rolling-update.md`.
- Production: smoke test passes on the public URL; the node survives a reboot; `kubectl top nodes` shows memory below 85%.
- Runbook `docs/runbooks/k3s.md` covers install, upgrade, rollback, backup, and restore, and the restore was actually performed once.

## P2.5 Release Radar

**Learn first:** OAuth 2 authorisation-code flow; polling with ETags and conditional requests; being a good API citizen (rate limits, user agent); dogfooding.

**Research before building (first task, 2 hours):** confirm the current way to list new releases for PyPI (the RSS updates feed; the XML-RPC `changelog_since_serial` call if the feed's 100-item window is too small) and for GitHub (per-repository releases Atom feed or REST API with a token), with their rate limits. Measure PyPI's actual release volume for one hour before choosing a polling interval. npm is optional and only added if its change feed is confirmed usable.

**Design:**
- A separate package `src/radar/` with its own PostgreSQL schema `radar` and its own container. It talks to Yizhan **only through the public HTTP API**, as one Yizhan application, exactly like an outside customer.
- Yizhan contract additions (ADR-008, update `contracts.md` first):
  - `endpoints.body_mode`: `envelope` (default) or `data_only` (the request body is exactly the `data` object). Slack and Discord webhooks need `data_only`.
  - `applications.endpoint_limit` raised for the Radar application by an admin script.
  - A retention job deleting messages older than 14 days (the release feed is high volume).
- Event types: `release.<ecosystem>.<normalised package name>`, with suffix `.slack` or `.discord` for pre-formatted variants. For each release Radar sends the generic message and, only if subscribers of that kind exist, the formatted ones.
- Radar tables: `users` (GitHub ID, login), `targets` (user, kind `generic | slack | discord`, URL, Yizhan endpoint ID), `subscriptions` (target, ecosystem, package), `seen_releases` (ecosystem, package, version; unique) for deduplication, `feed_items` for the public feed.
- Each target maps to one Yizhan endpoint whose `event_types` list is kept in sync with that target's subscriptions.
- **Live feed:** Radar registers its own endpoint `/hooks/feed` with Yizhan, subscribed to all generic release events. The handler verifies the signature with `yizhan.delivery.signing.verify_signature`, deduplicates on `Yizhan-Message-Id`, and stores the item. The public home page shows the latest releases from this table. This gives continuous real deliveries and demonstrates the receiving side.
- Pages (server-rendered, Jinja and HTMX, no separate frontend build): home with the live feed and delivery statistics pulled from Yizhan's API; sign in with GitHub; manage targets and subscriptions; a "send test notification" button.
- Radar uses an idempotency key `"{ecosystem}:{package}:{version}"` when calling Yizhan, so a poller restart cannot double-send.

**Accept:**
- Tests: poller parsing against recorded real feed samples; `seen_releases` prevents duplicates across restarts; `/hooks/feed` rejects a bad signature with 401 and accepts a duplicate message ID without storing twice; subscription changes update the Yizhan endpoint's `event_types`.
- Live: the public feed page updates with real releases; a Discord or Slack test webhook owned by the owner receives a correctly formatted message for a subscribed package; 24 hours of running with zero stuck deliveries and the invariant script passing.
- SSRF rules still apply to user-supplied target URLs (they are Yizhan endpoints, so this is inherited; a test confirms a private address is rejected at creation).
- `docs/experiments/008-radar-first-week.md`: one week of real numbers — releases ingested, deliveries, success rate, p50/p99 end-to-end latency.

---

*Cut line. Packages below are done only if time remains.*

## P2.6 React dashboard

Vite, React, TypeScript, in `dashboard/`. Built to static files served by the API or the Ingress. Views: deliveries table with filters and keyset "load more"; delivery detail with an attempt timeline; endpoint list with per-endpoint success rate over 24 hours (needs one new aggregate API route — contract change first); replay. API key in `sessionStorage`. **Accept:** type-check and unit tests in CI; a Playwright smoke test that logs in with a key, sees a delivery, and replays it.

## P2.7 Autoscaling on consumer lag

KEDA `ScaledObject` on the worker Deployment with the Kafka trigger, on the local kind cluster only (production has one node and little spare memory). Experiment E7: push a 50,000-message backlog; compare time to drain with 1 fixed worker versus autoscaling 1–6. **Accept:** report with a lag-over-time graph for both runs; explanation of why scaling beyond the partition count (6) gives nothing.

## P2.8 Terraform

`deploy/terraform/`: the EC2 instance, Elastic IP, security group, an S3 bucket for database backups with a lifecycle rule, the budget and its alert. Import the hand-made resources rather than recreating them. **Accept:** `terraform plan` shows no changes against the live account; the nightly `pg_dump` uploads to S3 and a restore from S3 was performed once.

## P2.9 Tracing

OpenTelemetry in the API, relay, worker; trace context carried in a Kafka record header so one trace covers accept → outbox → relay → worker → HTTP attempt. Export to Grafana Cloud or a local Tempo. **Accept:** one screenshot in the README of a full trace for a retried delivery.

## Phase 2 definition of done

1. P2.1–P2.5 accepted.
2. Experiment reports 001–008 merged, written by the owner.
3. ADR-005 to ADR-008 written by the owner.
4. Production runs on k3s, has run for 7 consecutive days with zero stuck deliveries, and the SLO dashboard shows whether each SLO was met.
5. `docs/resume-notes.md` updated with measured numbers only.
6. README updated: architecture diagram, experiment summary table, link to the live Radar page.
