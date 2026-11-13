# Yizhan — Webhook Delivery Platform: Design

Date: 2026-11-12
Status: Approved by owner 2026-11-13
Name: **Yizhan** (驛站, a relay station on a historical courier route) for the engine; **Release Radar** for the showcase app.

## 1. Purpose

Yizhan is a multi-tenant service that delivers events to HTTP endpoints reliably. A customer calls one API to say "send this event"; Yizhan queues it, delivers it, retries failures, signs each request, and records every attempt.

Release Radar is a first-party application built on Yizhan's public API. It watches PyPI, npm, and GitHub for new package releases and notifies subscribers through their webhook, Slack, or Discord URL. It gives the platform real traffic around the clock.

The project has two goals:

1. Learn the backend technologies that North American backend internships ask for, by using each where it is the natural design.
2. Produce a deployed, public project the owner can defend in depth in interviews, with measured results.

### Non-goals

- Paying customers, billing, or a team/organization permission model.
- Exactly-once delivery. The guarantee is at-least-once (section 4).
- Multi-region deployment, service mesh, gRPC.
- A managed Kubernetes or managed Kafka service in production (cost).

## 2. Success criteria

- A public URL with API documentation where anyone can create an account, register an endpoint, send an event, and see it delivered.
- Release Radar delivers real release events to real subscriptions every day.
- A written experiment log with measured numbers: sustained throughput, p50/p99 delivery latency, recovery time after a worker or broker is killed, and proof that no accepted event is lost in those failures.
- An ADR (architecture decision record) for every major decision, written by the owner in their own words.
- Monthly hosting cost of $0 while credits last, and at most about $12 afterward.

## 3. Concepts

| Term | Meaning |
|---|---|
| Application | A tenant. Owns API keys, endpoints, and messages. |
| Endpoint | A URL that receives events, with a signing secret and a list of subscribed event types. |
| Message | One event sent by an application: an event type, a JSON payload, and an optional idempotency key. |
| Delivery | One message destined for one endpoint. A message fans out to one delivery per subscribed endpoint. |
| Attempt | One HTTP request for a delivery, with its response status, latency, and error. |

## 4. Delivery semantics

- **At-least-once.** A delivery is retried until the endpoint returns 2xx or attempts run out. Receivers may see duplicates and deduplicate using the `Yizhan-Message-Id` header.
- **Idempotent ingestion.** A repeated `POST /v1/messages` with the same idempotency key returns the original message and creates nothing new.
- **Ordering is not guaranteed.** Deliveries to one endpoint are usually in order (Kafka key = endpoint ID) but retries break ordering. This is documented for users.
- **Retry schedule.** Exponential backoff with jitter: about 10 s, 1 min, 5 min, 30 min, 2 h, 6 h, 12 h. After 8 failed attempts the delivery is marked `failed` and published to a dead-letter topic. Users can replay it manually.
- **Timeout.** 10 seconds per attempt. Any non-2xx response, timeout, or connection error counts as a failure.

## 5. Architecture

```
client ──► API (FastAPI) ──► PostgreSQL (source of truth)
                │                  ▲           ▲
                ▼                  │           │
              Kafka ──► delivery workers ──► customer endpoints
                ▲                  │
                └── retry scheduler ◄── (due retries read from PostgreSQL)

Redis: rate limits, idempotency-key cache, endpoint circuit state
```

### Components

**API service (Python, FastAPI).** Authenticates with API keys. Manages applications, endpoints, and messages. On `POST /v1/messages` it stores the message and its deliveries, then publishes one Kafka record per delivery. Also serves attempt history and manual replay.

**PostgreSQL.** The source of truth for applications, endpoints, messages, deliveries, and attempts. Delivery state (`pending`, `succeeded`, `failed`) and `next_attempt_at` live here.

**Kafka.** Topic `deliveries` (key = endpoint ID) carries work to the workers. Topic `deliveries.dlq` records deliveries that exhausted their attempts. Single broker in KRaft mode in production; a three-broker cluster locally for failure experiments.

**Delivery worker (Python in phases 1–2; Go in phase 3).** Consumes `deliveries`, sends the signed HTTP request, records the attempt, updates the delivery state, and commits the Kafka offset only after the outcome is stored. Recording is idempotent through a unique key on (delivery ID, attempt number), so a redelivered Kafka record cannot create a duplicate attempt row.

**Retry scheduler.** Polls PostgreSQL for deliveries whose `next_attempt_at` has passed, using `FOR UPDATE SKIP LOCKED`, and republishes them to Kafka. Kafka has no delayed delivery, so retry timing is kept in the database. The alternative, tiered retry topics, is recorded in an ADR.

**Redis.** Per-application rate limiting and daily quotas on the API, and (phase 3) per-endpoint health and circuit-breaker state. Idempotency keys are enforced by a PostgreSQL unique constraint, not a Redis cache.

**Dashboard.** Phase 1: a minimal static page listing deliveries and attempts through the JSON API. Phase 2: a React and TypeScript dashboard with delivery logs, per-endpoint success rate, and replay.

**Release Radar (phase 2).** Pollers read the PyPI, npm, and GitHub release feeds and publish events through Yizhan's public API, the same way any customer would. Users sign in with GitHub and subscribe a package to a URL. Slack and Discord are supported through payload templates, since both accept plain HTTP webhooks.

### Known gap in phase 1, fixed in phase 2

In phase 1 the API writes to PostgreSQL and then publishes to Kafka as two separate steps. A crash between them loses the Kafka record while the database says the delivery is pending. Phase 2 first demonstrates this loss with a failure experiment, then fixes it with a transactional outbox: the API writes an outbox row in the same transaction, and a relay publishes outbox rows to Kafka. The gap is left in deliberately so the owner sees the failure before learning the fix.

## 6. Data model

This is a summary. The exact schema, routes, record formats, and algorithms are in `docs/design/contracts.md`, which is normative and wins where the two differ (for example: replay creates a new delivery row; endpoints are soft-deleted).

- `applications(id, name, created_at)`
- `api_keys(id, application_id, key_hash, created_at, revoked_at)`
- `endpoints(id, application_id, url, secret, event_types[], disabled, created_at)`
- `messages(id, application_id, event_type, payload jsonb, idempotency_key, created_at)`, unique on (application_id, idempotency_key)
- `deliveries(id, message_id, endpoint_id, status, attempt_count, next_attempt_at, created_at, updated_at)`
- `attempts(id, delivery_id, attempt_no, status_code, latency_ms, error, created_at)`, unique on (delivery_id, attempt_no)
- `outbox(id, topic, key, payload, created_at, published_at)` (phase 2)

## 7. Security

- API keys are stored hashed and shown once.
- Each request is signed with HMAC-SHA256 over the timestamp and body, using the endpoint's secret, so receivers can verify origin and reject replays.
- SSRF protection: endpoint URLs must be HTTPS, and the worker refuses to connect to private, loopback, or link-local addresses, checked after DNS resolution.
- Payload limit of 256 KB. Rate limits per application.
- Secrets come from environment variables and are never committed.

## 8. Phases

### Phase 1 — v0 (target: 2026-12-10, about 40–60 hours)

Scope: API for applications, endpoints, and messages; Kafka; one Python delivery worker with retries, signing, and idempotent attempt recording; retry scheduler; minimal dashboard page; Docker Compose; GitHub Actions CI; deployed on one cloud VM with a public URL.

Technologies learned: FastAPI, PostgreSQL, Kafka, Redis, Docker, CI/CD, basic cloud deployment.

Owner hand-writes: the delivery worker (consume, deliver, record, commit) and the retry-schedule logic. Models write the rest; the owner reviews every change.

Exit criteria: an event sent to the public API reaches a real endpoint; a failing endpoint is retried on schedule and ends in `failed` after 8 attempts; CI is green; the resume line can be written truthfully.

### Phase 2 — production depth (winter break, mid-December to mid-January)

In priority order; items at the bottom are cut first if time runs out.

1. Metrics and dashboards (Prometheus, Grafana), because nothing below can be judged without numbers.
2. Transactional outbox, preceded by the experiment that shows the phase 1 loss.
3. Load tests and failure experiments, with results in the experiment log.
4. Kubernetes: migrate from Docker Compose to k3s with Helm charts.
5. Release Radar.
6. React dashboard.
7. Worker autoscaling on Kafka consumer lag (KEDA).
8. Terraform for the cloud resources.
9. Tracing with OpenTelemetry.

Items 1–5 are required; 6–9 are done only if time remains. Details: `docs/superpowers/plans/2026-11-13-phase2-production-depth.md`.

Owner hand-writes: the outbox relay, and runs and writes up every experiment.

### Phase 3 — spring semester

1. Rewrite the delivery worker in Go; benchmark it against the Python worker.
2. Circuit breaker for failing endpoints and per-application fairness, so one slow tenant cannot starve others.
3. Three-broker Kafka failure experiments (local cluster).

A detailed implementation plan is written for one phase at a time, starting with phase 1.

## 9. Deployment and cost

Design choices that keep cost near zero:

- Production runs on **one small VM**: Docker Compose in phase 1, single-node k3s from phase 2. k3s has no control-plane fee.
- Kafka runs as **one broker in KRaft mode** with a capped heap. Estimated total memory for k3s, Kafka, PostgreSQL, Redis, and the services is about 4 GB; 8 GB is comfortable.
- **Multi-node Kubernetes and multi-broker Kafka experiments run locally** on the owner's laptop with kind or k3d, at no cost. Production does not need to be large for the learning to happen.
- Observability can use Grafana Cloud's free tier if the VM is short on memory.

Hosting ladder, in order of preference (prices checked 2026-11-12 from secondary sources; confirm on the provider's page before signing up):

1. **AWS Free plan** — $100 at sign-up and up to $100 more for activities, valid 6 months, for people who have never had an AWS account. Covers a 4 GB instance through roughly May 2027 and puts AWS on the resume.
2. **Azure for Students** — $100 per year, no credit card, renewable while a student.
3. **Oracle Cloud Always Free** — 2 OCPU and 12 GB ARM, free indefinitely, but capacity is often unavailable.
4. **Hetzner CAX21** — 4 vCPU, 8 GB, about €10.49 per month. The paid fallback.

Moving between rungs is a redeploy of the same Helm charts, which is itself a useful exercise. A domain name costs roughly $10–12 per year.

## 10. Testing

- Unit tests for pure logic: backoff schedule, signature, SSRF address checks.
- Integration tests against real PostgreSQL, Kafka, and Redis in containers (Testcontainers), not mocks.
- End-to-end test through Docker Compose with a mock receiver that can be told to fail, time out, or respond slowly.
- Every bug is reproduced through the real entry point before it is fixed, and the reproduction command is kept.
- Tooling: `uv` for the environment, `ruff` for lint and format, GitHub Actions for CI.

## 11. Working method

- The owner decides; Claude presents options and trade-offs. Each decision becomes an ADR in `docs/adr/`, written by the owner in English.
- The owner hand-writes the core mechanisms named in each phase. Models write boilerplate, infrastructure configuration, and tests; the owner reviews every change before merge.
- Experiments are logged in `docs/experiments/` with the command, the observation, and the numbers.
- Repository is public on GitHub. README, docs, and commit messages are in English.

## 12. Risks

| Risk | Mitigation |
|---|---|
| Interview preparation crowds out the project and v0 slips | Phase 1 scope is fixed and small; anything new goes to phase 2. |
| Phase 2 is too large for one winter break | Items are in priority order with an explicit cut line. |
| A public service that sends HTTP requests gets abused | SSRF checks, rate limits, HTTPS-only endpoints, payload limit. |
| Free credits or free-tier capacity run out | Hosting ladder; the deployment is portable across providers. |
| The owner cannot explain model-written code | Hand-written core, mandatory review, ADRs and experiment write-ups in the owner's words. |

## 13. Open decisions

1. The name Yizhan has not been checked for conflicts (domain, GitHub, PyPI).
2. The owner has an existing AWS account with unused credit, so AWS is the first hosting rung. Its plan type (Free or Paid) and the credit expiry date must be confirmed before deployment (M4).
