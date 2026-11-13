# Yizhan Phase 3 (Performance and Fairness) Plan

> **For agentic workers:** This plan is written at task-and-acceptance level. Before starting each work package, expand it into bite-sized steps with the superpowers:writing-plans skill and re-check tool and library versions against official documentation. Then execute with superpowers:subagent-driven-development or superpowers:executing-plans.

**Goal:** Fix the weaknesses the phase 2 experiments exposed, learn a second backend language by rewriting the hottest component, and package the project for interviews.

**When:** Spring semester 2027, about 10 hours per week, finishing before the internship starts.

**Spec:** `docs/superpowers/specs/2026-11-12-webhook-delivery-platform-design.md`
**Contracts (normative):** `docs/design/contracts.md`
**Testing standard:** `docs/testing.md`

## Global Constraints

- All earlier constraints apply.
- **The Go worker is written by the owner by hand.** Models may explain Go concepts, review, and write the tests and the benchmark harness, but not the worker.
- Each package starts from a measured problem (an experiment report) and ends with a measurement showing the change. No optimisation without a before and an after.
- First task: reread the phase 2 experiment reports and reorder the packages below if the data says something else is the bigger problem. Record the reasoning in ADR-009.

## Order and time budget

| # | Work package | Hours |
|---|---|---|
| P3.1 | Fairness and circuit breaker | 25 |
| P3.2 | Go delivery worker and benchmark | 45 |
| P3.3 | Three-broker Kafka failure experiment | 12 |
| P3.4 | Interview packaging | 15 |
| — | **cut line** | |
| P3.5 | Token-bucket rate limiting and per-endpoint delivery rate limits | 10 |
| P3.6 | Endpoint secret encryption and key rotation | 8 |

---

## P3.1 Fairness and circuit breaker

**Starting evidence:** experiment E6 (one slow endpoint degrades the others).

**Learn first:** head-of-line blocking; bulkheads; the circuit breaker states (closed, open, half-open); why a retry storm makes an outage worse; noisy-neighbour problems in multi-tenant systems.

**Decide (ADR-010):** how to isolate slow endpoints. Options to weigh: (a) a per-endpoint concurrency cap inside the worker; (b) a separate `deliveries.slow` topic and worker pool that endpoints are moved to when their recent latency is high; (c) both. Consider what happens to partition ordering, how "slow" is detected and un-detected, and what a single partition full of one endpoint's records does under each option.

**Build:**
- Per-endpoint health in Redis: rolling counts of recent successes and failures, and recent latency.
- Circuit breaker per endpoint:
  - **closed** → **open** after 5 consecutive failures.
  - While **open**, the worker does not send. It reschedules the delivery (`next_attempt_at = now + open interval`) **without** consuming an attempt number, and records nothing in `attempts`.
  - After the open interval (start 30 s, doubling to a maximum of 10 minutes), **half-open**: one probe delivery is allowed. Success → closed. Failure → open again with the next interval.
  - A delivery older than 3 days is marked `failed` regardless, so nothing waits forever.
- Contract changes go into `contracts.md` section 6 first: the new skip outcome `CIRCUIT_OPEN`, the reschedule-without-attempt rule, and the age limit. Invariant 2 (attempt_count equals attempt rows) must still hold.
- The chosen isolation mechanism from ADR-010.
- Endpoint auto-disable: after 5 days of continuous failure, set `disabled = true` and expose the reason in the API.

**Accept:**
- Tests: state transitions with an injected clock (closed→open at the 5th failure; open blocks sends; half-open allows exactly one probe under concurrency; success closes; failure reopens with a doubled interval); a `CIRCUIT_OPEN` reschedule leaves `attempt_count` unchanged; the age limit fails a 3-day-old delivery.
- **Rerun E6.** Report `docs/experiments/009-fairness.md` with before and after first-attempt delay p99 for the healthy endpoints. Pass: the healthy endpoints' p99 with a slow neighbour is within 2× their p99 without one.
- New experiment E8: an endpoint that is down for 10 minutes then recovers, at 50 messages per second to it. Report requests sent to the dead endpoint with and without the breaker, and time to full recovery after it heals.

## P3.2 Go delivery worker and benchmark

**Learn first (owner, about 10 hours before writing the worker):** the Go tour; goroutines and channels; `context` for cancellation and timeouts; error handling; `net/http` client and transport tuning; `database/sql` with pgx; table-driven tests; the race detector.

**Decide (ADR-011):** Kafka client library (a pure-Go client versus the librdkafka binding) and the concurrency model (one goroutine per partition, or a worker pool with per-partition ordering preserved for offset commits).

**Rules:**
- The Go worker implements `contracts.md` sections 5–8 exactly and is a drop-in replacement: same topic, same consumer group, same database protocol, same headers and signature, same SSRF rules, same metrics names.
- Code lives in `worker-go/` with its own Dockerfile. The Compose file and Helm chart select the implementation with one value (`worker.implementation: python | go`).
- **The black-box suite is the judge.** Before the owner starts, a model refactors the R tests, the C1/C2 retry-cycle tests, and the invariant script so they run against whichever implementation is selected, and adds black-box versions of W1–W13 that drive the worker through Kafka and observe only the database and the receiver. CI runs the suite twice, once per implementation.
- Signature compatibility test: a request produced by the Go worker passes Python's `verify_signature`.
- SSRF: the Go implementation must use a custom dialer `Control` function (or equivalent) that vets the address actually being connected to; the rebinding test from phase 1 is ported.

**Benchmark (`docs/experiments/010-python-vs-go-worker.md`):**
- Same machine, same commit of everything else, same sink, one worker process each, then each scaled to its best configuration.
- Measure: maximum sustained throughput (as in E2), first-attempt delay p50/p99 at 50% of that maximum, CPU seconds and peak memory per 100,000 deliveries.
- Three runs each; report the median and the spread. State the hypothesis first. Explain the result: where the time goes in each implementation (a profile for each: `py-spy` and `pprof`).
- Report honestly if the difference is small because the bottleneck is PostgreSQL or the network rather than the language; that is a finding, not a failure.

**Accept:** black-box suite green for both implementations in CI; `go vet` and `go test -race` clean; benchmark report merged; production switched to the Go worker for one week with zero stuck deliveries, then the decision of which to keep recorded in ADR-012.

## P3.3 Three-broker Kafka failure experiment

**Learn first:** replication factor; in-sync replicas; `min.insync.replicas`; what `acks=all` actually guarantees; leader election; unclean leader election; why 3 brokers with `min.insync.replicas=2` tolerates one failure.

**Build:** a local kind cluster with three Kafka brokers; `deliveries` with replication factor 3 and `min.insync.replicas=2`.

**Experiments (`docs/experiments/011-kafka-broker-failure.md`):**
- E9a: at 200 messages per second, kill the broker leading the busiest partition. Pass: every acknowledged record is consumed; invariant script passes; report the pause length seen by producers and consumers.
- E9b: kill two of three brokers. Expected: producers with `acks=all` are refused (not enough in-sync replicas); the outbox accumulates; the API keeps accepting; after one broker returns, the backlog drains. Pass: no loss; report drain time.
- E9c: repeat E9a with `acks=1` on a scratch topic and try to demonstrate a lost acknowledged write. Report whether loss was observed and explain why or why not.

**Accept:** report merged; the owner can explain each result at a whiteboard.

## P3.4 Interview packaging

- `README.md`: final architecture diagram; a results table (throughput, latencies, recovery times, duplicate rate) with links to each experiment; "design decisions" section linking the ADRs; "known limitations" section.
- `docs/interview/system-design-walkthrough.md`: a 5-minute and a 20-minute version of "design a webhook delivery system", told from this project, including what was tried and abandoned.
- `docs/interview/questions.md`: the owner's own written answers to every self-check question from all three phases, plus the 15 hardest follow-ups (for example: "why not exactly-once?", "what breaks at 100× the load?", "what would you do differently?").
- `docs/interview/stories.md`: four STAR-format stories drawn from real events in the project: a bug found by an experiment, a wrong hypothesis, a trade-off decision, a production incident.
- One technical blog post (1,200–1,800 words) on the most interesting finding, published on the owner's own site or a developer blogging platform.
- A 3-minute demo video: send an event, break the receiver, watch retries, heal it, show the dashboard and Grafana.
- Final resume bullets in `docs/resume-notes.md`: three bullets, each with a number that an experiment report backs up.

**Accept:** a mock system-design interview (with a model as interviewer, 45 minutes, no notes) in which the owner covers delivery guarantees, the outbox, retries, fairness, and scaling limits without being led.

---

*Cut line.*

## P3.5 Token-bucket rate limiting

Replace the fixed window with a token bucket in a Redis Lua script (atomic refill and take). Add an optional per-endpoint delivery rate limit (`endpoints.rate_limit_per_second`); a limited delivery is rescheduled without consuming an attempt. **Accept:** a test showing the fixed window's boundary burst (2× the limit across a window edge) and its absence with the token bucket; ADR-013.

## P3.6 Secret encryption and rotation

Encrypt `endpoints.secret` at rest with an application key from the environment (envelope encryption; key ID stored with the ciphertext). Add `POST /v1/endpoints/{id}/rotate-secret` with a 24-hour overlap during which requests carry two signatures. **Accept:** migration test encrypting existing rows; a receiver holding the old secret still verifies during the overlap and fails after it.

## Phase 3 definition of done

1. P3.1–P3.4 accepted.
2. Experiment reports 009–011 merged; ADR-009 to ADR-012 written by the owner.
3. Production healthy for 14 consecutive days: zero stuck deliveries, SLOs met or the miss explained.
4. The mock interview passed.
