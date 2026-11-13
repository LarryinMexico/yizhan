# Phase 1 Learning Guide

Read the section for a milestone **before** its tasks start. Each section has four parts:

- **Concepts** — what to understand, with the one idea that matters most.
- **Hands-on** — short exercises on the real stack. Do them; do not just read.
- **Decisions (ADRs)** — the question, the options, and what to weigh. The choice and the writing are yours. Use `docs/adr/0000-template.md`.
- **Self-check** — interview-style questions. A milestone is not finished until you can answer each one aloud, without notes, in about a minute.

For primary sources, prefer the official documentation of each tool (Docker, PostgreSQL, FastAPI, Apache Kafka, Redis) and *Designing Data-Intensive Applications* by Martin Kleppmann (chapters 7, 8, and 11 are the most relevant to this project).

---

## M0 Skeleton

### Concepts
- **Image versus container.** An image is a read-only template built from a Dockerfile; a container is a running instance of one. Many containers can run from one image.
- **Layers and build cache.** Each Dockerfile instruction makes a layer. Order instructions from least to most frequently changing (dependencies before source code) so rebuilds are fast.
- **Compose.** Declares several containers, their network, and their volumes in one file. Containers reach each other by service name (`kafka:9092`), not `localhost`.
- **Volumes.** A container's filesystem is thrown away when it is removed. Data that must survive lives in a volume.
- **Health checks and start order.** "Started" is not "ready". `depends_on` with a health condition waits for readiness.
- **Liveness versus readiness.** `/healthz`: the process is alive. `/readyz`: it can do useful work right now. They answer different questions and are used differently by an orchestrator.
- **Continuous integration.** Every push is built and tested automatically on a clean machine, so "works on my machine" is caught early.
- **Twelve-factor configuration.** Configuration comes from the environment, not from code.

**The one idea:** a container is just a process with its own isolated view of the filesystem and network. Everything else follows from that.

### Hands-on
1. `docker compose up -d`, then `docker compose ps`. Which containers are "healthy"?
2. `docker compose exec postgres psql -U yizhan -c 'select version()'`.
3. `docker compose exec api sh`, then try to reach `kafka:9092` and `localhost:9092`. Explain the difference.
4. Why does Kafka advertise two listeners (`kafka:9092` and `localhost:29092`)? Connect from your laptop and from inside a container to see.
5. `docker compose down`, then `up` again: is the data still there? Now `docker compose down -v`. Explain.
6. Change one line of Python and rebuild. Which layers were reused? Now add a dependency and rebuild.
7. Break a test on purpose, push to a branch, and watch CI fail.

### Self-check
1. What is the difference between an image and a container?
2. Why are dependencies copied and installed before the source code in a Dockerfile?
3. Inside the `api` container, why does `localhost:5432` not reach PostgreSQL?
4. What is the difference between liveness and readiness, and what would go wrong if `/healthz` checked the database?
5. What does CI protect you from?

---

## M1 API and database

### Concepts
- **HTTP semantics.** Methods (GET is safe, PUT and DELETE are idempotent, POST is neither by default); status code families; why `202 Accepted` is the honest answer for work that will happen later.
- **Idempotency.** An operation that has the same effect whether performed once or many times. Clients retry on timeouts, so a POST that creates something needs an idempotency key to be safe.
- **Transactions and ACID.** A transaction is all-or-nothing. Isolation decides what concurrent transactions can see of each other. Know what "read committed" (PostgreSQL's default) allows.
- **Constraints as the last line of defence.** Application checks can race; a unique constraint cannot. `INSERT ... ON CONFLICT` turns a race into a clean outcome.
- **Indexes.** A B-tree index makes lookups and ordered scans fast and makes writes slower. A partial index covers only the rows that match a condition.
- **Keyset versus offset pagination.** `OFFSET 100000` reads and discards 100,000 rows and shifts when rows are inserted; keyset pagination ("rows after this one") stays fast and stable.
- **Storing secrets.** Hash what you only need to verify (API keys); you cannot hash what you need to use (signing secrets).
- **Dependency injection in FastAPI.** `Depends` builds per-request objects (a database session, the authenticated application) and makes them replaceable in tests.
- **Schema migrations.** The schema changes through versioned, ordered scripts, never by hand.

**The one idea:** let the database enforce what must be true. Application code that checks and then writes has a gap between the check and the write.

### Hands-on
1. In `psql`, open two sessions. In both: `BEGIN; INSERT` a message with the same application and idempotency key. Commit the first. What happens to the second, and when?
2. Insert 100,000 deliveries with `generate_series`. Run `EXPLAIN ANALYZE` on the list query with `OFFSET 90000 LIMIT 50` and on the keyset version. Compare.
3. Drop the `deliveries_due` index, `EXPLAIN ANALYZE` the scheduler's query, recreate it, and compare.
4. Send the same `POST /v1/messages` twice with one `Idempotency-Key` using `curl -i`. Then send 20 at once (`xargs -P 20`). How many rows?
5. Read the generated migration file. Could you have written it by hand?

### Decisions
**ADR-001 Data model.** Why are `messages`, `deliveries`, and `attempts` three tables rather than one or two? What would a single `events` table with a JSON array of attempts cost you? Why is `application_id` duplicated onto `deliveries`? Why UUIDs rather than auto-increment integers (think about what an integer ID leaks, and about index locality)?

**ADR-002 Kafka or PostgreSQL as the queue.** PostgreSQL can be a queue (`FOR UPDATE SKIP LOCKED` — you use exactly this in the scheduler). Weigh: operational cost of another system; throughput ceiling; what happens to the database under a backlog; replay and retention; consumer groups and partitions for scaling workers; ordering per key. Be honest in the ADR: at this project's real traffic PostgreSQL alone would work, and part of the reason for Kafka is learning. State at what scale or under what requirement Kafka becomes the right engineering choice. An interviewer will respect that answer far more than a pretence.

### Self-check
1. A client sends `POST /v1/messages`, the request times out, and it retries. Walk through exactly what prevents a duplicate message.
2. Two requests with the same idempotency key arrive at the same instant on two API processes. What happens in the database?
3. Why is the API key stored as a hash, but the endpoint's signing secret is not?
4. Why does the API return 404 rather than 403 for another application's endpoint?
5. Why is keyset pagination better than offset here? When is offset acceptable?
6. What is a partial index and why does `deliveries_due` use one?
7. What does "read committed" let a transaction see? Give one anomaly it does not prevent.

---

## M2 Kafka and the delivery worker

### Concepts
- **The log.** A Kafka topic partition is an append-only, ordered log. Consumers do not remove records; they remember a position (the offset).
- **Partitions and keys.** Records with the same key go to the same partition, so they are ordered relative to each other. There is no ordering across partitions. The number of partitions caps how many consumers in a group can work in parallel.
- **Consumer groups and rebalancing.** Each partition is assigned to exactly one consumer in a group. When consumers join or leave, partitions are reassigned.
- **Offsets and commits.** Committing an offset says "everything before this is done". Where you commit relative to your work decides your guarantee:
  - commit **before** processing → at-most-once (a crash loses the record);
  - commit **after** processing → at-least-once (a crash repeats the record).
- **At-least-once plus idempotent processing.** Since repeats will happen, processing must be safe to repeat. Here: the unique `(delivery_id, attempt_no)` constraint and the `attempt_count >= attempt_no` check.
- **Why "exactly-once delivery" to an external HTTP endpoint is impossible.** You cannot atomically send an HTTP request and record that you sent it. You can crash between the two. The best achievable is at-least-once with a message ID the receiver deduplicates on.
- **Producer acknowledgements.** `acks=all` waits for the in-sync replicas. The idempotent producer prevents duplicates caused by the producer's own retries.
- **HMAC signatures.** A shared secret proves the sender and protects the body from tampering; the signed timestamp limits replay. Compare in constant time.
- **SSRF.** A service that fetches user-supplied URLs can be pointed at internal addresses, including the cloud metadata service. Vetting must apply to the address actually connected to.
- **asyncio.** One thread, many tasks; a task gives up control only at `await`. Blocking calls stall everything. Use a semaphore to bound concurrency.

**The one idea:** you choose your delivery guarantee by choosing where the commit goes relative to the side effect. There is no setting that gives exactly-once for external effects.

### Hands-on
1. With the stack up, run the console consumer on `deliveries` (`kafka-console-consumer.sh --from-beginning --property print.key=true --property print.partition=true`) and send some messages. Do messages for the same endpoint share a partition?
2. `kafka-consumer-groups.sh --describe --group delivery-workers`. Find current offset, log-end offset, and lag. Stop the worker, send 50 messages, and look again.
3. Start two workers (`docker compose up -d --scale worker=2`). Describe the group again: how were partitions split? Stop one and watch the rebalance.
4. Write a ten-line consumer that commits **before** processing, kill it mid-batch, and observe the loss. Then commit after, kill it, and observe the repeat. This is the best 30 minutes you will spend in this milestone.
5. Sign a body by hand in a Python shell with `hmac` and compare with what the receiver got.
6. In secure mode, create an endpoint for a host name that resolves to `127.0.0.1` and watch the attempt be blocked.

### Hand-writing `backoff.py` and `worker.py`
- Write `backoff.py` first. It is a small pure function and warms you up.
- For the worker, follow the order suggested in Task 2.5. Make one test pass at a time.
- Before writing any code for `handle_record`, write the steps as comments in your own words from contracts section 6, and mark where the transaction begins and ends.
- For every line that touches the database or the network, ask: "if the process dies right after this line, what state is left, and what happens when the record is delivered again?" Write your answers in ADR-003.
- When stuck, ask the model for a **hint**, not the answer. The hint ladder is in `CLAUDE.md`.

### Decisions
**ADR-003 When to commit the offset.** Options: auto-commit on a timer; commit after each record; commit after each batch. Weigh the guarantee each gives, throughput, and how much is repeated after a crash. Also record: why the attempt row and the delivery update are in one transaction; what the receiver experiences if the worker crashes after the HTTP request but before the database write; and why the payload is not in the Kafka record. Include the output of `scripts/kill_worker_test.sh` as evidence.

### Self-check
1. Explain at-most-once, at-least-once, and exactly-once. Which does Yizhan provide and why not the others?
2. The worker sends the HTTP request, the receiver processes it, and the worker crashes before writing to the database. What happens next, step by step? What must the receiver do?
3. The worker writes the attempt to the database and crashes before committing the Kafka offset. What happens next, and what prevents a second attempt row?
4. Why is the endpoint ID the partition key? What would change with the message ID as key, or no key?
5. You have 6 partitions and start 10 workers. How many do work? How do you scale beyond that?
6. What is a consumer rebalance, and what can go wrong during one?
7. Why must the signature cover the timestamp, and why compare signatures in constant time?
8. What is SSRF? Why is "resolve the host, check the IP, then let the HTTP library connect" not enough?
9. In asyncio, what happens if you call a blocking function inside a coroutine?
10. Two workers process the same record at the same time. Walk through how the system ends in a correct state.

---

## M3 Retries, limits, and the page

### Concepts
- **Transient versus permanent failures.** Retrying helps with the first and wastes effort on the second; from outside you often cannot tell which, so you bound the retries.
- **Exponential backoff and jitter.** Growing delays give a struggling receiver room to recover. Jitter spreads retries out; without it, everything that failed together retries together (the thundering herd).
- **Dead-letter queue.** Where work goes when retrying is no longer sensible, so it is visible and can be replayed rather than lost or retried forever.
- **Delayed work in Kafka.** Kafka delivers in log order and has no "deliver at time T". Common answers: keep the schedule in a database and re-publish when due (this project); a ladder of retry topics, one per delay; a separate delay service.
- **`SELECT ... FOR UPDATE SKIP LOCKED`.** Lets several workers take different rows from the same table without blocking or double-taking. The basis of most database-backed queues.
- **Rate limiting algorithms.** Fixed window (simple; allows a burst of 2× across a window edge), sliding window, token bucket (allows controlled bursts), leaky bucket.
- **Fail open versus fail closed.** When the limiter's own dependency is down, do you allow or deny? It depends on what the limit protects.
- **Cross-site scripting.** Never put untrusted text into a page as HTML. Webhook payloads and response bodies are untrusted.

**The one idea:** retries are a load multiplier. Every retry policy needs a bound, a spread, and somewhere for the failures to go.

### Hands-on
1. Start the stack with `RETRY_SCHEDULE_SECONDS=2,4,8` and a receiver that always fails. Watch `next_attempt_at` change in `psql` after each attempt.
2. Open two `psql` sessions. In each, `BEGIN;` and run the scheduler's `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 5`. Compare the rows each got. Repeat without `SKIP LOCKED`.
3. In `redis-cli`, `MONITOR` while you send messages, and watch the limiter's `INCR` and `EXPIRE`.
4. Set the limit to 10 per minute. Send 10 requests in the last second of a minute and 10 in the first second of the next. How many succeeded in 2 seconds? This is the fixed-window weakness.
5. Stop Redis and send a message. What happened, and is that what you want?
6. Read the `deliveries.dlq` topic after a delivery exhausts its attempts.

### Decisions
**ADR-004 Where retry timing lives.** Options: (a) `next_attempt_at` in PostgreSQL with a scheduler (chosen in the draft design — confirm or overturn it); (b) a ladder of retry topics (`retry-10s`, `retry-1m`, ...) whose consumers wait until each record is due; (c) sleeping in the worker. Weigh: precision of timing; what happens to a partition while a consumer waits; operational complexity; database load when there are millions of pending retries; how each behaves when the scheduler or a consumer crashes. Also record the fail-open decision for the rate limiter and its reasoning.

### Self-check
1. Why add jitter? Describe the failure it prevents with a concrete scenario.
2. Why does Yizhan stop after 8 attempts instead of retrying forever? What are the costs on each side?
3. Kafka has no delayed delivery. Describe two ways to implement retry delays and one drawback of each.
4. What does `SKIP LOCKED` do? What happens with two schedulers and no `SKIP LOCKED`? With plain `FOR UPDATE`?
5. The scheduler publishes a batch to Kafka and crashes before committing the database transaction. What happens, and why is it safe?
6. Explain the fixed-window burst problem and how a token bucket avoids it.
7. The rate limiter fails open when Redis is down. Argue for and against.
8. Why does replay create a new delivery row instead of resetting the old one?

---

## M4 Deployment

### Concepts
- **What a VM is and what you are responsible for on it:** the operating system, patches, the firewall, disk space, backups.
- **Security groups.** A stateful firewall in front of the instance. Open only what is needed.
- **TLS and certificates.** What a certificate proves; how ACME issues one automatically by proving you control the host name; why Caddy can do this without configuration.
- **Reverse proxy.** One public entry point that terminates TLS and forwards to internal services, which are not exposed.
- **Trusting `X-Forwarded-For`.** Anyone can send that header. Only believe it when the direct peer is your own proxy.
- **Secrets on a server.** In an environment file readable only by the deploy user, never in the repository, never in logs.
- **Backups.** A backup that has never been restored is a hope, not a backup.
- **Cloud cost control.** Budgets and alerts first; public IPv4 addresses, disks, and stopped-but-not-terminated resources all cost money.
- **Zero-downtime versus simple deploys.** A Compose `up -d` restarts containers and drops requests for a moment. That is acceptable for v0 and is one concrete reason for Kubernetes rolling updates in phase 2.

**The one idea:** production is the same software plus everything that can go wrong around it. Most of the work is the "around it".

### Hands-on
1. Follow the runbook yourself in the AWS console rather than asking a model to script it. Phase 2 automates it with Terraform, and you will understand what Terraform is doing because you did it by hand first.
2. From your laptop, try to connect to the instance's PostgreSQL and Kafka ports. They must be unreachable.
3. `curl -v https://<host>/healthz` and read the TLS handshake lines. Who issued the certificate, and when does it expire?
4. Restore last night's `pg_dump` into a scratch database and count the rows.
5. `docker stats` on the instance: which container uses the most memory? How close to the limit is the machine?
6. Reboot the instance and time how long until the smoke test passes.
7. Check the AWS billing page the day after launch. Is the daily cost what you predicted?

### Self-check
1. Trace a request from a customer's `curl` to the API process: DNS, TCP, TLS, Caddy, container network.
2. Why are the PostgreSQL and Kafka ports not published in production?
3. How does Caddy obtain a certificate without you doing anything?
4. Why is trusting `X-Forwarded-For` from anyone a security bug for the signup rate limit?
5. What happens to in-flight requests and in-flight deliveries during a deploy today? Which of those is a correctness problem and which is only an availability problem? (Hint: think about what the worker does on SIGTERM and what the system guarantees if it is killed anyway.)
6. The disk fills up. What breaks first, and how would you find out before users do?
7. What is this deployment's single biggest risk, and what does phase 2 do about it?
