# Yizhan Technical Contracts

This document is **normative**. Where it and the design spec disagree, this document wins. Implementers follow it exactly; changing it requires an ADR.

Spec: `docs/superpowers/specs/2026-11-12-webhook-delivery-platform-design.md`

## 1. Runtime and configuration

- Python `>=3.13`, managed with `uv`. Package name `yizhan`, source in `src/yizhan/`.
- All configuration comes from environment variables, read once by `yizhan.config.Settings` (pydantic-settings).

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | none (required) | `postgresql+asyncpg://...` |
| `KAFKA_BOOTSTRAP_SERVERS` | none (required) | e.g. `kafka:9092` |
| `REDIS_URL` | none (required) | e.g. `redis://redis:6379/0` |
| `ALLOW_INSECURE_ENDPOINTS` | `false` | If true, endpoints may use `http`, any port, and private addresses. Local and test only. |
| `DELIVERY_TIMEOUT_SECONDS` | `10` | Total time allowed for one attempt. |
| `RETRY_SCHEDULE_SECONDS` | `10,60,300,1800,7200,21600,43200` | Delay after the Nth failed attempt. |
| `RETRY_JITTER` | `0.2` | Each delay is multiplied by a uniform factor in `[1-j, 1+j]`. |
| `WORKER_BATCH_SIZE` | `50` | Max records fetched per poll. |
| `WORKER_CONCURRENCY` | `20` | Max attempts in flight at once. |
| `SCHEDULER_INTERVAL_SECONDS` | `1` | Sleep between scheduler passes. |
| `SCHEDULER_BATCH_SIZE` | `100` | Max deliveries re-enqueued per pass. |
| `RATE_LIMIT_MESSAGES_PER_MINUTE` | `600` | Per application, on `POST /v1/messages`. |
| `RATE_LIMIT_SIGNUPS_PER_HOUR` | `5` | Per client IP, on `POST /v1/applications`. |
| `MAX_PAYLOAD_BYTES` | `262144` | Request body limit (256 KB). |

`MAX_ATTEMPTS` is derived: `len(RETRY_SCHEDULE_SECONDS) + 1` (8 by default).

## 2. Database schema (PostgreSQL)

All primary keys are `uuid` generated in the application with `uuid.uuid4()`. All timestamps are `timestamptz` in UTC.

```sql
CREATE TABLE applications (
  id                  uuid PRIMARY KEY,
  name                text NOT NULL CHECK (char_length(name) BETWEEN 1 AND 100),
  endpoint_limit      integer NOT NULL DEFAULT 10,
  daily_message_limit integer NOT NULL DEFAULT 10000,
  created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
  id             uuid PRIMARY KEY,
  application_id uuid NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
  key_hash       text NOT NULL UNIQUE,   -- hex SHA-256 of the full key
  prefix         text NOT NULL,          -- first 12 characters, for display
  created_at     timestamptz NOT NULL DEFAULT now(),
  revoked_at     timestamptz
);

CREATE TABLE endpoints (
  id             uuid PRIMARY KEY,
  application_id uuid NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
  url            text NOT NULL,
  description    text NOT NULL DEFAULT '',
  secret         text NOT NULL,          -- 'whsec_' + base64 of 32 random bytes
  event_types    text[] NOT NULL DEFAULT '{}',  -- empty = all event types
  disabled       boolean NOT NULL DEFAULT false,
  created_at     timestamptz NOT NULL DEFAULT now(),
  deleted_at     timestamptz             -- soft delete
);
CREATE INDEX endpoints_app ON endpoints (application_id) WHERE deleted_at IS NULL;

CREATE TABLE messages (
  id              uuid PRIMARY KEY,
  application_id  uuid NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
  event_type      text NOT NULL,
  payload         jsonb NOT NULL,
  idempotency_key text,
  created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX messages_idempotency
  ON messages (application_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX messages_app_created ON messages (application_id, created_at DESC, id DESC);

CREATE TABLE deliveries (
  id              uuid PRIMARY KEY,
  message_id      uuid NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  endpoint_id     uuid NOT NULL REFERENCES endpoints(id),
  application_id  uuid NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
  status          text NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'succeeded', 'failed')),
  attempt_count   integer NOT NULL DEFAULT 0,   -- number of recorded attempts
  next_attempt_at timestamptz,                  -- see section 5
  replay_of       uuid REFERENCES deliveries(id),
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX deliveries_app_created ON deliveries (application_id, created_at DESC, id DESC);
CREATE INDEX deliveries_message ON deliveries (message_id);
CREATE INDEX deliveries_due ON deliveries (next_attempt_at)
  WHERE status = 'pending' AND next_attempt_at IS NOT NULL;

CREATE TABLE attempts (
  id            uuid PRIMARY KEY,
  delivery_id   uuid NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
  attempt_no    integer NOT NULL CHECK (attempt_no >= 1),
  status_code   integer,            -- NULL when no HTTP response was received
  latency_ms    integer NOT NULL,
  error         text,               -- NULL when a response was received; see section 6
  response_body text,               -- first 1024 characters of the response body
  created_at    timestamptz NOT NULL DEFAULT now(),
  UNIQUE (delivery_id, attempt_no)
);
```

Notes:
- Endpoint secrets are stored in plaintext because the worker needs them to sign. Encrypting them at rest is out of scope for phase 1 and is listed as phase 2 hardening.
- API keys are random with 256 bits of entropy, so an unsalted SHA-256 hash is sufficient; a slow password hash is not needed and would slow every request.

## 3. HTTP API

Base path `/v1`. JSON in, JSON out. OpenAPI is served by FastAPI at `/docs`.

### Authentication

`Authorization: Bearer <api key>`. Key format: `yz_` followed by `secrets.token_urlsafe(32)`. The server hashes the presented key with SHA-256 and looks it up in `api_keys.key_hash` where `revoked_at IS NULL`.

### Errors

Every error response has this body, and no other shape:

```json
{"error": {"code": "not_found", "message": "Endpoint not found"}}
```

| HTTP | `code` | When |
|---|---|---|
| 401 | `unauthorized` | Missing, malformed, unknown, or revoked key |
| 404 | `not_found` | Resource does not exist or belongs to another application |
| 409 | `conflict` | Replay requested for a delivery that is still `pending` |
| 413 | `payload_too_large` | Body exceeds `MAX_PAYLOAD_BYTES` |
| 422 | `validation_error` | Invalid body, invalid URL, limit exceeded (`endpoint_limit`) |
| 429 | `rate_limited` | Rate or daily limit hit; includes `Retry-After` seconds header |
| 503 | `unavailable` | A required dependency is down |

A resource owned by another application is always reported as 404, never 403, so IDs cannot be probed.

### Routes

| Method and path | Auth | Behaviour |
|---|---|---|
| `GET /healthz` | no | Always `200 {"status":"ok"}` while the process runs. |
| `GET /readyz` | no | `200` when PostgreSQL, Kafka, and Redis all answer; otherwise `503` with a per-dependency map. |
| `POST /v1/applications` | no | Body `{"name": str}`. Creates an application and its first key. `201 {"id", "name", "api_key"}`. The key is shown only here. Rate limited per IP. |
| `POST /v1/endpoints` | yes | Body `{"url": str, "description"?: str, "event_types"?: [str]}`. Validates the URL (section 7). `201` with the endpoint including `secret`. The secret is shown only here. |
| `GET /v1/endpoints` | yes | Lists non-deleted endpoints, without secrets. |
| `GET /v1/endpoints/{id}` | yes | One endpoint, without secret. |
| `PATCH /v1/endpoints/{id}` | yes | Any of `url`, `description`, `event_types`, `disabled`. |
| `DELETE /v1/endpoints/{id}` | yes | Soft delete (`deleted_at = now()`). `204`. |
| `POST /v1/messages` | yes | See below. |
| `GET /v1/messages/{id}` | yes | The message and its deliveries. |
| `GET /v1/messages` | yes | Paginated list. |
| `GET /v1/deliveries` | yes | Paginated list; filters `status`, `endpoint_id`. |
| `GET /v1/deliveries/{id}` | yes | The delivery and all its attempts, ordered by `attempt_no`. |
| `POST /v1/deliveries/{id}/replay` | yes | See below. |
| `GET /dashboard` | no | Static page; the user pastes an API key, which stays in the browser's `sessionStorage`. |

**Pagination.** Query `limit` (1–100, default 50) and `before` (an item ID). Results are ordered by `(created_at DESC, id DESC)`; `before` returns items strictly after that item in this order (keyset pagination, not `OFFSET`). Response: `{"data": [...], "next_before": "<id>" | null}`.

### `POST /v1/messages`

Request:

```json
{"event_type": "invoice.paid", "payload": {"any": "json object"}}
```

- `event_type` matches `^[A-Za-z0-9_.\-]{1,128}$`. `payload` must be a JSON object.
- Optional header `Idempotency-Key`, 1–255 characters.

Behaviour, in one database transaction:

1. Insert the message. If `(application_id, idempotency_key)` already exists, return the **existing** message with status `200` and create nothing.
2. Select the application's endpoints where `deleted_at IS NULL AND disabled = false AND (event_types = '{}' OR event_type = ANY(event_types))`.
3. Insert one delivery per selected endpoint with `status = 'pending'`, `attempt_count = 0`, `next_attempt_at = NULL`.
4. Commit.

After commit, publish one Kafka record per delivery (section 4) with `attempt_no = 1`, and return `202`:

```json
{"id": "...", "event_type": "invoice.paid", "created_at": "...",
 "deliveries": [{"id": "...", "endpoint_id": "...", "status": "pending"}]}
```

Zero matching endpoints is valid: the message is stored with no deliveries.

**Known gap (phase 1, deliberate).** A crash between step 4 and the publish leaves a delivery `pending` with `next_attempt_at IS NULL` and no Kafka record: it is stuck forever. Phase 2 demonstrates this and replaces the publish with a transactional outbox. Detection query:

```sql
SELECT count(*) FROM deliveries
WHERE status = 'pending' AND next_attempt_at IS NULL
  AND updated_at < now() - interval '5 minutes';
```

### `POST /v1/deliveries/{id}/replay`

Allowed when the delivery is `failed` or `succeeded`; `409` when `pending`. Creates a **new** delivery for the same message and endpoint with `replay_of` set, then publishes it like a first attempt. `202` with the new delivery. History of the original is never modified.

## 4. Kafka contract

| Topic | Partitions | Key | Purpose |
|---|---|---|---|
| `deliveries` | 6 | endpoint ID (UTF-8 string) | Work for delivery workers |
| `deliveries.dlq` | 1 | delivery ID | Record of deliveries that exhausted attempts, and of malformed records |

Topics are created explicitly by an init step. Broker auto-creation is disabled. Replication factor is 1 in single-broker environments.

Record value on `deliveries` (UTF-8 JSON):

```json
{"delivery_id": "uuid", "attempt_no": 1, "enqueued_at": "2026-11-13T10:00:00Z"}
```

The payload is **not** in the record. PostgreSQL is the source of truth and the worker loads what it needs. This keeps records small and makes a duplicate or stale record harmless.

Record value on `deliveries.dlq`:

```json
{"delivery_id": "uuid", "message_id": "uuid", "endpoint_id": "uuid",
 "reason": "attempts_exhausted" | "malformed_record", "detail": "string", "at": "timestamp"}
```

Producer settings: `acks=all`, idempotence enabled. Consumer settings: group `delivery-workers`, auto-commit **disabled**, `auto_offset_reset=earliest`.

## 5. Delivery state machine

`next_attempt_at` has exactly this meaning:

- `NULL` while `pending`: a Kafka record for the next attempt has been published (the delivery is "in the queue").
- non-NULL while `pending`: the delivery is waiting; the scheduler publishes it when the time passes.
- always `NULL` once `succeeded` or `failed`.

```
create ──► pending (next_attempt_at NULL, record published)
              │ worker attempt
              ├── 2xx ───────────────────────────► succeeded
              ├── failure, attempt_no < MAX ─────► pending (next_attempt_at = now + delay)
              │                                        │ scheduler, when due
              │                                        └─► pending (NULL, record published)
              └── failure, attempt_no = MAX ─────► failed  (+ DLQ record)
```

Invariants (tests assert these):

1. `attempts` never has two rows with the same `(delivery_id, attempt_no)`.
2. `deliveries.attempt_count` equals the number of attempt rows for that delivery.
3. A delivery in `succeeded` or `failed` never changes again.
4. The Kafka offset of a record is committed only after its outcome is stored in PostgreSQL.

## 6. Delivery worker behaviour

For one record `(delivery_id, attempt_no)`:

1. Load the delivery with its message and endpoint.
   - Not found → skip (`SKIPPED_UNKNOWN`).
   - `status != 'pending'` → skip (`SKIPPED_NOT_PENDING`).
   - `attempt_count >= attempt_no` → this attempt is already recorded; skip (`SKIPPED_DUPLICATE`).
   - `attempt_no != attempt_count + 1` → invalid gap; log and skip (`SKIPPED_INVALID`).
   - Endpoint `disabled` or deleted → set `status = 'failed'`, no request, no attempt row (`ENDPOINT_GONE`).
2. Build the signed request (section 8) and send it with the safe HTTP client (section 7).
3. In **one transaction**: insert the attempt row, then update the delivery:
   - 2xx → `status = 'succeeded'`, `attempt_count = attempt_no`, `next_attempt_at = NULL` (`DELIVERED`).
   - otherwise, if `attempt_no >= MAX_ATTEMPTS` → `status = 'failed'`, `attempt_count = attempt_no`, `next_attempt_at = NULL` (`DEAD`).
   - otherwise → `attempt_count = attempt_no`, `next_attempt_at = now() + next_delay(attempt_no)` (`RETRY_SCHEDULED`).
   - If the insert violates the unique constraint, another worker recorded this attempt first: roll back and skip (`SKIPPED_DUPLICATE`).
   - Always set `updated_at = now()`.
4. On `DEAD`, publish a DLQ record after the transaction commits. This publish is best-effort; the database already holds the truth.
5. The run loop commits Kafka offsets only after every record in the fetched batch has finished steps 1–4.

Attempt outcome fields:

| Situation | `status_code` | `error` |
|---|---|---|
| Response received | the code | `NULL` |
| Total time exceeded | `NULL` | `timeout` |
| TCP connect failed or DNS failed | `NULL` | `connect_error` |
| TLS handshake or certificate failed | `NULL` | `tls_error` |
| Address rejected by SSRF rules | `NULL` | `blocked_address` |
| Any other client-side failure | `NULL` | `request_error` |

Only 200–299 is success. Redirects are **not** followed; a 3xx is a failure.

Failure handling in the run loop:

- Malformed record (not JSON, missing fields) → publish to DLQ with `reason = "malformed_record"`, then treat as done.
- PostgreSQL or Kafka unavailable → do **not** commit; let the exception end the process. The container restart policy brings it back and the records are redelivered.

## 7. Endpoint URL rules and SSRF protection

At creation and update time (`validate_endpoint_url`):

- Scheme must be `https`. Port must be 443 (explicit or default). No userinfo. Host must not be an IP literal.
- With `ALLOW_INSECURE_ENDPOINTS=true`, all of the above are relaxed.

At delivery time (every attempt):

- Resolve the host. If **any** resolved address fails `ipaddress.ip_address(a).is_global` (this covers private, loopback, link-local including `169.254.169.254`, multicast, reserved, and IPv4-mapped IPv6), the attempt fails with `blocked_address` and nothing is sent.
- The connection must go to an address that was vetted. Resolving once to check and again to connect is a DNS-rebinding hole and is not acceptable. Implement this in the transport or network-backend layer so the vetted address is the one connected to, while TLS still verifies the certificate against the original host name.
- Redirects are disabled.

## 8. Request format and signature

Body (exact bytes are produced once and both signed and sent):

```json
{"id": "<message id>", "type": "<event_type>", "created_at": "<RFC 3339>", "data": <payload>}
```

Serialised with `json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")`.

Headers:

| Header | Value |
|---|---|
| `Content-Type` | `application/json` |
| `User-Agent` | `Yizhan/<version>` |
| `Yizhan-Message-Id` | message ID (stable across retries; receivers deduplicate on it) |
| `Yizhan-Delivery-Id` | delivery ID |
| `Yizhan-Attempt` | attempt number |
| `Yizhan-Timestamp` | Unix seconds at send time |
| `Yizhan-Signature` | `v1=<hex>` |

Signature: `hex(HMAC_SHA256(key, message_id + "." + timestamp + "." + body))`, where `key` is the base64-decoded part of the secret after `whsec_`.

`yizhan.delivery.signing` exposes both sides:

```python
def build_request(*, message_id: UUID, event_type: str, created_at: datetime, payload: dict,
                  delivery_id: UUID, attempt_no: int, secret: str, now: datetime) -> SignedRequest: ...
    # SignedRequest: dataclass with headers: dict[str, str] and body: bytes

def verify_signature(*, secret: str, headers: Mapping[str, str], body: bytes,
                     now: datetime, tolerance_seconds: int = 300) -> bool: ...
    # constant-time comparison; False if the timestamp is outside the tolerance
```

## 9. Retry scheduler behaviour

One pass (`run_once`), in a single transaction:

```sql
SELECT id, endpoint_id, attempt_count FROM deliveries
WHERE status = 'pending' AND next_attempt_at <= now()
ORDER BY next_attempt_at
LIMIT :batch
FOR UPDATE SKIP LOCKED;
```

Then publish one record per row with `attempt_no = attempt_count + 1`, wait for the broker acknowledgements, set `next_attempt_at = NULL` for those rows, and commit. If publishing fails, roll back; the rows stay due.

A crash after publishing and before commit re-publishes the same records on the next pass. That is safe because of rule `attempt_count >= attempt_no → skip` in section 6. `SKIP LOCKED` lets several scheduler instances run without publishing a row twice in the normal case.

## 10. Backoff

```python
DEFAULT_SCHEDULE: tuple[float, ...] = (10, 60, 300, 1800, 7200, 21600, 43200)

def next_delay(attempt_no: int, *, schedule: Sequence[float] = DEFAULT_SCHEDULE,
               jitter: float = 0.2, rng: random.Random | None = None) -> timedelta:
    """Delay to wait after failed attempt number `attempt_no` (1-based).

    Base delay is schedule[attempt_no - 1]. The result is the base multiplied by a factor
    drawn uniformly from [1 - jitter, 1 + jitter]. Raises ValueError if attempt_no < 1 or
    attempt_no > len(schedule).
    """
```

## 11. Rate limits and quotas (Redis)

- `POST /v1/messages`: fixed window per application, `RATE_LIMIT_MESSAGES_PER_MINUTE`. Key `rl:msg:{application_id}:{unix_minute}`, `INCR` then `EXPIRE 120` on first increment.
- Daily quota per application: key `quota:msg:{application_id}:{yyyymmdd}` against `applications.daily_message_limit`.
- `POST /v1/applications`: fixed window per client IP, `RATE_LIMIT_SIGNUPS_PER_HOUR`. The client IP is the direct peer address, unless that peer is inside `TRUSTED_PROXY_CIDRS` (default empty; set to the Caddy container network in production), in which case it is the left-most address in `X-Forwarded-For`.
- Endpoint count per application is checked against `applications.endpoint_limit` in PostgreSQL.
- If Redis is unreachable the limiter **fails open** (allows the request) and logs a warning: availability of the core path matters more than strict limiting.

## 12. Ownership of files

| File | Written by |
|---|---|
| `src/yizhan/delivery/backoff.py` | **Owner, by hand** |
| `src/yizhan/delivery/worker.py` | **Owner, by hand** (models provide the skeleton, tests, and review) |
| `src/yizhan/outbox/relay.py` (phase 2) | **Owner, by hand** |
| everything else | Models, reviewed by the owner |
