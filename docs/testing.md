# Yizhan Testing and Verification Standard

This standard applies to every phase. A task is not done until the checks named for it here and in its plan have been **run** and their output seen.

## 1. Rules

1. **No mocks for infrastructure.** PostgreSQL, Kafka, and Redis are always real, started by Testcontainers. A passing test that stubs the thing under test is not evidence.
2. **Every acceptance criterion maps to a named test or a named command.** Plans list them by ID (for example `W6`); test function names include the ID, for example `test_w6_duplicate_record_is_skipped`.
3. **Reproduce before fixing.** A bug is first reproduced through a real entry point (HTTP call, `docker compose`, script) and the command is kept in the commit message or an experiment note.
4. **Say what was run.** When reporting, separate "ran and passed", "ran and failed", and "not run". Never report a check as passing if it was not executed.
5. **Time is injected.** Code that depends on the clock or randomness takes `now` / `rng` as a parameter so tests are deterministic. Tests never `sleep` to wait for real backoff; they shorten `RETRY_SCHEDULE_SECONDS`.
6. **Tests assert invariants, not only examples.** The four invariants in `docs/design/contracts.md` section 5 each have at least one test.
7. **A flaky test is a bug.** Do not retry or skip it; find the race.

## 2. Layers

| Layer | Location | Uses | Runs |
|---|---|---|---|
| Unit | `tests/unit/` | Pure functions only: backoff, signing, URL rules, record encoding | On every save; under 5 seconds total |
| Integration | `tests/integration/` | Real PostgreSQL, Kafka, Redis via Testcontainers; the API through `httpx.ASGITransport`; a local mock receiver | In CI on every push |
| End-to-end | `tests/e2e/` | The full Docker Compose stack with all processes as containers | In CI on every push to `main`, and before each milestone is closed |
| Experiments | `docs/experiments/` | The running system under load or with injected faults | By hand, written up with numbers (phase 2 onward) |

Markers: `@pytest.mark.unit`, `@pytest.mark.integration`, `@pytest.mark.e2e`.

## 3. Commands

```bash
uv run ruff check .                 # lint
uv run ruff format --check .        # formatting
uv run pytest -m unit               # unit
uv run pytest -m integration        # integration (needs Docker running)
uv run pytest -m e2e                # end-to-end (builds and starts the compose stack)
uv run pytest                       # everything
```

Never run bare `pytest`, `ruff`, or `python file.py`; always go through `uv run`.

## 4. Shared test support

- `tests/conftest.py`: session-scoped Testcontainers for PostgreSQL, Kafka, and Redis; a fixture that runs Alembic migrations once; a per-test fixture that truncates all tables and deletes Redis keys.
- `tests/support/receiver.py`: the mock receiver. A small ASGI app started on a random local port with:
  - `POST /hook/{name}` — the webhook target. Behaviour is set per `name`.
  - `POST /_control/{name}` — body `{"mode": "ok" | "status" | "timeout" | "slow" | "close", "status": int, "delay": float}`.
  - `GET /_received/{name}` — list of received requests with headers and body, for assertions.
- `tests/support/factories.py`: helpers that create an application, key, endpoint, message, and delivery directly in the database.
- Tests that use the mock receiver set `ALLOW_INSECURE_ENDPOINTS=true`. SSRF rules are tested separately with a stub resolver.

## 5. Definition of done for any task

1. Its listed tests exist, were seen to fail before the implementation, and pass after.
2. `uv run ruff check .` and `uv run ruff format --check .` pass.
3. The full suite for the layers it touches passes.
4. Its "Verify" commands were run against the real stack and the output matches.
5. Committed with only the files that belong to the task (`git status -sb` checked first; no `git add -A`).
6. For hand-written files: the owner wrote the implementation, and can explain each branch without notes.

## 6. Definition of done for a milestone

1. Every task is done.
2. The milestone's end-to-end verification was run on a clean checkout: `docker compose down -v && docker compose up -d --build --wait`.
3. CI on `main` is green.
4. The milestone's ADRs are written by the owner and merged.
5. The owner answered the milestone's self-check questions in `docs/learning/` out loud or in writing.

## 7. Experiment protocol (phase 2 onward)

Each experiment is one file, `docs/experiments/NNN-short-title.md`, from `docs/experiments/0000-template.md`. It must contain: the question, the hypothesis written **before** running, the environment (machine size, versions, commit hash), exact commands, raw numbers, the conclusion, and what changed in the system as a result. An experiment whose hypothesis was wrong is still a valid, valuable result and is kept.

Correctness checks used by failure experiments (run after the system settles):

```sql
-- 1. No delivery is left non-terminal (expect 0)
SELECT count(*) FROM deliveries WHERE status = 'pending';
-- 2. attempt_count matches attempt rows (expect 0)
SELECT count(*) FROM deliveries d
WHERE attempt_count <> (SELECT count(*) FROM attempts a WHERE a.delivery_id = d.id);
-- 3. Stuck deliveries from the dual-write gap (expect 0 after the outbox exists)
SELECT count(*) FROM deliveries
WHERE status = 'pending' AND next_attempt_at IS NULL AND updated_at < now() - interval '5 minutes';
```

Plus, from the mock receiver: every accepted message ID was received at least once; the duplicate rate (received count ÷ distinct message IDs) is reported, not hidden.
