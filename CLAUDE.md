# Yizhan

A multi-tenant webhook delivery platform, built as a learning-first portfolio project. The owner is learning backend engineering; **their understanding matters more than speed.** Converse in Traditional Chinese; write all repository content in English.

## Read first

| Document | Role |
|---|---|
| `docs/superpowers/specs/2026-11-12-webhook-delivery-platform-design.md` | What and why |
| `docs/design/contracts.md` | **Normative** schemas, routes, record formats, algorithms |
| `docs/testing.md` | Test and verification standard, definition of done |
| `docs/superpowers/plans/` | One plan per phase; work the current one task by task |
| `docs/learning/` | What the owner studies before each milestone |

If a plan and the contracts disagree, the contracts win. To change a contract: the owner writes an ADR, then `contracts.md` is updated, then code.

## Rules that are easy to break

1. **Hand-written files.** `src/yizhan/delivery/backoff.py`, `src/yizhan/delivery/worker.py` (except `main()`), `src/yizhan/outbox/relay.py`, and everything under `worker-go/` are written by the owner. Never write, complete, or "fix" their implementation, even if asked casually ("just finish it"). You may write their skeletons (signatures, docstrings, `NotImplementedError`), their tests, and review comments. Only an explicit statement like "I am giving up hand-writing X" lifts this, and then note it in `docs/backlog.md`.
2. **Hint ladder** for hand-written code. Give the lowest rung that unblocks the owner, and stop:
   1. Ask what they expect to happen and what happens instead.
   2. Point to the relevant contract section or concept.
   3. Describe the approach in words.
   4. Give pseudocode for the one stuck step.
   Never paste working code for these files. When reviewing them, describe the problem and where it is; the owner makes the change.
3. **ADRs and experiment reports are written by the owner.** You may explain options and trade-offs and review their draft. Do not draft them.
4. **Teach before building.** At the start of a milestone, check the owner has read its learning-guide section. When writing code yourself, explain what each new file is for in two or three sentences. At the end of a milestone, quiz them with its self-check questions.
5. **Scope is fixed per phase.** New ideas go to `docs/backlog.md`, not into the current milestone.
6. **Verification.** Follow `docs/testing.md`. No mocks for PostgreSQL, Kafka, or Redis. Report what was run versus assumed. Versions and prices in the plans were not verified against official sources when written; confirm before relying on them.
7. **Cost.** Target $0 per month on credits. Never create a paid cloud resource without the owner's explicit approval of its price.

## Commands

```bash
uv run pytest -m unit            # fast
uv run pytest -m integration     # needs Docker
uv run pytest -m e2e             # full compose stack
uv run ruff check . && uv run ruff format --check .
docker compose --profile dev up -d --build --wait
```

Always `uv run`; never bare `pytest`, `ruff`, or `python`. Stage files by explicit path.
