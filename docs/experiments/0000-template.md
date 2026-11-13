# Experiment NNN: Title as a question ("Does killing the worker lose deliveries?")

- Date: YYYY-MM-DD
- Author: (the owner runs and writes every experiment)
- Commit: `<git hash>`
- Environment: machine (CPU, memory), where it ran (laptop Compose / kind / production VM), versions of Kafka and PostgreSQL

## Question

One sentence.

## Hypothesis

What you expect to happen and why, with a number where possible. **Write this before running anything and do not edit it afterwards.**

## Method

Exact commands, in order, so someone else can repeat it. Load level, duration, fault injected and when, configuration that differs from the default.

## Results

Raw numbers first (a table), then graphs or screenshots. Include the output of `scripts/check_invariants.sh` for failure experiments. Report the duplicate rate; do not hide it.

## Analysis

Was the hypothesis right? If not, why not — what did you misunderstand? What limited the result (CPU, database, network, the test tool itself)?

## Outcome

What changed in the system or the plan because of this experiment (a fix, an ADR, a backlog item, nothing). Link the commit or ADR.
