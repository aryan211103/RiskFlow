# RiskFlow — Design Document

This document explains the key architectural decisions in RiskFlow and the reasoning behind them. It is written for engineers who want to understand not just what was built, but why specific patterns were chosen over simpler alternatives.

---

## The Core Problem

Fraud decisioning systems face a fundamental tension: rules need to change faster than software can be deployed.

A new fraud pattern might emerge on a Monday morning. By the time a developer writes the rule, gets it reviewed, merged, and deployed, it could be Tuesday afternoon. Every transaction in between is evaluated against stale rules.

The standard industry response is to move rules out of code and into a database. RiskFlow implements this pattern with a hot-reloadable rule engine — rules are stored as boolean expressions in PostgreSQL, and a single API call refreshes the in-memory rule set without any service restart.

---

## Multi-Stage Scoring Pipeline

RiskFlow processes every transaction through three distinct stages. Each stage has a different purpose and a different failure mode.

### Stage 1 — Hard Rules

Hard rules are short-circuit conditions. The moment any hard rule fires, scoring stops and the transaction is AUTO_REJECTED with a score of 100. No further analysis is needed or wanted.

Hard rules cover three categories:

- **Blocklisted cards** — known fraudulent card fingerprints
- **Amount cap** — transactions above $10,000 are rejected regardless of other signals
- **Sanctioned countries** — transactions from OFAC-sanctioned jurisdictions are rejected by regulatory requirement

The key design decision here is short-circuiting. Hard rules do not accumulate — the first hit ends everything. This is correct because each hard rule is individually sufficient for rejection. There is no scenario where a sanctioned-country transaction should be approved because its behavioral score is low.

### Stage 2 — Behavioral Velocity Analytics

Hard rules catch known-bad signals. Behavioral analytics catch unknown-bad patterns.

A fraudster with a clean card, normal amounts, and a US IP address passes every hard rule. What gives them away is behavior over time: they burn stolen cards as fast as possible because the window before the card is cancelled is short.

RiskFlow tracks three behavioral signals using Redis sorted sets:

**Per-card velocity** — how many transactions has this card made in the last 1 minute, 5 minutes, and 1 hour? Three windows instead of one because window shape matters. A legitimate traveler might make 8 transactions in an hour at a human pace. A fraudster makes 8 transactions in 3 minutes. The 1-minute window distinguishes them.

**Per-device cross-card** — how many distinct card fingerprints has this device used in the last 24 hours? A legitimate user has 1-2 cards on a device. A fraud laptop cycles through dozens of stolen cards. Each card individually looks clean; the device fingerprint reveals the pattern.

**Per-IP cross-card** — how many distinct card fingerprints has this IP touched in the last hour? Same logic as per-device but at the network level. Catches fraud rings operating from the same IP block.

**Why Redis sorted sets?**

A plain counter with a TTL cannot answer "how many events in the last 60 seconds from right now." It can only answer "how many since the last reset" — which is vulnerable to boundary attacks where a fraudster fires just under the threshold in two adjacent windows.

A Redis sorted set stores each event as a member with its Unix timestamp as the score. At any moment, a `ZRANGEBYSCORE` query against `[now - window, now]` returns a true sliding window anchored to the present. `ZADD` adds the new event, `ZREMRANGEBYSCORE` trims old entries, and `ZCARD` counts what remains. Three atomic operations per window per signal.

For cross-entity tracking (device and IP), the sorted set member is the card fingerprint rather than the transaction ID. This means repeated use of the same card on the same device stays as exactly one entry — `ZADD` overwrites the score to the latest timestamp. `ZCARD` then returns the true count of distinct cards, not raw transaction volume.

### Stage 3 — Rule Engine

The rule engine evaluates all enabled rules against the transaction and accumulates score contributions from every rule that matches. Unlike Stage 1, there is no short-circuiting — every rule runs.

This is the correct design because individual rules capture weak signals. A transaction from a high-risk merchant category is suspicious but not decisive. The same transaction on a new device adds more signal. From an IP that recently touched multiple cards adds more still. No single rule crosses the rejection threshold, but the combination does.

Short-circuiting on the first rule match would mean missing the full picture. Accumulation is what turns "slightly suspicious plus slightly suspicious plus slightly suspicious" into "probably fraud."

---

## Hot-Reload Architecture

This is the most operationally significant feature in the system.

Rules are stored in a PostgreSQL table with a TEXT column for the expression — TEXT rather than VARCHAR(255) because complex boolean expressions can exceed 255 characters and a silent truncation would produce a malformed rule.

At startup, `@PostConstruct` on the `RuleEngine` bean loads all enabled rules into a `CopyOnWriteArrayList` in memory. The `@PostConstruct` annotation guarantees this runs after all dependencies are injected but before the application starts consuming Kafka messages — so no transaction is ever scored against an empty rule set.

The `CopyOnWriteArrayList` is the critical thread-safety choice. The Kafka consumer thread reads this list on every transaction — potentially hundreds of times per second. The admin reload endpoint writes to it occasionally. A plain `ArrayList` is not thread-safe for concurrent read and write: a simultaneous read and write causes a `ConcurrentModificationException`. `CopyOnWriteArrayList` solves this by making writes create a new copy of the underlying array. Reads are always lock-free and see either the old list or the new list — never a partial state.

The reload flow:

1. Analyst inserts or modifies a rule in PostgreSQL directly
2. Analyst calls `POST /admin/rules/reload`
3. `RuleEngine.reload()` calls `findByEnabledTrue()` and replaces the in-memory list
4. The Kafka consumer thread picks up the new list on its next iteration
5. No restart. No downtime. Transactions continue scoring throughout.

Rule evaluation uses Spring Expression Language (SpEL). The expression string from the database is parsed into an `Expression` object by `SpelExpressionParser`, then evaluated against a `StandardEvaluationContext` wrapping the `TransactionEvent` as the root object. SpEL resolves field names to getter calls — `amount > 500` becomes `event.getAmount() > 500` at evaluation time.

Each rule is evaluated inside a try-catch. A malformed expression — a typo from an analyst — throws a `ParseException` or `EvaluationException`. Catching it per-rule means one bad rule skips silently while all other rules continue evaluating. The error is logged so the analyst knows to fix it, but the scoring pipeline never crashes.

---

## Idempotency

Kafka guarantees at-least-once delivery. A consumer restart, a rebalance, or a network partition can cause the same message to be delivered more than once. Without protection, the same transaction could be scored twice — producing two `RiskDecision` records and potentially triggering duplicate actions downstream.

RiskFlow uses a Redis idempotency cache keyed on transaction ID. Before scoring begins, the consumer attempts `SET risk:processed:{txnId} 1 NX EX 86400` — a single atomic command that sets the key only if it does not already exist. If the key already exists, the transaction was already scored and the message is skipped. The 24-hour TTL ensures the cache does not grow unbounded.

The atomicity of `SET NX EX` is important. A non-atomic check-then-set (GET, then SET if empty) has a race condition where two consumer threads could both read "not exists" and both proceed to score the same transaction. The single-command atomic version eliminates this race entirely.

---

## Kafka Payload Format

The ingestion service publishes pipe-delimited key:value pairs rather than JSON. This was a deliberate choice for resilience: a pipe-delimited parser using `getOrDefault` is tolerant of missing or reordered fields, while a JSON deserializer bound to a fixed schema throws on any unexpected structure. The scoring service's `TransactionEvent.from()` parser handles field absence gracefully, which matters for schema evolution as new fields are added to the ingestion service over time.

---

## What Is Not Built Yet

This document covers what is currently implemented. The following are planned but not yet built:

- **Transactional Outbox Pattern** — solves the dual-write problem in the ingestion service where a crash between the PostgreSQL write and the Kafka publish could lose a transaction
- **DLQ Processor Service** — classifies and replays failed scoring events
- **MCP Server** — exposes the platform as typed tools for fraud analyst workflows
- **External Enrichment** — calls to third-party risk APIs with Resilience4j circuit breakers
