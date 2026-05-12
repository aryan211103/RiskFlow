# RiskFlow

![CI](https://github.com/aryan211103/RiskFlow/actions/workflows/ci.yml/badge.svg)

**Event-driven transaction risk scoring platform built for distributed systems portfolio.**

RiskFlow ingests payment transactions, scores them through a 4-stage risk pipeline, and produces decisions in real time — all without blocking the caller. No ML, no frontend. Pure backend distributed systems engineering.

---

## Architecture

```
                        ┌─────────────────────────────────────────────────────┐
                        │                     RiskFlow                        │
                        │                                                     │
  REST POST             │  ┌──────────────┐      Kafka Topic                 │
 /transactions ────────►│  │  Ingestion   │──── transactions ──►┐            │
                        │  │  Service     │                      │            │
                        │  │  :8082       │                      ▼            │
                        │  └──────┬───────┘            ┌──────────────┐      │
                        │         │                     │    Risk      │      │
                        │  Outbox │                     │   Scoring    │──► PostgreSQL
                        │  Event  │                     │   Service    │      │
                        │         ▼                     │   :8083      │      │
                        │  PostgreSQL                   └──────┬───────┘      │
                        │  (outbox_events)                     │              │
                        │                                      │ DLQ Topic    │
                        │  Redis ◄────── Rate Limiting         ▼              │
                        │  Redis ◄────── Velocity Analytics  ┌──────────────┐ │
                        │  Redis ◄────── Idempotency Cache   │     DLQ      │ │
                        │                                    │  Processor   │ │
                        │  Auth Service :8081                │   :8084      │ │
                        │  JWT-based authentication          └──────────────┘ │
                        │                                                     │
                        │  Jaeger :16686  ◄──── OpenTelemetry Tracing        │
                        └─────────────────────────────────────────────────────┘
```

---

## What Makes This Technically Interesting

- **Transactional outbox pattern** — transactions and outbox events are written in a single database transaction, eliminating the dual-write problem. Kafka publish happens separately via outbox polling, guaranteeing no message is ever lost even if Kafka is temporarily down.

- **4-stage scoring pipeline with SpEL hot reload** — rules are stored in PostgreSQL and evaluated using Spring Expression Language. The rule engine reloads every 60 seconds without a service restart, making it possible to deploy new fraud rules with zero downtime.

- **Behavioral velocity analytics** — Redis sorted sets track card, device, and user transaction frequency across sliding time windows. High-velocity patterns (e.g. 10 transactions from the same card in 60 seconds) contribute to the risk score.

- **Distributed tracing across Kafka** — OpenTelemetry trace context is propagated through Kafka message headers, so a single transaction produces a 151-span trace visible in Jaeger from ingestion through scoring.

- **Testcontainers integration tests** — the full pipeline is tested with real PostgreSQL, Kafka, and Redis containers. No mocks, no in-memory fakes. If container wiring or Hibernate mapping breaks, these tests catch it.

- **Redis-backed distributed rate limiting** — the ingestion endpoint enforces per-client request limits using Redis INCR with TTL. Limits are shared across service instances — the production pattern for horizontal scaling.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 17 |
| Framework | Spring Boot 3.2 |
| Messaging | Apache Kafka (Confluent 7.4.0) |
| Cache / Rate Limiting | Redis 7 |
| Database | PostgreSQL 16 |
| Resilience | Resilience4j 2.1.0 (circuit breaker) |
| Tracing | OpenTelemetry Java agent + Jaeger |
| Testing | JUnit 5, Testcontainers 1.19.3 |
| CI/CD | GitHub Actions |
| Containerization | Docker + Docker Compose |
| MCP Integration | TypeScript MCP server |

---

## Services

| Service | Port | Responsibility |
|---|---|---|
| auth-service | 8081 | JWT token issuance and validation |
| transaction-ingestion-service | 8082 | Accepts transactions via REST, writes to outbox, publishes to Kafka |
| risk-scoring-service | 8083 | Consumes Kafka events, runs 4-stage scoring pipeline, writes RiskDecision to PostgreSQL |
| dlq-processor-service | 8084 | Consumes failed events, classifies as TRANSIENT or POISON_PILL, retries or quarantines |

---

## Scoring Pipeline

Each transaction passes through four sequential stages. Scores accumulate. The final score determines the decision.

```
Stage 1 — Hard Rules
  Blocklisted card fingerprint       → immediate AUTO_REJECTED
  Sanctioned country (IP or billing) → immediate AUTO_REJECTED
  Amount exceeds cap ($50,000)       → immediate AUTO_REJECTED

Stage 2 — Behavioral Velocity (Redis sorted sets)
  Card velocity:   > 5 txns / 60s   → +30 score
  Device velocity: > 3 txns / 60s   → +20 score
  User velocity:   > 8 txns / 5min  → +25 score

Stage 3 — SpEL Rule Engine (hot reload every 60s)
  Rules stored in PostgreSQL, evaluated via Spring Expression Language
  Example rule: amount > 10000 && merchantRiskTier == 'HIGH' → +40 score
  New rules deploy without service restart

Stage 4 — External Enrichment
  IP reputation check (Resilience4j circuit breaker protects this call)
  High-risk IP → +20 score

Final Decision
  score >= 80  → AUTO_REJECTED
  score 50–79  → NEEDS_REVIEW
  score < 50   → APPROVED
```

---

## Load Test Results

Tested locally (MacBook, all services via Docker + Maven) on May 11, 2026.

**Throughput test (rate limiter disabled):**

| Metric | Result |
|---|---|
| Sustained throughput | 36 transactions/second |
| Total sent | 2,175 transactions in ~60 seconds |
| HTTP failures | 0 |
| Message loss | 0 |
| Kafka consumer lag | Fully drained within ~60s after burst |
| Decision breakdown | AUTO_REJECTED majority, NEEDS_REVIEW on rule matches |

**Rate limiting test (100 requests / 60s window):**

| Metric | Result |
|---|---|
| Requests attempted | 1,225 in ~30 seconds |
| Passed rate limiter | ~100 (first window exhausted in seconds at full speed) |
| Rate limited (429) | ~1,125 — all correctly rejected with Retry-After header |
| Message loss on passed requests | 0 |

**Scaling observation:** The ingestion layer is stateless and handled 36 req/sec with zero failures. The bottleneck is the single Kafka consumer partition — one partition means one consumer thread. To scale scoring throughput: add Kafka partitions and run parallel consumer instances. Kafka's consumer group model handles coordination automatically with no code changes required.

---

## Running Locally

**Prerequisites:** Docker Desktop, Java 17, Maven

```bash
# Start infrastructure (Kafka, PostgreSQL, Redis, Jaeger)
docker-compose up -d

# Start all four services (from project root)
./start.sh
```

Services start on ports 8081–8084. Jaeger UI is available at http://localhost:16686.

**Sending a test transaction:**

```bash
# Get a JWT token first
TOKEN=$(curl -s -X POST http://localhost:8081/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"password"}' | jq -r .token)

# Submit a transaction
curl -X POST http://localhost:8082/transactions \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user-001",
    "cardFingerprint": "card-abc123",
    "amount": 15000,
    "currency": "USD",
    "merchantId": "merchant-1",
    "merchantCategoryCode": "5411",
    "merchantRiskTier": "HIGH",
    "deviceFingerprint": "device-xyz",
    "ipAddress": "1.2.3.4",
    "ipCountry": "US",
    "billingCountry": "US",
    "userAgent": "Mozilla/5.0"
  }'
```

---

## Running Tests

**Unit tests only (no Docker required):**
```bash
# Run from any service directory
./mvnw test -Dtest="!ScoringPipelineIntegrationTest,!RateLimitIntegrationTest"
```

**Integration tests (requires Docker Desktop):**
```bash
# Scoring pipeline end-to-end (PostgreSQL + Kafka + Redis containers)
cd risk-scoring-service
./mvnw test -Dtest=ScoringPipelineIntegrationTest

# Rate limiting (PostgreSQL + Kafka + Redis containers)
cd transaction-ingestion-service
./mvnw test -Dtest=RateLimitIntegrationTest
```

**Note for Testcontainers on Docker Desktop 29.x:**
```bash
echo "api.version=1.44" > ~/.docker-java.properties
```

---

## CI/CD

GitHub Actions runs 4 parallel jobs on every push:

| Job | What it tests |
|---|---|
| Auth Service | Unit tests |
| Transaction Ingestion Service | Unit tests + RateLimitIntegrationTest |
| Risk Scoring Service | Unit tests + ScoringPipelineIntegrationTest (5 end-to-end tests) |
| DLQ Processor Service | Unit tests |

Total CI time: ~45 seconds. The green badge at the top of this README reflects the latest run.

---

## Observability

**Jaeger UI:** http://localhost:16686

RiskFlow uses OpenTelemetry distributed tracing with manual spans for all four scoring stages. Trace context is propagated through Kafka message headers, so each transaction produces a single trace that spans two services and crosses the Kafka boundary.

**What to look for in Jaeger:**
- Search for service `risk-scoring-service`
- Open any trace — you will see 151 spans
- The root span starts in `transaction-ingestion-service` and continues into `risk-scoring-service` via Kafka
- Each scoring stage (hard rules, velocity, SpEL engine, IP enrichment) has its own span with duration and outcome

---

## MCP Integration

RiskFlow includes a TypeScript MCP server (`riskflow-mcp-server`) that exposes 8 tools to Claude Desktop:

- `get_transaction` — look up a transaction by ID
- `get_risk_decision` — retrieve the scoring decision for a transaction
- `list_pending_review` — list all transactions in NEEDS_REVIEW state
- `override_decision` — manually approve or reject a transaction
- `get_velocity_stats` — inspect Redis velocity counters for a user/card/device
- `list_active_rules` — show all active SpEL rules in the rule engine
- `reload_rules` — trigger immediate rule reload without waiting for the 60s cycle
- `get_dlq_events` — inspect quarantined POISON_PILL events
