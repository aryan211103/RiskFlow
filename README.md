# RiskFlow

> An event-driven transaction risk scoring platform built on Java, Spring Boot, Apache Kafka, Redis, and PostgreSQL.

RiskFlow is a distributed microservices backend that ingests transaction events, scores them through a multi-stage asynchronous pipeline, and produces auditable risk decisions with sub-100ms end-to-end latency. The architecture is modeled after production fraud decisioning backends at payment companies — rule-based scoring is the industry norm in fraud (used by Stripe Radar, Visa, and major banks) because it is auditable, explainable, and tunable without retraining a model.

This is a pure software engineering project. Its purpose is to demonstrate production-grade backend patterns: idempotent Kafka consumers, the transactional outbox, dead-letter classification and replay, a hot-reloadable composable rule engine, and cross-entity behavioral analytics in Redis. An MCP server exposes the entire platform as typed tools drivable by fraud analysts via Claude Desktop or any external client.

---

## Architecture

```
Transaction event (POST /transactions or synthetic generator)
        │
        ▼
Transaction Ingestion Service (port 8082)
        │
        ├── Single DB transaction:
        │     INSERT transaction (status=PENDING)
        │     INSERT outbox_events row
        │     COMMIT
        │
        └── Outbox Relay (background poller)
              Reads unpublished outbox rows
              Publishes to Kafka → marks published_at
        │
        ▼
Kafka topic: transaction.received
        │
        ▼
Risk Scoring Service (port 8083)
        │
        ├── Idempotency check (Redis) ── already scored? skip + commit offset
        │
        ├── Stage 1: Hard Rules
        │     Card on blocklist       → AUTO_REJECTED immediately
        │     Amount above hard cap   → AUTO_REJECTED immediately
        │     Sanctioned country      → AUTO_REJECTED immediately
        │
        ├── Stage 2: Behavioral Analyzer (Redis sliding windows)
        │     Per-card velocity:      txn count in last 60s / 5min / 1hr
        │     Per-device cross-card:  distinct cards from device in last hour
        │     Per-IP cross-card:      distinct cards from IP in last hour
        │     Geographic state:       impossible travel detection
        │     Spend pattern:          rolling mean/stddev deviation per user
        │
        ├── Stage 3: Rule Engine (hot-reloadable, composable SpEL expressions)
        │     Rules loaded from PostgreSQL at startup
        │     Hot reload via POST /admin/rules/reload
        │     Each rule evaluates a boolean expression over TransactionContext
        │     Each rule fire → reason code + score contribution
        │
        ├── Stage 4: External Enrichment (circuit-breaker-protected)
        │     Resilience4j circuit breaker wraps external API call
        │     On breaker open → graceful degradation, pipeline continues
        │
        ├── Decision Engine
        │     risk_score < 20        → APPROVED
        │     20 ≤ risk_score < 60   → NEEDS_REVIEW
        │     risk_score ≥ 60        → AUTO_REJECTED
        │     Full audit trail stored in PostgreSQL
        │
        └── On failure (after retries) → publish to DLQ topic
                │
                ▼
        DLQ Processor Service (port 8084)
                │
                ├── Transient failure     → retry with exponential backoff
                ├── Poison message        → quarantine + alert
                └── Downstream unavailable → park, replay on health recovery
```

---

## Services

| Service | Port | Status | Description |
|---|---|---|---|
| Auth Service | 8081 | Complete | User registration, login, JWT authentication |
| Transaction Ingestion Service | 8082 | Complete (outbox in progress) | Accepts transaction events, persists to PostgreSQL, publishes via outbox relay |
| Risk Scoring Service | 8083 | In progress | Multi-stage scoring pipeline: hard rules, behavioral analytics, rule engine, decision engine |
| DLQ Processor Service | 8084 | Planned | Failure classification, replay, and quarantine |
| MCP Server | — | Planned | Typed tools exposing the platform to external clients and LLM agents |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Backend | Java 17, Spring Boot 3 |
| Database | PostgreSQL 16 |
| Cache and behavioral state | Redis 7 |
| Messaging | Apache Kafka (Confluent 7.4) |
| Resilience | Resilience4j |
| Containers | Docker, Docker Compose |
| Testing | JUnit 5, Mockito, Testcontainers |
| External interface | Model Context Protocol (MCP) server |
| CI/CD | GitHub Actions (planned) |

---

## Transaction Event Schema

Every transaction event carries a rich data model enabling cross-entity behavioral tracking:

```json
{
  "transactionId": "txn_8f2a4c",
  "userId": "user_abc123",
  "cardFingerprint": "fpr_4a7b2",
  "amount": 4500,
  "currency": "USD",
  "merchantId": "mch_amazon_us",
  "merchantCategoryCode": "5942",
  "merchantRiskTier": "low",
  "deviceFingerprint": "dev_xyz",
  "ipAddress": "203.0.113.45",
  "ipCountry": "RO",
  "billingCountry": "US",
  "userAgent": "Mozilla/5.0...",
  "createdAt": "2026-05-03T14:22:18Z"
}
```

---

## Key Engineering Patterns

### Transactional Outbox
The ingestion service writes the transaction row and the outbox event row inside a single database transaction. A background relay polls unpublished outbox rows and publishes them to Kafka. This eliminates the dual-write problem: no transaction is ever persisted without its corresponding Kafka event, and no Kafka event is ever published for a transaction that was not committed.

### Idempotent Kafka Consumer
Kafka's at-least-once delivery guarantee means the risk scoring service can receive the same message more than once under failure conditions. Before processing any message, the service checks a Redis key for the transaction ID. If the key exists, the message is skipped and the offset is committed. This prevents duplicate risk decisions from appearing in the audit trail.

### Hot-Reloadable Rule Engine
Risk rules change constantly. Hardcoded if-statements require a redeployment for every policy change. Rules in RiskFlow are persisted in PostgreSQL and loaded into an in-memory engine at startup. A single admin endpoint triggers a live reload without restarting the service. Rules are expressed as boolean SpEL expressions evaluated against a structured TransactionContext object containing all transaction fields and behavioral signals from Stage 2.

Example rule expressions:

```
# Card testing pattern
amount < 5 and behavioral.velocity_card_5min >= 3 and behavioral.distinct_merchants_5min >= 3

# Device takeover signal
behavioral.device_distinct_cards_1hr >= 5

# Impossible travel
behavioral.geo_distance_from_last_txn_km > 1000 and behavioral.minutes_since_last_txn < 30

# First-time high-risk country
user.previous_txns_in_country == 0 and ipCountry in highRiskCountries and amount > 200
```

### Cross-Entity Behavioral Analytics
Single-entity velocity tracking (per user) is shallow. RiskFlow tracks velocity across three entity types in Redis using sorted sets with TTL-based expiry:

```
Per-card:    ZADD vel:card:<fingerprint> <timestamp> <txnId>
Per-device:  SADD device:<fingerprint>:cards:1hr <cardFingerprint>
Per-IP:      SADD ip:<address>:cards:1hr <cardFingerprint>
```

This catches coordinated fraud patterns that single-entity tracking misses: card testing rings (many small transactions across merchants from one card), account takeover (many distinct cards from one device), and carding rings (many distinct cards from one IP).

### Dedicated DLQ Processor
A dead-letter topic with no active consumer is a graveyard. The DLQ Processor Service actively consumes the failure topic, classifies each failure, and takes the appropriate action. Transient failures are retried with exponential backoff. Poison messages (deserialization failures, validation errors) are quarantined and alerted on. Messages that failed because a downstream service was unavailable are parked and replayed automatically on health recovery.

### MCP Server
The platform exposes a Model Context Protocol server with typed tools covering the full analyst workflow: scoring hypothetical transactions, reviewing the NEEDS_REVIEW queue, overriding decisions with a full audit log entry, explaining any past decision with a complete breakdown of every rule that fired and every behavioral signal that contributed, and inspecting and replaying DLQ messages. The MCP client is Claude Desktop, but the tools are plain HTTP under the hood and could be wired to any client.

---

## Running Locally

**Prerequisites:** Docker Desktop, Java 17, Maven

```bash
# Clone the repo
git clone https://github.com/aryan211103/riskflow.git
cd riskflow

# Start all infrastructure (PostgreSQL, Redis, Kafka, ZooKeeper)
docker compose up -d

# Start Auth Service
cd auth-service && ./mvnw spring-boot:run

# Start Transaction Ingestion Service (separate terminal)
cd transaction-ingestion-service && ./mvnw spring-boot:run

# Start Risk Scoring Service (separate terminal)
cd risk-scoring-service && ./mvnw spring-boot:run
```

**Verify Kafka is receiving events:**
```bash
docker exec -it <kafka-container-id> \
  kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic transaction.received --from-beginning
```

**Create a test transaction:**
```bash
curl -X POST http://localhost:8082/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user_abc123",
    "cardFingerprint": "fpr_4a7b2",
    "amount": 4500,
    "currency": "USD",
    "merchantId": "mch_amazon_us",
    "ipCountry": "RO",
    "billingCountry": "US",
    "deviceFingerprint": "dev_xyz"
  }'
```

---

## Project Status

This project is under active development. The tier system below governs build order: Tier 1 ships as a standalone defensible system before any Tier 2 work begins.

**Tier 1 (in progress)**
- [x] Auth Service
- [x] Transaction Ingestion Service (naive Kafka producer)
- [x] Risk Scoring Service skeleton (Kafka consumer wired, stub analyzer)
- [ ] Domain refactor: Post to Transaction across codebase
- [ ] Idempotency and basic DLQ topic publishing
- [ ] Behavioral Analyzer (Redis sliding windows)
- [ ] Rule Engine v1 (Postgres-persisted rules, hot reload)

**Tier 2 (planned)**
- [ ] Transactional Outbox Pattern in Ingestion Service
- [ ] DLQ Processor Service with failure classification and replay
- [ ] Synthetic Transaction Generator
- [ ] Composable SpEL rule expressions
- [ ] External Enrichment with Resilience4j circuit breaker
- [ ] MCP Server with analyst tools

**Tier 3 (stretch)**
- [ ] API Gateway with Redis-backed rate limiting
- [ ] Testcontainers integration tests
- [ ] GitHub Actions CI/CD
- [ ] OpenTelemetry distributed tracing

---

## Why Rule-Based Scoring

Rule-based decisioning is not a compromise. It is the production standard in fraud and risk systems at payment companies, banks, and processors. The reasons are regulatory (decisions must be explainable to auditors and customers), operational (rules can be updated in minutes without retraining a model), and practical (obvious fraud patterns are caught cheaply by simple rules; expensive ML inference is reserved for ambiguous cases). RiskFlow's rule engine is designed to reflect this reality: hot-reloadable, composable, auditable, and instrumented per rule.

---

## Author

Aryan — MSCS student at Northeastern University (Khoury College of Computer Sciences), specializing in AI and ML.
