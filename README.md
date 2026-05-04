# RiskFlow

A distributed backend system that scores financial transactions for fraud risk in real time.

When a transaction comes in, RiskFlow runs it through a multi-stage pipeline — checking hard rules, analyzing behavioral patterns, and evaluating a configurable rule engine — then stamps it as **APPROVED**, **NEEDS REVIEW**, or **AUTO REJECTED**. Every decision is logged with a full audit trail explaining exactly why it was made.

Built with Java, Spring Boot, Apache Kafka, Redis, and PostgreSQL across five microservices.

---

## What it does

1. A transaction event arrives via REST API
2. It gets saved to the database and published to a Kafka topic
3. The Risk Scoring Service picks it up asynchronously and runs it through three scoring stages
4. A decision comes out the other end — with a score, a reason, and a full record of what triggered it
5. A fraud analyst can review flagged transactions and override decisions through a natural language interface powered by Claude Desktop

---

## The scoring pipeline

**Stage 1 — Hard Rules**
Instant blocks. Card on a blocklist, amount above a hard cap, transaction from a sanctioned country — these get rejected immediately without going further.

**Stage 2 — Behavioral Analysis**
Redis tracks activity in real time across three dimensions:
- Per card: how many transactions in the last 60 seconds, 5 minutes, and 1 hour
- Per device: how many distinct cards used from this device in the last hour
- Per IP address: how many distinct cards from this IP in the last hour

This catches coordinated fraud patterns that single-account checks miss — card testing rings, device takeovers, carding operations.

**Stage 3 — Rule Engine**
Rules are stored in the database and can be updated without redeploying. Each rule is a boolean expression evaluated against the transaction and its behavioral signals. Examples:

- Card testing: small amount, multiple transactions, multiple merchants in 5 minutes
- Impossible travel: transaction from a country 1000km away, less than 30 minutes after the last one
- Device takeover: five or more distinct cards from the same device in an hour

**Decision**
Scores below 20 are approved. Scores between 20 and 59 go to a review queue. Scores of 60 and above are auto rejected.

---

## What makes it interesting

**The outbox pattern**
Most systems do two separate writes when a transaction comes in — save to the database, then publish to Kafka. If anything fails between those two steps, you get silent data loss. RiskFlow uses the transactional outbox pattern: both writes happen inside a single database transaction, and a background relay handles the Kafka publish. No transaction is ever lost.

**Idempotent consumers**
Kafka guarantees at-least-once delivery, which means the same message can arrive twice under failure conditions. Before processing any message, the Risk Scoring Service checks Redis to see if this transaction has already been scored. If yes, it skips — no duplicate decisions, no corrupt audit trail.

**Dead letter queue with active handling**
When message processing fails, the message goes to a dead letter topic. A dedicated DLQ Processor Service reads that topic, classifies each failure (transient error, bad message, downstream service down), and takes the right action — retry, quarantine, or park and replay when the downstream service recovers. This is not just a graveyard topic.

**Analyst interface via MCP**
An MCP server exposes the platform as tools that can be called by Claude Desktop. A fraud analyst can have a conversation like:

> "Show me the five most borderline transactions from the last hour"

> "Why was transaction txn_8f2a4c rejected?"

> "Override txn_3b1d to approved — user appealed and the context is clean"

Every override is stored in the audit log. The tools are plain HTTP under the hood — Claude is just one possible client.

---

## Services

| Service | Port | Status |
|---|---|---|
| Auth Service | 8081 | Complete |
| Transaction Ingestion Service | 8082 | Complete |
| Risk Scoring Service | 8083 | In progress |
| DLQ Processor Service | 8084 | Planned |
| MCP Server | — | Planned |

---

## Tech stack

| | |
|---|---|
| Language | Java 17 |
| Framework | Spring Boot 3 |
| Database | PostgreSQL 16 |
| Cache | Redis 7 |
| Messaging | Apache Kafka |
| Resilience | Resilience4j |
| Containers | Docker, Docker Compose |
| Analyst interface | Model Context Protocol (MCP) |

---

## Running locally

You need Docker Desktop and Java 17.

```bash
# Clone and start infrastructure
git clone https://github.com/aryan211103/RiskFlow.git
cd RiskFlow
docker compose up -d

# Create the database
docker exec -it riskflow-postgres-1 psql -U postgres -c "CREATE DATABASE riskflow;"

# Start the Auth Service
cd auth-service && ./mvnw spring-boot:run

# Start the Transaction Ingestion Service (new terminal)
cd transaction-ingestion-service && ./mvnw spring-boot:run
```

Send a test transaction:

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

## Why rule-based and not ML

Rule-based scoring is the production standard at payment companies, banks, and processors — not a compromise. Rules are explainable to auditors, updateable in minutes without retraining anything, and handle the obvious cases cheaply so expensive inference is reserved for the hard ones. RiskFlow's rule engine is designed to reflect this: hot-reloadable, composable, and fully auditable.

---

## Author

Aryan — MSCS student at Northeastern University (Khoury College of Computer Sciences)
