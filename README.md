# RiskFlow

An event-driven transaction risk scoring platform that ingests payment events, scores them through a multi-stage asynchronous pipeline, and produces auditable **APPROVED / NEEDS_REVIEW / AUTO_REJECTED** decisions with sub-second end-to-end latency.

Built as a pure software and distributed systems showcase — demonstrating event-driven architecture, behavioral analytics, and hot-reloadable decisioning at production quality.

---

## What It Does

When a transaction is submitted, RiskFlow runs it through three scoring stages in sequence:

1. **Hard Rules** — instant rejection for blocklisted cards, amounts above a hard cap, or sanctioned countries
2. **Behavioral Velocity Analytics** — Redis sliding windows detect rapid card usage, and cross-entity tracking catches a single device or IP cycling through multiple cards
3. **Rule Engine** — composable SpEL boolean expressions loaded from PostgreSQL, hot-reloadable without restarting the service

Every transaction produces a `RiskDecision` record in PostgreSQL with the numeric score, decision, reason, and stage — a complete audit trail.

---

## Architecture

```
POST /transactions
        │
        ▼
Transaction Ingestion Service (port 8082)
        │
        ├── Persists Transaction to PostgreSQL (status=PENDING)
        └── Publishes to Kafka topic: transaction.received
                │
                ▼
        Risk Scoring Service (port 8083)
                │
                ├── Idempotency check (Redis) — skip if already scored
                │
                ├── Stage 1: Hard Rules
                │     Card on blocklist       → AUTO_REJECTED
                │     Amount above hard cap   → AUTO_REJECTED
                │     Sanctioned country      → AUTO_REJECTED
                │
                ├── Stage 2: Behavioral Analyzer (Redis sliding windows)
                │     Per-card velocity:       1m / 5m / 1h windows
                │     Per-device cross-card:   distinct cards in 24h
                │     Per-IP cross-card:       distinct cards in 1h
                │
                ├── Stage 3: Rule Engine (hot-reloadable SpEL)
                │     Rules loaded from PostgreSQL
                │     Reload via POST /admin/rules/reload
                │
                └── Decision Engine
                      score < 20   → APPROVED
                      score 20-59  → NEEDS_REVIEW
                      score ≥ 60   → AUTO_REJECTED
```

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Backend | Java Spring Boot | Microservices, REST APIs |
| Database | PostgreSQL | Persistent storage, audit trail |
| Cache + State | Redis | Sliding-window velocity counters, idempotency |
| Messaging | Apache Kafka | Async event streaming between services |
| Containers | Docker + Docker Compose | Run full stack locally |

---

## Services

| Service | Port | Status |
|---|---|---|
| Auth Service | 8081 | ✅ Complete |
| Transaction Ingestion Service | 8082 | ✅ Complete |
| Risk Scoring Service | 8083 | ✅ Complete |

---

## Running Locally

**Prerequisites:** Docker, Java 17, Maven

**Step 1 — Start infrastructure:**
```bash
docker compose up -d
```

**Step 2 — Start services (each in a separate terminal):**
```bash
cd auth-service && ./mvnw spring-boot:run
cd transaction-ingestion-service && ./mvnw spring-boot:run
cd risk-scoring-service && ./mvnw spring-boot:run
```

**Step 3 — Verify the scoring service loaded rules:**

Look for this line in the risk-scoring-service terminal:
```
Rule engine loaded 4 enabled rules
```

---

## Example API Calls

**Register and login:**
```bash
curl -X POST http://localhost:8081/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"analyst1","password":"password123"}'

curl -X POST http://localhost:8081/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"analyst1","password":"password123"}'
```

**Submit a transaction (triggers NEEDS_REVIEW via rule engine):**
```bash
curl -X POST http://localhost:8082/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user_001",
    "cardFingerprint": "fpr_abc123",
    "amount": 600,
    "currency": "USD",
    "merchantId": "mch_001",
    "merchantCategoryCode": "5942",
    "merchantRiskTier": "high",
    "deviceFingerprint": "dev_001",
    "ipAddress": "203.0.113.45",
    "ipCountry": "US",
    "billingCountry": "US"
  }'
```

**Submit a transaction that hits a hard rule (AUTO_REJECTED):**
```bash
curl -X POST http://localhost:8082/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user_002",
    "cardFingerprint": "fpr_BLOCKED_001",
    "amount": 50,
    "currency": "USD",
    "merchantId": "mch_001",
    "merchantCategoryCode": "5942",
    "merchantRiskTier": "low",
    "deviceFingerprint": "dev_002",
    "ipAddress": "203.0.113.45",
    "ipCountry": "US",
    "billingCountry": "US"
  }'
```

**Hot-reload rules after updating the database:**
```bash
curl -X POST http://localhost:8083/admin/rules/reload
```

---

## Seeding Rules

Connect to PostgreSQL and insert scoring rules:

```bash
docker exec -it riskflow-postgres-1 psql -U postgres -d riskflow
```

```sql
INSERT INTO rules (id, name, expression, score, enabled)
VALUES (nextval('rule_seq'), 'high_risk_merchant', 'merchantRiskTier == ''high''', 25, true);

INSERT INTO rules (id, name, expression, score, enabled)
VALUES (nextval('rule_seq'), 'high_amount', 'amount > 500', 20, true);

INSERT INTO rules (id, name, expression, score, enabled)
VALUES (nextval('rule_seq'), 'gambling_mcc', 'merchantCategoryCode == ''7995''', 30, true);

INSERT INTO rules (id, name, expression, score, enabled)
VALUES (nextval('rule_seq'), 'country_mismatch', 'ipCountry != billingCountry', 15, true);
```

Then reload without restarting:
```bash
curl -X POST http://localhost:8083/admin/rules/reload
```

---

## Decision Thresholds

| Score Range | Decision |
|---|---|
| 0 – 19 | APPROVED |
| 20 – 59 | NEEDS_REVIEW |
| 60+ | AUTO_REJECTED |

Hard rules always produce score 100 and bypass the pipeline entirely.
