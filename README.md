# Toxiq — Real-Time Content Moderation Pipeline

A distributed microservices backend system similar to Trust & Safety infrastructure at companies like Twitter, YouTube, and Meta.

## Architecture
- **Auth Service** (port 8081) — User registration, login, JWT authentication
- **Post Service** (port 8082) — Accepts posts, stores in PostgreSQL, publishes Kafka events
- **Moderation Service** (port 8083) — Consumes Kafka events, runs rule-based risk scoring *(in progress)*
- **API Gateway** (port 8080) — Routes requests to all services *(coming soon)*

## Tech Stack
- Java 17 + Spring Boot 3.x
- Apache Kafka (async event streaming)
- PostgreSQL (persistent storage)
- Redis (caching + rate limiting)
- Docker + Docker Compose

## Running Locally
```bash
docker compose up -d
cd auth-service && ./mvnw spring-boot:run
cd post-service && ./mvnw spring-boot:run
```
