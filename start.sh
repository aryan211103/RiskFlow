#!/bin/bash
# RiskFlow — Start All Services
# Opens one Terminal window per service so logs stay separate.
# Run from the project root: chmod +x start.sh && ./start.sh

ROOT="/Users/aryansmac/RiskFLow"

# The OpenTelemetry Java agent jar — downloaded once, shared by all services.
# It instruments Spring Boot, Kafka, PostgreSQL, and Redis automatically
# without any code changes. We pass it as a JVM argument at startup.
AGENT="$ROOT/opentelemetry-javaagent.jar"

# Jaeger is running locally via Docker on port 4318 (OTLP HTTP format).
# Each service sends its spans here. The agent reads this from the environment.
EXPORTER_ENDPOINT="http://localhost:4318"

echo "Starting Docker infrastructure (Postgres, Redis, Zookeeper, Kafka, Jaeger)..."
cd "$ROOT" && docker compose up -d

echo "Waiting 10 seconds for Kafka to be ready..."
sleep 10

echo "Opening Auth Service window (port 8081)..."
open -a Terminal "$ROOT/auth-service"
sleep 1
osascript -e "tell application \"Terminal\" to do script \"cd $ROOT/auth-service && OTEL_SERVICE_NAME=auth-service OTEL_EXPORTER_OTLP_ENDPOINT=$EXPORTER_ENDPOINT ./mvnw spring-boot:run -Dspring-boot.run.jvmArguments='-javaagent:$AGENT'\" in front window"

sleep 1

echo "Opening Transaction Ingestion Service window (port 8082)..."
open -a Terminal "$ROOT/transaction-ingestion-service"
sleep 1
osascript -e "tell application \"Terminal\" to do script \"cd $ROOT/transaction-ingestion-service && OTEL_SERVICE_NAME=transaction-ingestion-service OTEL_EXPORTER_OTLP_ENDPOINT=$EXPORTER_ENDPOINT ./mvnw spring-boot:run -Dspring-boot.run.jvmArguments='-javaagent:$AGENT'\" in front window"

sleep 1

echo "Opening Risk Scoring Service window (port 8083)..."
open -a Terminal "$ROOT/risk-scoring-service"
sleep 1
osascript -e "tell application \"Terminal\" to do script \"cd $ROOT/risk-scoring-service && OTEL_SERVICE_NAME=risk-scoring-service OTEL_EXPORTER_OTLP_ENDPOINT=$EXPORTER_ENDPOINT ./mvnw spring-boot:run -Dspring-boot.run.jvmArguments='-javaagent:$AGENT'\" in front window"

sleep 1

echo "Opening DLQ Processor Service window (port 8084)..."
open -a Terminal "$ROOT/dlq-processor-service"
sleep 1
osascript -e "tell application \"Terminal\" to do script \"cd $ROOT/dlq-processor-service && OTEL_SERVICE_NAME=dlq-processor-service OTEL_EXPORTER_OTLP_ENDPOINT=$EXPORTER_ENDPOINT ./mvnw clean spring-boot:run -Dspring-boot.run.jvmArguments='-javaagent:$AGENT'\" in front window"

echo ""
echo "Done. Four windows are starting up:"
echo "  8081 → Auth Service"
echo "  8082 → Transaction Ingestion Service"
echo "  8083 → Risk Scoring Service"
echo "  8084 → DLQ Processor Service"
echo ""
echo "Wait for all four to print 'Started ... in ... seconds'"
echo "then run: python3 generate_transactions.py"
echo ""
echo "Jaeger UI: http://localhost:16686"