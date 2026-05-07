#!/bin/bash
# RiskFlow — Start All Services
# Opens one Terminal window per service so logs stay separate.
# Run from the project root: chmod +x start.sh && ./start.sh

ROOT="/Users/aryansmac/RiskFLow"

echo "Starting Docker infrastructure (Postgres, Redis, Zookeeper, Kafka)..."
cd "$ROOT" && docker compose up -d

echo "Waiting 10 seconds for Kafka to be ready..."
sleep 10

# -----------------------------------------------------------------------
# open -a Terminal <path> opens a new Terminal window pointed at that path.
# The follow-up osascript tells that front window to run the Maven command.
# We sleep 1 second between each so macOS has time to open the window
# before we try to send it a command.
# -----------------------------------------------------------------------

echo "Opening Auth Service window (port 8081)..."
open -a Terminal "$ROOT/auth-service"
sleep 1
osascript -e "tell application \"Terminal\" to do script \"cd $ROOT/auth-service && ./mvnw spring-boot:run\" in front window"

sleep 1

echo "Opening Transaction Ingestion Service window (port 8082)..."
open -a Terminal "$ROOT/transaction-ingestion-service"
sleep 1
osascript -e "tell application \"Terminal\" to do script \"cd $ROOT/transaction-ingestion-service && ./mvnw spring-boot:run\" in front window"

sleep 1

echo "Opening Risk Scoring Service window (port 8083)..."
open -a Terminal "$ROOT/risk-scoring-service"
sleep 1
osascript -e "tell application \"Terminal\" to do script \"cd $ROOT/risk-scoring-service && ./mvnw spring-boot:run\" in front window"

sleep 1

echo "Opening DLQ Processor Service window (port 8084)..."
open -a Terminal "$ROOT/dlq-processor-service"
sleep 1
osascript -e "tell application \"Terminal\" to do script \"cd $ROOT/dlq-processor-service && ./mvnw clean spring-boot:run\" in front window"

echo ""
echo "Done. Four windows are starting up:"
echo "  8081 → Auth Service"
echo "  8082 → Transaction Ingestion Service"
echo "  8083 → Risk Scoring Service"
echo "  8084 → DLQ Processor Service"
echo ""
echo "Wait for all four to print 'Started ... in ... seconds'"
echo "then run: python3 generate_transactions.py"
