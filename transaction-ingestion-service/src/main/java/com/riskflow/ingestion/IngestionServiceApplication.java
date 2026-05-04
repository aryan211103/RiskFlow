package com.riskflow.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// Entry point for the Transaction Ingestion Service.
// This service accepts incoming transaction events via REST,
// persists them to PostgreSQL, and publishes them to Kafka
// for downstream processing by the Risk Scoring Service.
@SpringBootApplication
public class IngestionServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(IngestionServiceApplication.class, args);
    }
}