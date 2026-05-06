package com.riskflow.ingestion.model;

// The two possible states of an outbox record.
// PENDING: written to the database, not yet published to Kafka.
// PUBLISHED: the poller has successfully sent this to Kafka.
public enum OutboxStatus {
    PENDING,
    PUBLISHED
}