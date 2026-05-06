package com.riskflow.ingestion.model;

import jakarta.persistence.*;
import java.time.Instant;

// This entity maps to the outbox_events table in PostgreSQL.
// Every time a transaction is created, one OutboxEvent is also created
// in the same database transaction. The OutboxPoller reads this table
// and publishes pending events to Kafka.
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    // Standard auto-generated primary key
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // The transactionId this event is for.
    // The poller includes this in the Kafka message so the scoring
    // service knows which transaction to process.
    @Column(nullable = false)
    private String transactionId;

    // The full pipe-delimited payload string, pre-built at write time.
    // We store the payload here so the poller does not need to
    // reconstruct it — it just reads and publishes.
    @Column(nullable = false, length = 2000)
    private String payload;

    // PENDING = not yet published to Kafka
    // PUBLISHED = successfully published, safe to ignore
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OutboxStatus status = OutboxStatus.PENDING;

    // When this record was inserted. Useful for alerting if a record
    // stays PENDING for too long — that would indicate the poller is stuck.
    @Column(nullable = false)
    private Instant createdAt = Instant.now();

    // When the poller successfully published this to Kafka.
    // Null until the poller processes it.
    private Instant publishedAt;

    // --- Getters and Setters ---

    public Long getId() { return id; }

    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }

    public OutboxStatus getStatus() { return status; }
    public void setStatus(OutboxStatus status) { this.status = status; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public Instant getPublishedAt() { return publishedAt; }
    public void setPublishedAt(Instant publishedAt) { this.publishedAt = publishedAt; }
}