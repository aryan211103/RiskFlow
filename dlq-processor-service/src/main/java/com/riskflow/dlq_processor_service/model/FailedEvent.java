package com.riskflow.dlq_processor_service.model;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

// Represents a single failed transaction scoring event.
// Every message that arrives on the DLQ topic gets persisted here
// before any retry or quarantine logic runs. This gives us a complete
// audit trail of every failure the system has ever seen.
@Entity
@Table(name = "failed_events")
public class FailedEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // The transactionId from the original Kafka message.
    // Used to correlate this failure with the transaction and risk decision records.
    @Column(nullable = false)
    private String transactionId;

    // The full original Kafka payload — the same pipe-delimited string
    // that the scoring service failed to process. Stored here so the
    // DLQ Processor can replay it without needing to look anything up.
    @Column(nullable = false, length = 2000)
    private String originalPayload;

    // The exception message from the scoring service.
    // Tells us why it failed — used by the classifier to determine
    // whether this is transient or a poison pill.
    @Column(length = 2000)
    private String errorMessage;

    // TRANSIENT: a temporary infrastructure problem, worth retrying
    // POISON_PILL: a message that will always fail, needs quarantine
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private FailureType failureType;

    // PENDING: just arrived, not yet processed by the DLQ Processor
    // RETRYING: currently being retried
    // RESOLVED: a retry succeeded
    // QUARANTINED: gave up after max retries, needs human review
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private FailedEventStatus status = FailedEventStatus.PENDING;

    // How many retry attempts have been made so far.
    // The DLQ Processor will give up and quarantine after 3 attempts.
    @Column(nullable = false)
    private int retryCount = 0;

    // When this failure was first received by the DLQ Processor
    @Column(nullable = false)
    private Instant createdAt = Instant.now();

    // When the most recent retry attempt was made. Null until first retry.
    private Instant lastRetriedAt;

    // When this was resolved or quarantined. Null until then.
    private Instant resolvedAt;

    // --- Getters and Setters ---

    public Long getId() { return id; }

    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }

    public String getOriginalPayload() { return originalPayload; }
    public void setOriginalPayload(String originalPayload) { this.originalPayload = originalPayload; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public FailureType getFailureType() { return failureType; }
    public void setFailureType(FailureType failureType) { this.failureType = failureType; }

    public FailedEventStatus getStatus() { return status; }
    public void setStatus(FailedEventStatus status) { this.status = status; }

    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }

    public Instant getCreatedAt() { return createdAt; }

    public Instant getLastRetriedAt() { return lastRetriedAt; }
    public void setLastRetriedAt(Instant lastRetriedAt) { this.lastRetriedAt = lastRetriedAt; }

    public Instant getResolvedAt() { return resolvedAt; }
    public void setResolvedAt(Instant resolvedAt) { this.resolvedAt = resolvedAt; }
}