package com.riskflow.dlq_processor_service.model;

// Tracks the lifecycle of a failed event through the DLQ Processor.
// PENDING     → just arrived, awaiting classification and retry
// RETRYING    → a retry attempt is in progress
// RESOLVED    → a retry succeeded, no further action needed
// QUARANTINED → max retries exhausted, needs human review
public enum FailedEventStatus {
    PENDING,
    RETRYING,
    RESOLVED,
    QUARANTINED
}