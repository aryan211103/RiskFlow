package com.riskflow.ingestion.model;

// Represents the lifecycle state of a transaction as it moves
// through the risk scoring pipeline.
//
// PENDING     — transaction received and persisted, awaiting scoring
// APPROVED    — risk score below threshold, transaction cleared
// NEEDS_REVIEW — risk score in the uncertain band, routed to analyst queue
// AUTO_REJECTED — risk score above threshold, transaction blocked automatically
public enum TransactionStatus {
    PENDING,
    APPROVED,
    NEEDS_REVIEW,
    AUTO_REJECTED
}