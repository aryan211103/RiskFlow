package com.riskflow.scoring.model;

/**
 * The three possible outcomes of the risk scoring pipeline.
 *
 * These map directly to the TransactionStatus enum in the ingestion service.
 * When a RiskDecision is written, we also update the Transaction row's status
 * to match. This keeps both services' views of the world consistent.
 *
 * APPROVED     - score < 20. Transaction is clean. Let it through.
 * NEEDS_REVIEW - score 20-59. Uncertain. Route to a human analyst queue.
 * AUTO_REJECTED - score >= 60, or a hard rule triggered. Block immediately.
 */
public enum DecisionType {
    APPROVED,
    NEEDS_REVIEW,
    AUTO_REJECTED
}