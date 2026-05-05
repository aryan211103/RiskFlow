package com.riskflow.scoring.model;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;

/**
 * Persistent audit record of every scoring decision made by this service.
 *
 * Design principles:
 *
 * 1. Immutability by convention. Once written, a RiskDecision row is never
 *    updated. If a transaction is re-scored (e.g. after a manual override),
 *    a new row is inserted. The full history is always preserved.
 *
 * 2. Self-contained. Every field needed to understand the decision is stored
 *    here. We don't rely on joining to the transactions table to explain why
 *    a decision was made — the decisionReason field tells the story.
 *
 * 3. Audit-first. decidedAt uses Instant (UTC) so timestamps are unambiguous
 *    regardless of server timezone.
 *
 * JPA annotations:
 * @Entity  - tells Hibernate this class maps to a database table
 * @Table   - names the table explicitly ("risk_decisions")
 * @Id      - marks the primary key field
 * @GeneratedValue - tells Hibernate to auto-assign the PK (uses a PostgreSQL
 *                   sequence under the hood with SEQUENCE strategy)
 * @Column  - optional; used here to add nullable=false constraints and to
 *            name columns explicitly
 * @Enumerated(STRING) - stores the enum as its string name ("APPROVED") rather
 *                       than its ordinal (0, 1, 2). Always use STRING — if you
 *                       ever reorder the enum values, ordinal-based storage
 *                       silently corrupts your data.
 */
@Entity
@Table(name = "risk_decisions")
public class RiskDecision {

    /**
     * Auto-generated primary key.
     * GenerationType.SEQUENCE is preferred over IDENTITY for PostgreSQL
     * because Hibernate can batch inserts more efficiently with sequences.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE,
                    generator = "risk_decision_seq")
    @SequenceGenerator(name = "risk_decision_seq",
                       sequenceName = "risk_decision_seq",
                       allocationSize = 1)
    private Long id;

    /**
     * The transactionId from the Kafka message payload.
     * This links back to the transactions table in the ingestion service.
     * nullable=false ensures we never accidentally write a decision without
     * knowing which transaction it belongs to.
     */
    @Column(name = "transaction_id", nullable = false)
    private String transactionId;

    /**
     * The outcome: APPROVED, NEEDS_REVIEW, or AUTO_REJECTED.
     * EnumType.STRING stores "AUTO_REJECTED" rather than the number 2.
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "decision", nullable = false)
    private DecisionType decision;

    /**
     * The numeric score (0-100) that the pipeline computed before
     * applying the threshold logic. Useful for analyst review — they
     * can see how close a transaction was to a threshold.
     */
    @Column(name = "risk_score", nullable = false)
    private int riskScore;

    /**
     * Short human-readable code explaining WHY this decision was made.
     * Examples: "BLOCKLIST_HIT", "AMOUNT_EXCEEDS_CAP", "SANCTIONED_COUNTRY",
     *           "HIGH_CARD_VELOCITY", "RULE_ENGINE_THRESHOLD"
     * This is the field analysts will filter on most frequently.
     */
    @Column(name = "decision_reason", nullable = false)
    private String decisionReason;

    /**
     * Which pipeline stage made the final call.
     * Examples: "HARD_RULES", "BEHAVIORAL_ANALYZER", "RULE_ENGINE"
     * Useful for monitoring — if 90% of rejections happen at HARD_RULES,
     * maybe those rules are too aggressive.
     */
    @Column(name = "processing_stage", nullable = false)
    private String processingStage;

    /**
     * UTC timestamp of when this decision was written.
     * Instant is always UTC, which avoids timezone confusion in a distributed
     * system where different services might run in different timezones.
     */
    @Column(name = "decided_at", nullable = false)
    private Instant decidedAt;

    // -----------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------

    /**
     * No-arg constructor required by JPA.
     * Hibernate needs to instantiate entities via reflection, and reflection
     * requires a no-arg constructor. Mark it protected so application code
     * can't accidentally create an empty, invalid RiskDecision.
     */
    protected RiskDecision() {}

    /**
     * The constructor your application code will actually use.
     * Every field is required — there is no valid RiskDecision with a null
     * decision or missing transactionId.
     */
    public RiskDecision(String transactionId,
                        DecisionType decision,
                        int riskScore,
                        String decisionReason,
                        String processingStage) {
        this.transactionId = transactionId;
        this.decision = decision;
        this.riskScore = riskScore;
        this.decisionReason = decisionReason;
        this.processingStage = processingStage;
        this.decidedAt = Instant.now(); // capture the current UTC moment
    }

    // -----------------------------------------------------------------
    // Getters — no setters, enforcing immutability by convention
    // -----------------------------------------------------------------

    public Long getId() { return id; }
    public String getTransactionId() { return transactionId; }
    public DecisionType getDecision() { return decision; }
    public int getRiskScore() { return riskScore; }
    public String getDecisionReason() { return decisionReason; }
    public String getProcessingStage() { return processingStage; }
    public Instant getDecidedAt() { return decidedAt; }
}