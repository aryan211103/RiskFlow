package com.riskflow.scoring.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;

/**
 * Represents a single scoring rule stored in PostgreSQL.
 *
 * Each rule has:
 *   - A human-readable name for logging and analyst UI
 *   - A SpEL expression that is evaluated against the transaction
 *   - A score contribution added to the total if the expression is true
 *   - An enabled flag so analysts can disable rules without deleting them
 *
 * Why store rules in the database instead of code?
 * Analysts can add, modify, or disable rules without a code deploy.
 * The hot-reload endpoint refreshes the in-memory rule list from this table.
 */
@Entity
@Table(name = "rules")
public class Rule {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "rule_seq")
    @SequenceGenerator(name = "rule_seq", sequenceName = "rule_seq", allocationSize = 1)
    private Long id;

    // Human-readable name — used in logs so you know which rule fired
    // Example: "high_amount_risky_merchant"
    @Column(nullable = false)
    private String name;

    /**
     * The SpEL boolean expression evaluated against the transaction.
     *
     * The expression has access to all fields of TransactionEvent:
     *   amount, cardFingerprint, merchantRiskTier, merchantCategoryCode,
     *   ipCountry, billingCountry, deviceFingerprint, ipAddress
     *
     * Examples:
     *   "amount > 500"
     *   "merchantRiskTier == 'high' and amount > 200"
     *   "merchantCategoryCode == '7995'"  (gambling MCC)
     *
     * Stored as TEXT in PostgreSQL — no length limit.
     */
    @Column(nullable = false, columnDefinition = "TEXT")
    private String expression;

    // Score added to the transaction's total if this rule matches.
    // Typical range: 10 (weak signal) to 40 (strong signal).
    @Column(nullable = false)
    private int score;

    // When false, this rule is loaded from the DB but skipped during evaluation.
    // Analysts use this to temporarily disable a rule without losing it.
    @Column(nullable = false)
    private boolean enabled;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    // Required by JPA — never call this directly in application code
    protected Rule() {}

    public Rule(String name, String expression, int score, boolean enabled) {
        this.name = name;
        this.expression = expression;
        this.score = score;
        this.enabled = enabled;
    }

    // -------------------------------------------------------------------------
    // Getters — no setters needed, rules are immutable after creation
    // -------------------------------------------------------------------------

    public Long getId() { return id; }
    public String getName() { return name; }
    public String getExpression() { return expression; }
    public int getScore() { return score; }
    public boolean isEnabled() { return enabled; }

    @Override
    public String toString() {
        return "Rule{id=" + id + ", name='" + name + "', score=" + score +
               ", enabled=" + enabled + "}";
    }
}