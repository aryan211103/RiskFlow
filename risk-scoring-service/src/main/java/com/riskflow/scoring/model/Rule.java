package com.riskflow.scoring.model;

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
 * Represents a single scoring rule stored in PostgreSQL.
 *
 * Each rule has:
 *   - A human-readable name for logging and analyst UI
 *   - A SpEL expression evaluated against the transaction
 *   - A score contribution added to the total if the expression matches
 *   - An enabled flag so analysts can disable rules without deleting them
 *   - An optional groupName that places this rule into a logical group
 *   - An optional matchMode (ALL or ANY) that controls how the group fires
 *
 * --- UNGROUPED RULES (groupName is null) ---
 * Evaluated independently, exactly as before Phase 9.
 * If the expression matches, the rule's score is added directly to the total.
 *
 * --- GROUPED RULES (groupName is not null) ---
 * All rules sharing the same groupName are evaluated as a unit.
 * The group's score is taken from whichever member rule has score > 0.
 * Signal-carrier members (the ones with boolean expressions) have score = 0.
 *
 * Example group "coordinated_fraud" with matchMode ALL:
 *   - cf_high_amount:       amount > 300             score=0
 *   - cf_high_risk_merchant: merchantRiskTier=='high' score=0
 *   - cf_country_mismatch:  ipCountry!=billingCountry score=0
 *   - cf_score:             true                     score=60
 *
 * Score 60 applies only when ALL three signal rules match simultaneously.
 * If only two match, the group contributes nothing.
 */
@Entity
@Table(name = "rules")
public class Rule {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "rule_seq")
    @SequenceGenerator(name = "rule_seq", sequenceName = "rule_seq", allocationSize = 1)
    private Long id;

    // Human-readable name — used in logs so you know which rule fired
    @Column(nullable = false)
    private String name;

    /**
     * The SpEL boolean expression evaluated against the transaction.
     *
     * Has access to all fields of TransactionEvent via getters:
     *   amount, cardFingerprint, merchantRiskTier, merchantCategoryCode,
     *   ipCountry, billingCountry, deviceFingerprint, ipAddress
     *
     * Also supports compound boolean logic natively in SpEL:
     *   "amount > 300 && merchantRiskTier == 'high' && ipCountry != billingCountry"
     *   "merchantCategoryCode == '7995' || merchantCategoryCode == '7993'"
     *
     * Signal-carrier rules in a group use "true" as their expression
     * because the group's matchMode does the combining — the individual
     * expressions just need to evaluate cleanly.
     *
     * Stored as TEXT — no length limit.
     */
    @Column(nullable = false, columnDefinition = "TEXT")
    private String expression;

    // Score added to the transaction total if this rule (or its group) matches.
    // Signal-carrier rules inside a group have score = 0.
    // The designated score-bearer rule in the group has the actual score.
    @Column(nullable = false)
    private int score;

    // When false, this rule is skipped during evaluation.
    @Column(nullable = false)
    private boolean enabled;

    /**
     * Optional group identifier.
     *
     * Rules sharing the same groupName are evaluated together as a unit.
     * Null means this is an ungrouped rule — evaluated independently.
     *
     * Naming convention: use snake_case, e.g. "coordinated_fraud", "velocity_burst"
     */
    @Column(nullable = true)
    private String groupName;

    /**
     * Controls how the group fires. Only meaningful when groupName is set.
     *
     * ALL → every member rule must match (logical AND)
     * ANY → at least one member rule must match (logical OR)
     *
     * @Enumerated(EnumType.STRING) stores "ALL" or "ANY" as text in PostgreSQL,
     * not as an integer ordinal. This is safer for long-term maintainability.
     */
    @Enumerated(EnumType.STRING)
    @Column(nullable = true)
    private MatchMode matchMode;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    // Required by JPA — never call this directly in application code
    protected Rule() {}

    // Constructor for ungrouped rules (backward compatible)
    public Rule(String name, String expression, int score, boolean enabled) {
        this.name = name;
        this.expression = expression;
        this.score = score;
        this.enabled = enabled;
    }

    // Constructor for grouped rules
    public Rule(String name, String expression, int score, boolean enabled,
                String groupName, MatchMode matchMode) {
        this.name = name;
        this.expression = expression;
        this.score = score;
        this.enabled = enabled;
        this.groupName = groupName;
        this.matchMode = matchMode;
    }

    // -------------------------------------------------------------------------
    // Getters
    // -------------------------------------------------------------------------

    public Long getId() { return id; }
    public String getName() { return name; }
    public String getExpression() { return expression; }
    public int getScore() { return score; }
    public boolean isEnabled() { return enabled; }
    public String getGroupName() { return groupName; }
    public MatchMode getMatchMode() { return matchMode; }

    @Override
    public String toString() {
        return "Rule{id=" + id + ", name='" + name + "', score=" + score +
               ", enabled=" + enabled + ", groupName='" + groupName +
               "', matchMode=" + matchMode + "}";
    }
}