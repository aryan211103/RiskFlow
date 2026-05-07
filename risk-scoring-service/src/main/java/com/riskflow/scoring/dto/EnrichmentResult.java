package com.riskflow.scoring.dto;

/**
 * The response returned by the external IP reputation enrichment call.
 *
 * Reputation levels and their score contributions:
 *
 *   CLEAN      → score += 0   (no signal, do not penalize)
 *   SUSPICIOUS → score += 20  (moderate signal — VPN, proxy, unusual ASN)
 *   MALICIOUS  → score += 40  (strong signal — known fraud IP, Tor exit node)
 *
 * Source is a string identifying which data provider returned this result.
 * In production this would be the vendor name (e.g. "MaxMind", "IPQualityScore").
 * In our simulation it is always "simulated".
 *
 * This is an immutable value object — created once, never modified.
 * We use a static factory method (of()) instead of a public constructor
 * to make call sites read more naturally:
 *   EnrichmentResult.of(Reputation.CLEAN, "simulated")
 * vs
 *   new EnrichmentResult(Reputation.CLEAN, "simulated")
 */
public class EnrichmentResult {

    /**
     * The three possible reputation levels returned by the IP enrichment service.
     * Stored as an enum so the scoring logic can switch on it without
     * comparing strings (which are error-prone and not refactor-safe).
     */
    public enum Reputation {
        CLEAN,
        SUSPICIOUS,
        MALICIOUS
    }

    private final Reputation reputation;
    private final String source;

    // Private constructor — callers use the static factory method below
    private EnrichmentResult(Reputation reputation, String source) {
        this.reputation = reputation;
        this.source = source;
    }

    /**
     * Static factory method.
     * Preferred over public constructors for DTOs — clearer intent at the call site.
     */
    public static EnrichmentResult of(Reputation reputation, String source) {
        return new EnrichmentResult(reputation, source);
    }

    /**
     * Convenience factory for the fallback case — used when the circuit is open
     * or the call times out. Returns CLEAN so the pipeline is not penalized
     * for an enrichment service failure.
     */
    public static EnrichmentResult fallback() {
        return new EnrichmentResult(Reputation.CLEAN, "fallback");
    }

    /**
     * Translates the reputation level into a score contribution.
     * Called by TransactionEventConsumer when adding enrichment to the total.
     */
    public int toScore() {
        if (reputation == Reputation.MALICIOUS) {
            return 40;
        } else if (reputation == Reputation.SUSPICIOUS) {
            return 20;
        } else {
            return 0;
        }
    }

    public Reputation getReputation() { return reputation; }
    public String getSource()         { return source; }

    @Override
    public String toString() {
        return "EnrichmentResult{reputation=" + reputation + ", source='" + source + "'}";
    }
}