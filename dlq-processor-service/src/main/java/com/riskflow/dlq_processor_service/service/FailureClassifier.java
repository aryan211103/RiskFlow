package com.riskflow.dlq_processor_service.service;

import org.springframework.stereotype.Component;

import com.riskflow.dlq_processor_service.model.FailureType;

// Classifies a failure as either TRANSIENT or POISON_PILL
// based on the error message from the scoring service.
//
// This is the decision point that determines what happens next:
// TRANSIENT   → retry with a delay, likely to succeed
// POISON_PILL → quarantine immediately, will never succeed
//
// The classification logic is intentionally simple and rule-based.
// In a production system this could be extended with more error types,
// regex matching, or even a small lookup table in PostgreSQL.
@Component
public class FailureClassifier {

    // These keywords indicate infrastructure problems — temporary issues
    // that are likely to resolve on their own. Worth retrying.
    private static final String[] TRANSIENT_KEYWORDS = {
        "connection",       // database or redis connection failure
        "timeout",          // network or query timeout
        "unavailable",      // service temporarily unavailable
        "refused",          // connection refused — service restarting
        "redis",            // Redis-specific errors
        "datasource",       // database pool exhausted or down
        "could not execute" // Hibernate could not reach the database
    };

    // These keywords indicate problems with the message itself — bad data
    // that will cause the same failure no matter how many times we retry.
    private static final String[] POISON_PILL_KEYWORDS = {
        "nullpointer",          // null field in the payload
        "numberformat",         // amount or other number field is malformed
        "illegal argument",     // invalid value passed to a method
        "could not deserialize", // payload format is broken
        "constraint violation"  // data violates a database constraint
    };

    // Takes the error message string and returns the failure type.
    // Checks transient keywords first — if any match, it is transient.
    // Then checks poison pill keywords — if any match, it is a poison pill.
    // If nothing matches, we default to POISON_PILL to be safe.
    // It is better to quarantine an unknown failure than to retry forever.
    public FailureType classify(String errorMessage) {

        if (errorMessage == null || errorMessage.isBlank()) {
            // No error message means we cannot classify — quarantine to be safe
            return FailureType.POISON_PILL;
        }

        // Normalize to lowercase so matching is case-insensitive
        String normalized = errorMessage.toLowerCase();

        for (String keyword : TRANSIENT_KEYWORDS) {
            if (normalized.contains(keyword)) {
                return FailureType.TRANSIENT;
            }
        }

        for (String keyword : POISON_PILL_KEYWORDS) {
            if (normalized.contains(keyword)) {
                return FailureType.POISON_PILL;
            }
        }

        // Unknown error type — default to POISON_PILL to avoid infinite retries
        return FailureType.POISON_PILL;
    }
}