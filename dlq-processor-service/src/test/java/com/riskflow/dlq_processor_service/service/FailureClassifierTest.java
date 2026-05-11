package com.riskflow.dlq_processor_service.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.riskflow.dlq_processor_service.model.FailureType;

/**
 * Tests for FailureClassifier — the DLQ routing decision logic.
 *
 * Why test this?
 * FailureClassifier makes a binary decision that determines whether a failed
 * transaction gets retried or quarantined forever. A wrong classification has
 * real consequences: a misclassified TRANSIENT event gets quarantined and
 * never processed. A misclassified POISON_PILL gets retried 3 times, wastes
 * resources, and still ends up quarantined.
 *
 * The logic is pure Java — no Spring, no database, no Kafka.
 * These tests are the fastest in the entire codebase.
 */
class FailureClassifierTest {

    // The class under test. Instantiated directly — no Spring needed.
    private FailureClassifier classifier;

    // @BeforeEach runs before every single test method.
    // Gives each test a fresh classifier instance — no shared state.
    @BeforeEach
    void setUp() {
        classifier = new FailureClassifier();
    }

    // =========================================================================
    // TRANSIENT KEYWORD TESTS
    // Each test checks one keyword from the TRANSIENT_KEYWORDS array.
    // If a keyword is removed or misspelled in the source, the test fails.
    // =========================================================================

    @Test
    void classify_connectionError_returnsTransient() {
        // "connection" is a transient keyword — database or Redis connectivity issue
        FailureType result = classifier.classify("Could not open connection to database");
        assertEquals(FailureType.TRANSIENT, result);
    }

    @Test
    void classify_timeoutError_returnsTransient() {
        // "timeout" — network or query timed out, likely temporary
        FailureType result = classifier.classify("Query execution timeout after 30s");
        assertEquals(FailureType.TRANSIENT, result);
    }

    @Test
    void classify_unavailableError_returnsTransient() {
        // "unavailable" — service temporarily down
        FailureType result = classifier.classify("IP reputation service unavailable");
        assertEquals(FailureType.TRANSIENT, result);
    }

    @Test
    void classify_refusedError_returnsTransient() {
        // "refused" — service restarting or not yet up
        FailureType result = classifier.classify("Connection refused at localhost:5432");
        assertEquals(FailureType.TRANSIENT, result);
    }

    @Test
    void classify_redisError_returnsTransient() {
        // "redis" — Redis-specific connectivity or command failure
        FailureType result = classifier.classify("Redis command failed: NOAUTH");
        assertEquals(FailureType.TRANSIENT, result);
    }

    @Test
    void classify_datasourceError_returnsTransient() {
        // "datasource" — database pool exhausted
        FailureType result = classifier.classify("Unable to acquire connection from datasource");
        assertEquals(FailureType.TRANSIENT, result);
    }

    @Test
    void classify_couldNotExecuteError_returnsTransient() {
        // "could not execute" — Hibernate could not reach the DB
        FailureType result = classifier.classify("could not execute statement");
        assertEquals(FailureType.TRANSIENT, result);
    }

    // =========================================================================
    // POISON PILL KEYWORD TESTS
    // Each test checks one keyword from the POISON_PILL_KEYWORDS array.
    // These errors indicate broken data — retrying will never help.
    // =========================================================================

    @Test
    void classify_nullPointerError_returnsPoisonPill() {
        // "nullpointer" — missing field in the payload caused a NPE
        FailureType result = classifier.classify("NullPointerException: cardFingerprint is null");
        assertEquals(FailureType.POISON_PILL, result);
    }

    @Test
    void classify_numberFormatError_returnsPoisonPill() {
        // "numberformat" — amount field is malformed (e.g. "abc" instead of "250")
        FailureType result = classifier.classify("NumberFormatException: For input string: 'abc'");
        assertEquals(FailureType.POISON_PILL, result);
    }

    @Test
    void classify_illegalArgumentError_returnsPoisonPill() {
        // "illegal argument" — invalid value passed to a method
        FailureType result = classifier.classify("Illegal argument: missing required field 'amount'");
        assertEquals(FailureType.POISON_PILL, result);
    }

    @Test
    void classify_couldNotDeserializeError_returnsPoisonPill() {
        // "could not deserialize" — broken payload format
        FailureType result = classifier.classify("could not deserialize JSON payload");
        assertEquals(FailureType.POISON_PILL, result);
    }

    @Test
    void classify_constraintViolationError_returnsPoisonPill() {
        // "constraint violation" — data violates a DB constraint
        FailureType result = classifier.classify("constraint violation: transactionId already exists");
        assertEquals(FailureType.POISON_PILL, result);
    }

    // =========================================================================
    // EDGE CASE TESTS
    // Tests for null, blank, unknown, and case-insensitivity behavior.
    // =========================================================================

    @Test
    void classify_nullErrorMessage_returnsPoisonPill() {
        // Null error message cannot be classified — default to POISON_PILL
        // to avoid infinite retry loops on unknown failures.
        FailureType result = classifier.classify(null);
        assertEquals(FailureType.POISON_PILL, result,
            "Null error message should default to POISON_PILL");
    }

    @Test
    void classify_blankErrorMessage_returnsPoisonPill() {
        // Blank message same as null — cannot classify
        FailureType result = classifier.classify("   ");
        assertEquals(FailureType.POISON_PILL, result,
            "Blank error message should default to POISON_PILL");
    }

    @Test
    void classify_unknownErrorMessage_defaultsToPoisonPill() {
        // An error message that matches no known keyword.
        // Better to quarantine once than retry forever on unknown failures.
        FailureType result = classifier.classify("Something completely unexpected happened");
        assertEquals(FailureType.POISON_PILL, result,
            "Unknown error should default to POISON_PILL");
    }

    @Test
    void classify_uppercaseKeyword_matchesCaseInsensitively() {
        // The classifier normalizes to lowercase before matching.
        // "TIMEOUT" in the error message should still match "timeout".
        FailureType result = classifier.classify("TIMEOUT waiting for database response");
        assertEquals(FailureType.TRANSIENT, result,
            "Keyword matching should be case-insensitive");
    }

    @Test
    void classify_mixedCaseKeyword_matchesCaseInsensitively() {
        // Mixed case check — "NullPointer" contains "nullpointer" when lowercased
        FailureType result = classifier.classify("NullPointer in scoring pipeline");
        assertEquals(FailureType.POISON_PILL, result,
            "Mixed-case keyword should still classify correctly");
    }

    @Test
    void classify_transientKeywordTakesPrecedenceOverPoisonPill() {
        // If an error message contains BOTH a transient keyword AND a poison
        // pill keyword, transient is checked first in the source code.
        // This test locks in that precedence so a future refactor cannot
        // silently change the behavior.
        //
        // "connection" is transient, "nullpointer" is poison pill
        FailureType result = classifier.classify("NullPointerException on connection setup");
        assertEquals(FailureType.TRANSIENT, result,
            "Transient keywords are checked before poison pill keywords");
    }

    @Test
    void classify_emptyString_returnsPoisonPill() {
        // Empty string is treated like blank — cannot classify
        FailureType result = classifier.classify("");
        assertEquals(FailureType.POISON_PILL, result);
    }
}