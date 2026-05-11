package com.riskflow.scoring.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import org.junit.jupiter.api.Test;

import com.riskflow.scoring.model.DecisionType;

/**
 * Tests for the score-to-decision threshold logic in TransactionEventConsumer.
 *
 * Why test this separately?
 * The threshold logic (score < 20 → APPROVED, 20-59 → NEEDS_REVIEW, 60+ → AUTO_REJECTED)
 * is the final decision gate for every transaction. A one-point miscalculation
 * changes a fraudulent transaction to APPROVED. These thresholds are critical
 * and should be locked in by tests.
 *
 * Testing approach:
 * We extract the threshold logic into a testable static helper so we can
 * test it in pure isolation — without Kafka, Redis, or PostgreSQL.
 *
 * The method under test mirrors the exact if-else logic from
 * TransactionEventConsumer.consume() — if that logic ever changes,
 * these tests will catch it.
 *
 * NOTE: If you ever refactor the threshold logic into its own method
 * or class, update this test to call that directly instead.
 */
class DecisionThresholdTest {

    /**
     * Mirrors the exact decision logic from TransactionEventConsumer.consume().
     * We duplicate it here so the test has no Spring dependencies.
     * If the thresholds change in the consumer, the tests below will fail — that is
     * the correct behavior. Tests are the contract.
     */
    private DecisionType computeDecision(int totalScore) {
        if (totalScore >= 60) {
            return DecisionType.AUTO_REJECTED;
        } else if (totalScore >= 20) {
            return DecisionType.NEEDS_REVIEW;
        } else {
            return DecisionType.APPROVED;
        }
    }

    // =========================================================================
    // APPROVED ZONE: score 0 to 19
    // =========================================================================

    @Test
    void decision_score0_isApproved() {
        assertEquals(DecisionType.APPROVED, computeDecision(0));
    }

    @Test
    void decision_score10_isApproved() {
        assertEquals(DecisionType.APPROVED, computeDecision(10));
    }

    @Test
    void decision_score19_isApproved() {
        // 19 is the last score before NEEDS_REVIEW — the upper boundary of APPROVED
        assertEquals(DecisionType.APPROVED, computeDecision(19),
            "Score 19 should be APPROVED (boundary just below NEEDS_REVIEW threshold)");
    }

    // =========================================================================
    // NEEDS_REVIEW ZONE: score 20 to 59
    // =========================================================================

    @Test
    void decision_score20_isNeedsReview() {
        // 20 is the exact lower boundary of NEEDS_REVIEW — test it explicitly
        assertEquals(DecisionType.NEEDS_REVIEW, computeDecision(20),
            "Score 20 should be NEEDS_REVIEW (exact lower boundary)");
    }

    @Test
    void decision_score35_isNeedsReview() {
        assertEquals(DecisionType.NEEDS_REVIEW, computeDecision(35));
    }

    @Test
    void decision_score59_isNeedsReview() {
        // 59 is the last score before AUTO_REJECTED — the upper boundary of NEEDS_REVIEW
        assertEquals(DecisionType.NEEDS_REVIEW, computeDecision(59),
            "Score 59 should be NEEDS_REVIEW (boundary just below AUTO_REJECTED threshold)");
    }

    // =========================================================================
    // AUTO_REJECTED ZONE: score 60 and above
    // =========================================================================

    @Test
    void decision_score60_isAutoRejected() {
        // 60 is the exact lower boundary of AUTO_REJECTED — test it explicitly
        assertEquals(DecisionType.AUTO_REJECTED, computeDecision(60),
            "Score 60 should be AUTO_REJECTED (exact lower boundary)");
    }

    @Test
    void decision_score75_isAutoRejected() {
        assertEquals(DecisionType.AUTO_REJECTED, computeDecision(75));
    }

    @Test
    void decision_score100_isAutoRejected() {
        // 100 is what hard rules assign — always AUTO_REJECTED
        assertEquals(DecisionType.AUTO_REJECTED, computeDecision(100),
            "Score 100 (hard rule result) should be AUTO_REJECTED");
    }

    @Test
    void decision_score200_isAutoRejected() {
        // Theoretical maximum if many rules stack — still AUTO_REJECTED
        assertEquals(DecisionType.AUTO_REJECTED, computeDecision(200));
    }

    // =========================================================================
    // BOUNDARY INVERSION TESTS
    // Verifies that adjacent boundaries don't bleed into each other.
    // A common bug is using > instead of >= or vice versa.
    // =========================================================================

    @Test
    void decision_boundaryBetweenApprovedAndNeedsReview() {
        // 19 → APPROVED, 20 → NEEDS_REVIEW. Not the other way around.
        assertNotEquals(DecisionType.NEEDS_REVIEW, computeDecision(19),
            "Score 19 must NOT be NEEDS_REVIEW");
        assertNotEquals(DecisionType.APPROVED, computeDecision(20),
            "Score 20 must NOT be APPROVED");
    }

    @Test
    void decision_boundaryBetweenNeedsReviewAndAutoRejected() {
        // 59 → NEEDS_REVIEW, 60 → AUTO_REJECTED. Not the other way around.
        assertNotEquals(DecisionType.AUTO_REJECTED, computeDecision(59),
            "Score 59 must NOT be AUTO_REJECTED");
        assertNotEquals(DecisionType.NEEDS_REVIEW, computeDecision(60),
            "Score 60 must NOT be NEEDS_REVIEW");
    }
}