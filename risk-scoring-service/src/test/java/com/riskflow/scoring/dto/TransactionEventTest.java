package com.riskflow.scoring.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

/**
 * Tests for TransactionEvent.from() — the Kafka payload parser.
 *
 * Why test this?
 * This parser is the entry point for every transaction that flows through
 * the scoring pipeline. A bug here means corrupted data in every downstream
 * stage. It has real branching logic: key:value splitting, required field
 * validation, optional field defaults, and type parsing (amount as double).
 * All of that is worth testing independently of Kafka or Spring.
 *
 * No Spring context needed — TransactionEvent is a plain Java class.
 * These tests run instantly with zero infrastructure.
 */
class TransactionEventTest {

    // -------------------------------------------------------------------------
    // Helper: builds a valid pipe-delimited payload string.
    // Used as the baseline for most tests — individual tests mutate it
    // to test specific edge cases.
    // -------------------------------------------------------------------------
    private String validPayload() {
        return "transactionId:txn_abc123" +
               "|userId:user_001" +
               "|cardFingerprint:fpr_xyz" +
               "|amount:250" +
               "|currency:USD" +
               "|merchantId:mch_001" +
               "|merchantCategoryCode:5412" +
               "|merchantRiskTier:low" +
               "|deviceFingerprint:dev_001" +
               "|ipAddress:192.168.1.1" +
               "|ipCountry:US" +
               "|billingCountry:US";
    }

    // -------------------------------------------------------------------------
    // TEST 1: Happy path — all 12 required fields parse correctly.
    //
    // Why: Confirms the baseline case works before testing edge cases.
    // If this fails, nothing else matters.
    // -------------------------------------------------------------------------
    @Test
    void from_validPayload_parsesAllFieldsCorrectly() {
        TransactionEvent event = TransactionEvent.from(validPayload());

        // Each assertion checks one field in isolation.
        // We verify both string fields and the double parsing of amount.
        assertEquals("txn_abc123", event.getTransactionId());
        assertEquals("user_001", event.getUserId());
        assertEquals("fpr_xyz", event.getCardFingerprint());
        assertEquals(250.0, event.getAmount(), 0.001); // delta for double comparison
        assertEquals("USD", event.getCurrency());
        assertEquals("mch_001", event.getMerchantId());
        assertEquals("5412", event.getMerchantCategoryCode());
        assertEquals("low", event.getMerchantRiskTier());
        assertEquals("dev_001", event.getDeviceFingerprint());
        assertEquals("192.168.1.1", event.getIpAddress());
        assertEquals("US", event.getIpCountry());
        assertEquals("US", event.getBillingCountry());
    }

    // -------------------------------------------------------------------------
    // TEST 2: Optional fields default to empty string when absent.
    //
    // Why: The ingestion service does NOT include userAgent or createdAt
    // in the current payload (documented in master prompt). The parser
    // must handle their absence gracefully — not throw an exception.
    // The getOrDefault in TransactionEvent.from() handles this, but we
    // verify that contract is upheld.
    // -------------------------------------------------------------------------
    @Test
    void from_missingOptionalFields_defaultsToEmptyString() {
        // validPayload() does not include userAgent or createdAt
        TransactionEvent event = TransactionEvent.from(validPayload());

        assertEquals("", event.getUserAgent(),
            "userAgent should default to empty string when absent");
        assertEquals("", event.getCreatedAt(),
            "createdAt should default to empty string when absent");
    }

    // -------------------------------------------------------------------------
    // TEST 3: Missing required field throws IllegalArgumentException.
    //
    // Why: The parser has explicit required-field validation. If a required
    // field is missing, we want a loud, immediate failure — not a NullPointerException
    // buried in stage 1 or 2 logic. This test verifies that contract.
    //
    // We test with 'amount' missing because it's also a double parse —
    // tests two failure modes at once (missing AND type-parse protection).
    // -------------------------------------------------------------------------
    @Test
    void from_missingRequiredField_throwsIllegalArgumentException() {
        // Build a payload without 'amount'
        String payloadWithoutAmount =
               "transactionId:txn_abc123" +
               "|userId:user_001" +
               "|cardFingerprint:fpr_xyz" +
               "|currency:USD" +
               "|merchantId:mch_001" +
               "|merchantCategoryCode:5412" +
               "|merchantRiskTier:low" +
               "|deviceFingerprint:dev_001" +
               "|ipAddress:192.168.1.1" +
               "|ipCountry:US" +
               "|billingCountry:US";

        // assertThrows verifies that the exact exception type is thrown.
        // If from() returns normally instead of throwing, this test fails.
        assertThrows(IllegalArgumentException.class,
            () -> TransactionEvent.from(payloadWithoutAmount),
            "Parser should throw when a required field is missing");
    }

    // -------------------------------------------------------------------------
    // TEST 4: Field reordering is handled correctly.
    //
    // Why: The parser uses key:value pairs, not positional splitting.
    // This means "amount:250|currency:USD" and "currency:USD|amount:250"
    // should produce identical results. This is the key resilience property
    // of the current parser design — worth an explicit test.
    // -------------------------------------------------------------------------
    @Test
    void from_fieldsInDifferentOrder_parsesCorrectly() {
        // Same fields as validPayload() but in a different order
        String reorderedPayload =
               "ipAddress:10.0.0.1" +
               "|billingCountry:CA" +
               "|amount:999" +
               "|transactionId:txn_reorder" +
               "|merchantCategoryCode:7995" +
               "|userId:user_reorder" +
               "|currency:CAD" +
               "|ipCountry:CA" +
               "|deviceFingerprint:dev_reorder" +
               "|merchantId:mch_reorder" +
               "|merchantRiskTier:high" +
               "|cardFingerprint:fpr_reorder";

        TransactionEvent event = TransactionEvent.from(reorderedPayload);

        // Spot-check key fields — if reordering broke parsing, these will be wrong
        assertEquals("txn_reorder", event.getTransactionId());
        assertEquals(999.0, event.getAmount(), 0.001);
        assertEquals("10.0.0.1", event.getIpAddress());
        assertEquals("CA", event.getBillingCountry());
        assertEquals("7995", event.getMerchantCategoryCode());
    }

    // -------------------------------------------------------------------------
    // TEST 5: Amount field with a decimal value parses as a double correctly.
    //
    // Why: The ingestion service formats amount as an integer (%d) in the
    // current payload. But the parser calls Double.parseDouble() which handles
    // both "250" and "250.99". We test the decimal case to ensure no
    // regression if the ingestion service ever sends fractional amounts.
    // -------------------------------------------------------------------------
    @Test
    void from_decimalAmount_parsesCorrectly() {
        // Replace amount:250 with a decimal value
        String payload = validPayload().replace("amount:250", "amount:149.99");
        TransactionEvent event = TransactionEvent.from(payload);

        assertEquals(149.99, event.getAmount(), 0.001);
    }

    // -------------------------------------------------------------------------
    // TEST 6: Missing transactionId throws immediately.
    //
    // Why: transactionId is the primary key used throughout the pipeline
    // for idempotency, DLQ routing, and audit. If it's missing, the message
    // is fundamentally broken. We test it specifically (not just "a required
    // field") because of its critical role.
    // -------------------------------------------------------------------------
    @Test
    void from_missingTransactionId_throwsIllegalArgumentException() {
        String payload =
               "userId:user_001" +
               "|cardFingerprint:fpr_xyz" +
               "|amount:250" +
               "|currency:USD" +
               "|merchantId:mch_001" +
               "|merchantCategoryCode:5412" +
               "|merchantRiskTier:low" +
               "|deviceFingerprint:dev_001" +
               "|ipAddress:192.168.1.1" +
               "|ipCountry:US" +
               "|billingCountry:US";

        assertThrows(IllegalArgumentException.class,
            () -> TransactionEvent.from(payload));
    }

    // -------------------------------------------------------------------------
    // TEST 7: Completely empty string throws.
    //
    // Why: Belt-and-suspenders. A Kafka consumer receiving an empty message
    // should fail fast and route to DLQ, not produce a NullPointerException
    // somewhere in the pipeline.
    // -------------------------------------------------------------------------
    @Test
    void from_emptyString_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
            () -> TransactionEvent.from(""));
    }
}