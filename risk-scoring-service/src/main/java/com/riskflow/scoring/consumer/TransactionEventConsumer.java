package com.riskflow.scoring.consumer;

import java.time.Duration;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.riskflow.scoring.dto.TransactionEvent;
import com.riskflow.scoring.model.DecisionType;
import com.riskflow.scoring.model.RiskDecision;
import com.riskflow.scoring.repository.RiskDecisionRepository;

/**
 * Kafka consumer for the transaction.received topic.
 *
 * This is the entry point for all risk scoring work. Every transaction
 * published by the ingestion service eventually lands here.
 *
 * Responsibilities in Phase 4b:
 *   1. Parse the raw pipe-delimited payload into a TransactionEvent
 *   2. Idempotency check via Redis — skip if already processed
 *   3. Stage 1 hard rules — blocklist, amount cap, geographic block
 *   4. Write RiskDecision to PostgreSQL
 *
 * Responsibilities added in later phases:
 *   Phase 4c: Stage 2 behavioral velocity analytics (Redis sliding windows)
 *   Phase 5:  Stage 3 rule engine (hot-reloadable SpEL rules from PostgreSQL)
 *
 * @Component registers this class as a Spring bean so it gets picked up
 * by component scanning and the @KafkaListener annotation is activated.
 */
@Component
public class TransactionEventConsumer {

    private static final Logger log =
        LoggerFactory.getLogger(TransactionEventConsumer.class);

    // -----------------------------------------------------------------------
    // Hard rule constants
    // -----------------------------------------------------------------------

    /**
     * Maximum allowed transaction amount in USD.
     * Any transaction above this is AUTO_REJECTED immediately, no further
     * analysis needed. In production this would be loaded from config,
     * but a constant is correct for Phase 4b.
     */
    private static final double HARD_CAP_AMOUNT = 10_000.0;

    /**
     * Countries under sanctions or with extremely high fraud rates.
     * Transactions originating from IPs in these countries are AUTO_REJECTED.
     * RU = Russia, KP = North Korea, IR = Iran, BY = Belarus, SY = Syria.
     *
     * In production this list would be externalized to a config file or
     * database table, but a hardcoded Set is correct for Phase 4b.
     */
    private static final Set<String> SANCTIONED_COUNTRIES =
        Set.of("RU", "KP", "IR", "BY", "SY");

    /**
     * Simulated blocklist of known fraudulent card fingerprints.
     * In Phase 5, this will be replaced by a database lookup.
     * For Phase 4b, a hardcoded set is sufficient to prove the logic works.
     */
    private static final Set<String> BLOCKLISTED_CARDS =
        Set.of("fpr_BLOCKED_001", "fpr_BLOCKED_002", "fpr_BLOCKED_003");

    /**
     * Redis key prefix for idempotency markers.
     * Full key format: risk:processed:{transactionId}
     * TTL: 24 hours (86400 seconds)
     *
     * We covered why this pattern works atomically in the Redis concepts
     * section — SET NX EX is a single command, safe under concurrent consumers.
     */
    private static final String IDEMPOTENCY_KEY_PREFIX = "risk:processed:";
    private static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);

    // -----------------------------------------------------------------------
    // Dependencies — injected by Spring via constructor injection
    // -----------------------------------------------------------------------

    /**
     * Why constructor injection instead of @Autowired on fields?
     * 1. Testability: you can create this class in a unit test by just
     *    passing mock objects to the constructor. No Spring context needed.
     * 2. Immutability: final fields can't be accidentally reassigned.
     * 3. Fail-fast: if a dependency is missing, the app won't start.
     *    With field injection, the app starts and crashes only when the
     *    method is first called.
     */
    private final RiskDecisionRepository decisionRepository;
    private final StringRedisTemplate redis;

    public TransactionEventConsumer(RiskDecisionRepository decisionRepository,
                                     StringRedisTemplate redis) {
        this.decisionRepository = decisionRepository;
        this.redis = redis;
    }

    // -----------------------------------------------------------------------
    // Kafka listener
    // -----------------------------------------------------------------------

    /**
     * Entry point for every message on the transaction.received topic.
     *
     * @KafkaListener wires this method to the topic. Spring Kafka runs a
     * background polling loop and calls this method for each message.
     *
     * Parameters:
     *   @Payload String raw       — the message value (our pipe-delimited string)
     *   @Header partition         — which Kafka partition this message came from
     *   @Header offset            — the message's position within that partition
     *
     * The partition and offset aren't used in business logic — they're
     * included purely for logging, so we can trace a decision back to an
     * exact Kafka message if we ever need to replay or debug.
     *
     * groupId is NOT set here — it's already in application.properties as
     * spring.kafka.consumer.group-id=risk-scoring-service. Repeating it here
     * would be redundant.
     */
    @KafkaListener(topics = "transaction.received")
    public void consume(
            @Payload String raw,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.debug("Received message from partition={} offset={}", partition, offset);

        // -------------------------------------------------------------------
        // Step 1: Parse the raw payload
        // -------------------------------------------------------------------
        // If the payload is malformed (wrong number of fields, non-numeric
        // amount), TransactionEvent.from() throws IllegalArgumentException.
        // We catch it, log it, and return — there's nothing we can do with
        // an unparseable message, and we don't want to block the consumer.
        // In Phase 7 (DLQ Processor), we'd publish this to a dead-letter topic.
        TransactionEvent event;
        try {
            event = TransactionEvent.from(raw);
        } catch (IllegalArgumentException e) {
            log.error("Failed to parse transaction payload. Skipping. raw={} error={}",
                raw, e.getMessage());
            return;
        }

        String txnId = event.getTransactionId();
        log.info("Processing transaction txnId={} amount={} ipCountry={} card={}",
            txnId, event.getAmount(), event.getIpCountry(), event.getCardFingerprint());

        // -------------------------------------------------------------------
        // Step 2: Idempotency check
        // -------------------------------------------------------------------
        // setIfAbsent() maps to Redis SET ... NX EX — atomic, single command.
        // Returns Boolean.TRUE if we successfully set the key (first time).
        // Returns Boolean.FALSE if the key already existed (duplicate message).
        //
        // We pass "1" as the value — the actual value doesn't matter for
        // idempotency, we only care whether the key exists.
        String idempotencyKey = IDEMPOTENCY_KEY_PREFIX + txnId;
        Boolean claimed = redis.opsForValue()
            .setIfAbsent(idempotencyKey, "1", IDEMPOTENCY_TTL);

        if (!Boolean.TRUE.equals(claimed)) {
            log.info("Duplicate message detected. Already processed txnId={}. Skipping.", txnId);
            return;
        }

        log.debug("Idempotency claim successful for txnId={}", txnId);

        // -------------------------------------------------------------------
        // Step 3: Stage 1 — Hard Rules
        // -------------------------------------------------------------------
        // Hard rules are short-circuit: the FIRST rule that fires ends
        // scoring immediately. We don't accumulate scores here — any hit
        // means AUTO_REJECTED with score=100.
        //
        // The order matters slightly: blocklist first (most specific),
        // then amount cap (easiest arithmetic check),
        // then geographic block (string lookup).

        // Rule 1: Blocklisted card fingerprint
        if (BLOCKLISTED_CARDS.contains(event.getCardFingerprint())) {
            log.warn("HARD_RULE: Blocklisted card. txnId={} card={}",
                txnId, event.getCardFingerprint());
            saveDecision(txnId, DecisionType.AUTO_REJECTED, 100,
                "BLOCKLIST_HIT", "HARD_RULES");
            return;
        }

        // Rule 2: Amount above hard cap
        if (event.getAmount() > HARD_CAP_AMOUNT) {
            log.warn("HARD_RULE: Amount exceeds cap. txnId={} amount={} cap={}",
                txnId, event.getAmount(), HARD_CAP_AMOUNT);
            saveDecision(txnId, DecisionType.AUTO_REJECTED, 100,
                "AMOUNT_EXCEEDS_CAP", "HARD_RULES");
            return;
        }

        // Rule 3: Sanctioned country
        if (SANCTIONED_COUNTRIES.contains(event.getIpCountry())) {
            log.warn("HARD_RULE: Sanctioned country. txnId={} ipCountry={}",
                txnId, event.getIpCountry());
            saveDecision(txnId, DecisionType.AUTO_REJECTED, 100,
                "SANCTIONED_COUNTRY", "HARD_RULES");
            return;
        }

        // -------------------------------------------------------------------
        // Step 4: No hard rules triggered — APPROVED for now
        // -------------------------------------------------------------------
        // In Phase 4c, this is where behavioral velocity analytics will run.
        // In Phase 5, this is where the rule engine will run.
        // For Phase 4b, anything that passes hard rules is APPROVED with
        // a score of 0.
        log.info("No hard rules triggered. Approving txnId={}", txnId);
        saveDecision(txnId, DecisionType.APPROVED, 0,
            "PASSED_HARD_RULES", "HARD_RULES");
    }

    // -----------------------------------------------------------------------
    // Private helper
    // -----------------------------------------------------------------------

    /**
     * Builds a RiskDecision and persists it to PostgreSQL.
     *
     * Extracted as a helper method to avoid repeating the same
     * build-and-save pattern in every hard rule branch above.
     * Every code path that produces a decision goes through here.
     */
    private void saveDecision(String transactionId,
                               DecisionType decision,
                               int score,
                               String reason,
                               String stage) {
        RiskDecision rd = new RiskDecision(transactionId, decision, score, reason, stage);
        decisionRepository.save(rd);
        log.info("Decision saved. txnId={} decision={} score={} reason={}",
            transactionId, decision, score, reason);
    }
}