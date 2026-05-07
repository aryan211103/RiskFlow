package com.riskflow.scoring.consumer;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.riskflow.scoring.dto.EnrichmentResult;
import com.riskflow.scoring.dto.TransactionEvent;
import com.riskflow.scoring.model.DecisionType;
import com.riskflow.scoring.model.RiskDecision;
import com.riskflow.scoring.repository.RiskDecisionRepository;
import com.riskflow.scoring.service.BehavioralAnalyzer;
import com.riskflow.scoring.service.ExternalEnrichmentService;
import com.riskflow.scoring.service.RuleEngine;

@Component
public class TransactionEventConsumer {

    private static final Logger log =
        LoggerFactory.getLogger(TransactionEventConsumer.class);

    private static final double HARD_CAP_AMOUNT = 10_000.0;

    private static final Set<String> SANCTIONED_COUNTRIES =
        Set.of("RU", "KP", "IR", "BY", "SY");

    private static final Set<String> BLOCKLISTED_CARDS =
        Set.of("fpr_BLOCKED_001", "fpr_BLOCKED_002", "fpr_BLOCKED_003");

    private static final String IDEMPOTENCY_KEY_PREFIX = "risk:processed:";
    private static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);

    private static final String DLQ_TOPIC = "transaction.scoring.failed";

    private final RiskDecisionRepository decisionRepository;
    private final StringRedisTemplate redis;
    private final BehavioralAnalyzer behavioralAnalyzer;
    private final RuleEngine ruleEngine;
    private final KafkaTemplate<String, String> kafkaTemplate;

    // Phase 10: injected enrichment service — wraps external call with Resilience4j
    private final ExternalEnrichmentService enrichmentService;

    public TransactionEventConsumer(RiskDecisionRepository decisionRepository,
                                    StringRedisTemplate redis,
                                    BehavioralAnalyzer behavioralAnalyzer,
                                    RuleEngine ruleEngine,
                                    KafkaTemplate<String, String> kafkaTemplate,
                                    ExternalEnrichmentService enrichmentService) {
        this.decisionRepository = decisionRepository;
        this.redis = redis;
        this.behavioralAnalyzer = behavioralAnalyzer;
        this.ruleEngine = ruleEngine;
        this.kafkaTemplate = kafkaTemplate;
        this.enrichmentService = enrichmentService;
    }

    @KafkaListener(topics = "transaction.received")
    public void consume(
            @Payload String raw,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.debug("Received message from partition={} offset={}", partition, offset);

        TransactionEvent event;
        try {
            event = TransactionEvent.from(raw);
        } catch (IllegalArgumentException e) {
            log.error("Failed to parse payload. Publishing to DLQ. raw={} error={}",
                raw, e.getMessage());
            publishToDlq("UNKNOWN", raw, e.getMessage());
            return;
        }

        String txnId = event.getTransactionId();
        log.info("Processing transaction txnId={} amount={} ipCountry={} card={}",
            txnId, event.getAmount(), event.getIpCountry(), event.getCardFingerprint());

        try {

            // Step 2: Idempotency check
            String idempotencyKey = IDEMPOTENCY_KEY_PREFIX + txnId;
            Boolean claimed = redis.opsForValue()
                .setIfAbsent(idempotencyKey, "1", IDEMPOTENCY_TTL);

            if (!Boolean.TRUE.equals(claimed)) {
                log.info("Duplicate message detected. Already processed txnId={}. Skipping.", txnId);
                return;
            }

            // Step 3: Stage 1 — Hard Rules
            if (BLOCKLISTED_CARDS.contains(event.getCardFingerprint())) {
                log.warn("HARD_RULE: Blocklisted card. txnId={} card={}",
                    txnId, event.getCardFingerprint());
                saveDecision(txnId, DecisionType.AUTO_REJECTED, 100,
                    "BLOCKLIST_HIT", "HARD_RULES");
                return;
            }

            if (event.getAmount() > HARD_CAP_AMOUNT) {
                log.warn("HARD_RULE: Amount exceeds cap. txnId={} amount={} cap={}",
                    txnId, event.getAmount(), HARD_CAP_AMOUNT);
                saveDecision(txnId, DecisionType.AUTO_REJECTED, 100,
                    "AMOUNT_EXCEEDS_CAP", "HARD_RULES");
                return;
            }

            if (SANCTIONED_COUNTRIES.contains(event.getIpCountry())) {
                log.warn("HARD_RULE: Sanctioned country. txnId={} ipCountry={}",
                    txnId, event.getIpCountry());
                saveDecision(txnId, DecisionType.AUTO_REJECTED, 100,
                    "SANCTIONED_COUNTRY", "HARD_RULES");
                return;
            }

            // Step 4: Stage 2 — Behavioral Analysis
            int behavioralScore = behavioralAnalyzer.analyze(event);
            log.info("Behavioral score. txnId={} score={}", txnId, behavioralScore);

            // Step 5: Stage 3 — Rule Engine
            int ruleScore = ruleEngine.evaluate(event);

            // ---------------------------------------------------------------
            // Step 6: Stage 4 — External Enrichment (Phase 10)
            //
            // Call the IP reputation service wrapped in Resilience4j.
            // The call returns a CompletableFuture because @TimeLimiter
            // requires async execution.
            //
            // .get() blocks the Kafka consumer thread until the future
            // completes or the fallback fires. This is intentional —
            // we want the enrichment score before making the final decision.
            //
            // If the circuit is OPEN or the call times out, the fallback
            // returns EnrichmentResult.fallback() with score=0 and the
            // pipeline continues normally. No exception propagates here.
            //
            // The try-catch here is belt-and-suspenders: .get() can throw
            // InterruptedException or ExecutionException in edge cases.
            // We treat those as non-fatal and proceed with enrichScore=0.
            // ---------------------------------------------------------------
            int enrichScore = 0;
            try {
                CompletableFuture<EnrichmentResult> enrichFuture =
                    enrichmentService.enrich(event.getIpAddress(), txnId);

                EnrichmentResult enrichResult = enrichFuture.get();
                enrichScore = enrichResult.toScore();

                log.info("[ENRICHMENT] Result. txnId={} reputation={} score={}",
                    txnId, enrichResult.getReputation(), enrichScore);

            } catch (Exception e) {
                // This catches InterruptedException and ExecutionException.
                // The fallback inside ExternalEnrichmentService already handles
                // circuit open / timeout / retry exhaustion — those never reach here.
                // This catch is for truly unexpected errors in the Future itself.
                log.warn("[ENRICHMENT] Unexpected error getting enrichment result. " +
                    "txnId={} error={}. Proceeding with enrichScore=0.", txnId, e.getMessage());
            }

            // Combine all four stage scores
            int totalScore = behavioralScore + ruleScore + enrichScore;
            log.info("Total score. txnId={} behavioral={} rules={} enrichment={} total={}",
                txnId, behavioralScore, ruleScore, enrichScore, totalScore);

            // Final decision
            DecisionType finalDecision;
            if (totalScore >= 60) {
                finalDecision = DecisionType.AUTO_REJECTED;
            } else if (totalScore >= 20) {
                finalDecision = DecisionType.NEEDS_REVIEW;
            } else {
                finalDecision = DecisionType.APPROVED;
            }

            saveDecision(txnId, finalDecision, totalScore, "RULE_ENGINE", "STAGE_4");

        } catch (Exception e) {
            log.error("Unexpected error scoring txnId={}. Publishing to DLQ. error={}",
                txnId, e.getMessage());
            publishToDlq(txnId, raw, e.getMessage());
        }
    }

    private void publishToDlq(String transactionId, String payload, String errorMessage) {
        org.apache.kafka.clients.producer.ProducerRecord<String, String> record =
            new org.apache.kafka.clients.producer.ProducerRecord<>(
                DLQ_TOPIC, null, transactionId, payload
            );
        record.headers().add("error-message",
            (errorMessage != null ? errorMessage : "Unknown error").getBytes());

        kafkaTemplate.send(record);
        log.info("Published to DLQ. txnId={}", transactionId);
    }

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